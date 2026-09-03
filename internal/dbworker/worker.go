package dbworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"predix/internal/engine"
	"predix/internal/repository"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	redis   *redis.Client
	queries *repository.Queries
}

func New(
	redisClient *redis.Client,
	queries *repository.Queries,
) *Worker {
	return &Worker{
		redis:   redisClient,
		queries: queries,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	log.Println("DB worker started")

	for {
		select {
		case <-ctx.Done():
			log.Println("DB worker shutting down")
			return ctx.Err()

		default:
		}

		err := w.processNext(ctx)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}

			log.Printf("DB worker error: %v", err)

			select {
			case <-ctx.Done():
				return ctx.Err()

			case <-time.After(time.Second):
			}
		}
	}
}

func (w *Worker) processNext(ctx context.Context) error {
	result, err := w.redis.BRPop(
		ctx,
		0,
		"db_processor",
	).Result()

	if err != nil {
		return err
	}

	if len(result) != 2 {
		return errors.New("invalid redis message")
	}

	return w.handleMessage(
		ctx,
		[]byte(result[1]),
	)
}

func (w *Worker) handleMessage(
	ctx context.Context,
	data []byte,
) error {

	var envelope EventEnvelope

	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf(
			"decode event envelope: %w",
			err,
		)
	}

	switch envelope.Type {

	case EventTradeExecuted:
		return w.handleTradeExecuted(
			ctx,
			envelope,
		)

	default:
		return fmt.Errorf(
			"unknown event type: %s",
			envelope.Type,
		)
	}
}

func (w *Worker) handleTradeExecuted(
	ctx context.Context,
	envelope EventEnvelope,
) error {

	var trade engine.Trade

	if err := json.Unmarshal(
		envelope.Data,
		&trade,
	); err != nil {
		return fmt.Errorf(
			"decode trade: %w",
			err,
		)
	}

	if err := validateTrade(&trade); err != nil {
		return err
	}

	return w.settleTrade(
		ctx,
		&trade,
	)
}

func validateTrade(trade *engine.Trade) error {
	if trade.ID == "" {
		return errors.New("trade id is required")
	}

	if trade.EventID == "" {
		return errors.New("event id is required")
	}

	if trade.OrderID == "" {
		return errors.New("taker order id is required")
	}

	if trade.MatchOrderID == "" {
		return errors.New("maker order id is required")
	}

	if trade.BuyerID == "" {
		return errors.New("buyer id is required")
	}

	if trade.SellerID == "" {
		return errors.New("seller id is required")
	}

	if trade.Outcome != "YES" &&
		trade.Outcome != "NO" {
		return errors.New("invalid outcome")
	}

	if trade.Quantity <= 0 {
		return errors.New("quantity must be greater than zero")
	}

	if trade.Price < 0 {
		return errors.New("price cannot be negative")
	}

	return nil
}

func (w *Worker) settleTrade(
	ctx context.Context,
	trade *engine.Trade,
) error {

	tx, err := w.queries.DB().Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	qtx := w.queries.WithTx(tx)

	tradeID, err := uuid.Parse(trade.ID)
	if err != nil {
		return fmt.Errorf("invalid trade id: %w", err)
	}

	eventID, err := uuid.Parse(trade.EventID)
	if err != nil {
		return fmt.Errorf("invalid event id: %w", err)
	}

	buyerID, err := uuid.Parse(trade.BuyerID)
	if err != nil {
		return fmt.Errorf("invalid buyer id: %w", err)
	}

	sellerID, err := uuid.Parse(trade.SellerID)
	if err != nil {
		return fmt.Errorf("invalid seller id: %w", err)
	}

	takerOrderID, err := uuid.Parse(trade.OrderID)
	if err != nil {
		return fmt.Errorf("invalid taker order id: %w", err)
	}

	makerOrderID, err := uuid.Parse(trade.MatchOrderID)
	if err != nil {
		return fmt.Errorf("invalid maker order id: %w", err)
	}

	/*
		1. Insert trade.

		If the same trade is delivered again,
		the unique constraint prevents duplication.
	*/
	created, err := qtx.InsertTrade(
		ctx,
		repository.InsertTradeParams{
			ID:           tradeID,
			EventID:      eventID,
			MakerOrderID: makerOrderID,
			TakerOrderID: takerOrderID,
			BuyerID:      buyerID,
			SellerID:     sellerID,
			Outcome:      trade.Outcome,
			TakerSide:    trade.TakerSide,
			Quantity:     trade.Quantity,
			Price:        trade.Price,
			CreatedAt:    trade.CreatedAt,
		},
	)

	if err != nil {
		// If trade already exists, treat it as
		// already settled.
		if isUniqueViolation(err) {
			return nil
		}

		return fmt.Errorf(
			"insert trade: %w",
			err,
		)
	}

	_ = created

	/*
		2. Update taker order.
	*/
	if err := qtx.ApplyOrderFill(
		ctx,
		repository.ApplyOrderFillParams{
			ID:       takerOrderID,
			Quantity: trade.Quantity,
		},
	); err != nil {
		return fmt.Errorf(
			"update taker order: %w",
			err,
		)
	}

	/*
		3. Update maker order.
	*/
	if err := qtx.ApplyOrderFill(
		ctx,
		repository.ApplyOrderFillParams{
			ID:       makerOrderID,
			Quantity: trade.Quantity,
		},
	); err != nil {
		return fmt.Errorf(
			"update maker order: %w",
			err,
		)
	}

	/*
		4. Buyer balance.

		BUY:
		    cost = quantity * price

		SELL:
		    seller receives quantity * price.
	*/
	total := trade.Quantity * trade.Price

	if err := qtx.DeductBalance(
		ctx,
		repository.DeductBalanceParams{
			UserID: buyerID,
			Amount: total,
		},
	); err != nil {
		return fmt.Errorf(
			"deduct buyer balance: %w",
			err,
		)
	}

	if err := qtx.AddBalance(
		ctx,
		repository.AddBalanceParams{
			UserID: sellerID,
			Amount: total,
		},
	); err != nil {
		return fmt.Errorf(
			"credit seller balance: %w",
			err,
		)
	}

	/*
		5. Buyer position.
	*/
	if err := qtx.UpsertPosition(
		ctx,
		repository.UpsertPositionParams{
			UserID:  buyerID,
			EventID: eventID,
			Outcome: trade.Outcome,
			Shares:  trade.Quantity,
			Price:   trade.Price,
		},
	); err != nil {
		return fmt.Errorf(
			"update buyer position: %w",
			err,
		)
	}

	/*
		6. Seller position.
	*/
	if err := qtx.UpsertPosition(
		ctx,
		repository.UpsertPositionParams{
			UserID:  sellerID,
			EventID: eventID,
			Outcome: trade.Outcome,
			Shares:  -trade.Quantity,
			Price:   trade.Price,
		},
	); err != nil {
		return fmt.Errorf(
			"update seller position: %w",
			err,
		)
	}

	/*
		7. Transactions.
	*/
	if err := qtx.InsertTransaction(
		ctx,
		repository.InsertTransactionParams{
			TradeID:     tradeID,
			OrderID:     takerOrderID,
			UserID:      buyerID,
			Side:        "BUY",
			Outcome:     trade.Outcome,
			Quantity:    trade.Quantity,
			Price:       trade.Price,
			TotalAmount: total,
		},
	); err != nil {
		return fmt.Errorf(
			"insert buyer transaction: %w",
			err,
		)
	}

	if err := qtx.InsertTransaction(
		ctx,
		repository.InsertTransactionParams{
			TradeID:     tradeID,
			OrderID:     makerOrderID,
			UserID:      sellerID,
			Side:        "SELL",
			Outcome:     trade.Outcome,
			Quantity:    trade.Quantity,
			Price:       trade.Price,
			TotalAmount: total,
		},
	); err != nil {
		return fmt.Errorf(
			"insert seller transaction: %w",
			err,
		)
	}

	/*
		8. Update event volume.
	*/
	if err := qtx.IncrementEventVolume(
		ctx,
		repository.IncrementEventVolumeParams{
			EventID: eventID,
			Volume:  total,
		},
	); err != nil {
		return fmt.Errorf(
			"update event volume: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit settlement: %w",
			err,
		)
	}

	log.Printf(
		"trade settled trade=%s quantity=%f price=%f",
		trade.ID,
		trade.Quantity,
		trade.Price,
	)

	return nil
}