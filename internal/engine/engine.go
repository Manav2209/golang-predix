package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"predix/pkg/redis"

	"github.com/google/uuid"
)

var (
	ErrEventNotFound      = errors.New("event not found")
	ErrOrderNotFound      = errors.New("order not found")
	ErrOrderAlreadyExists = errors.New("order already exists")
	ErrOrderNotCancelable = errors.New("order cannot be canceled")
	ErrInvalidOrder       = errors.New("invalid order")
)

type Engine struct {
	// Single synchronization boundary.
	//
	// Every access to:
	// - markets
	// - orders
	// - orderbooks
	// happens through this mutex.
	mu sync.RWMutex

	markets map[string]*Market
	orders  map[string]*Order

	pendingQueue chan *Order

	redisManager *redis.RedisManager
	wal          *WAL
	metrics      *Metrics

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewEngine(rm *redis.RedisManager) (*Engine, error) {
	if rm == nil {
		return nil, errors.New("redis manager is required")
	}

	wal, err := NewWAL("data/wal.log")
	if err != nil {
		return nil, fmt.Errorf("create WAL: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		markets:      make(map[string]*Market),
		orders:       make(map[string]*Order),
		pendingQueue: make(chan *Order, 10000),

		redisManager: rm,
		wal:          wal,
		metrics:      NewMetrics(),

		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (e *Engine) Start() {
	e.startOnce.Do(func() {
		log.Println("Engine started. Waiting for orders...")

		e.wg.Add(2)

		go func() {
			defer e.wg.Done()
			e.consumeMessages()
		}()

		go func() {
			defer e.wg.Done()
			e.processOrders()
		}()
	})
}

func (e *Engine) Shutdown(ctx context.Context) {
	e.stopOnce.Do(func() {
		log.Println("Shutting down engine...")

		// Stop Redis consumer.
		e.cancel()

		// Wait for consumer to stop.
		//
		// processOrders is still alive at this point.
		// We close the queue only after the consumer is gone.
		e.wg.Wait()

		close(e.pendingQueue)

		if e.wal != nil {
			if err := e.wal.Close(); err != nil {
				log.Println("WAL close error:", err)
			}
		}

		log.Println("Engine stopped.")
	})

	_ = ctx
}


func (e *Engine) consumeMessages() {
	client := e.redisManager.GetClient()

	for {
		result, err := client.BRPop(
			e.ctx,
			0,
			"messages",
		).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			log.Println("BRPop error:", err)

			// Don't spin aggressively if Redis is unavailable.
			select {
			case <-time.After(time.Second):
			case <-e.ctx.Done():
				return
			}

			continue
		}

		if len(result) != 2 {
			log.Println("invalid BRPop response")
			continue
		}

		data := result[1]

		var payload struct {
			ClientID string          `json:"clientId"`
			Message  json.RawMessage `json:"message"`
		}

		if err := json.Unmarshal(
			[]byte(data),
			&payload,
		); err != nil {
			log.Println("payload unmarshal error:", err)
			continue
		}

		if payload.ClientID == "" {
			log.Println("message missing clientId")
			continue
		}

		var msg redis.MessageToEngine

		if err := json.Unmarshal(
			payload.Message,
			&msg,
		); err != nil {
			log.Println("message unmarshal error:", err)
			continue
		}

		log.Printf(
			"Received message type=%s clientId=%s",
			msg.Type,
			payload.ClientID,
		)

		response := e.handleMessage(msg)

		respBytes, err := json.Marshal(response)
		if err != nil {
			log.Println("response marshal error:", err)
			continue
		}

		if err := client.Publish(
			e.ctx,
			payload.ClientID,
			respBytes,
		).Err(); err != nil {
			// During shutdown this is expected.
			if !errors.Is(err, context.Canceled) {
				log.Println("response publish error:", err)
			}

			if e.ctx.Err() != nil {
				return
			}
		}
	}
}

func (e *Engine) handleMessage(
	msg redis.MessageToEngine,
) *redis.EngineResponse {

	switch msg.Type {

	case "CREATE_ORDER":
		return e.handleCreateOrder(msg.Payload)

	case "CANCEL_ORDER":
		return e.handleCancelOrder(msg.Payload)

	case "GET_DEPTH":
		return e.handleGetDepth(msg.Payload)

	case "GET_OPEN_ORDERS":
		return e.handleGetOpenOrders(msg.Payload)

	case "CREATE_EVENT":
		return e.handleCreateEvent(msg.Payload)

	default:
		return &redis.EngineResponse{
			Success: false,
			Error:   "unknown message type",
		}
	}
}

func (e *Engine) processOrders() {
	for order := range e.pendingQueue {
		if order == nil {
			continue
		}

		e.matchOrder(order)

		if e.metrics != nil {
			e.metrics.OrdersProcessed.Inc()
		}
	}
}

func (e *Engine) handleCreateEvent(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		EventID string `json:"eventId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	if req.EventID == "" {
		return failure("eventId is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.markets[req.EventID]; exists {
		// Idempotent behavior.
		return successJSON(map[string]any{
			"status": "already_exists",
		})
	}

	e.markets[req.EventID] = NewMarket()

	return successJSON(map[string]any{
		"status": "created",
	})
}

func (e *Engine) handleCreateOrder(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		OrderID   string  `json:"orderId"`
		EventID   string  `json:"eventId"`
		UserID    string  `json:"userId"`
		OrderType string  `json:"orderType"`
		Outcome   string  `json:"outcome"`
		Side      string  `json:"side"`
		Quantity  float64 `json:"quantity"`
		Price     float64 `json:"price"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	orderID := req.OrderID

	if orderID == "" {
		orderID = uuid.NewString()
	}

	order := &Order{
		ID:        orderID,
		EventID:   req.EventID,
		UserID:    req.UserID,
		OrderType: req.OrderType,
		Outcome:   req.Outcome,
		Side:      req.Side,

		Quantity:          req.Quantity,
		FilledQuantity:    0,
		RemainingQuantity: req.Quantity,

		Price:     req.Price,
		Status:    StatusPending,
		CreatedAt: time.Now().UTC(),
	}

	if err := validateOrder(order); err != nil {
		return failure(err.Error())
	}

	e.mu.Lock()

	if _, exists := e.orders[order.ID]; exists {
		e.mu.Unlock()

		return failure(ErrOrderAlreadyExists.Error())
	}

	if _, exists := e.markets[order.EventID]; !exists {
		e.mu.Unlock()

		order.Status = StatusRejected

		return failure(ErrEventNotFound.Error())
	}

	e.orders[order.ID] = order

	e.mu.Unlock()

	// WAL must succeed before the order enters
	// the matching queue.
	if e.wal != nil {
		if err := e.wal.Write(order); err != nil {
			e.mu.Lock()
			delete(e.orders, order.ID)
			order.Status = StatusRejected
			e.mu.Unlock()

			return failure(
				fmt.Sprintf("failed to persist order: %v", err),
			)
		}
	}

	// Queue order for matching.
	select {
	case e.pendingQueue <- order:

		return successJSON(map[string]any{
			"orderId": order.ID,
			"status":  StatusPending,
		})

	case <-e.ctx.Done():
		return failure("engine shutting down")
	}
}

func validateOrder(order *Order) error {
	if order == nil {
		return ErrInvalidOrder
	}

	if order.EventID == "" {
		return errors.New("eventId is required")
	}

	if order.UserID == "" {
		return errors.New("userId is required")
	}

	if order.Quantity <= 0 {
		return errors.New("quantity must be greater than zero")
	}

	switch order.Side {
	case SideBuy, SideSell:
	default:
		return errors.New("invalid side")
	}

	switch order.Outcome {
	case OutcomeYes, OutcomeNo:
	default:
		return errors.New("invalid outcome")
	}

	switch order.OrderType {
	case OrderTypeLimit:
		if order.Price <= 0 {
			return errors.New("limit order price must be greater than zero")
		}

	case OrderTypeMarket:
		// Price is ignored for market orders.

	default:
		return errors.New("invalid order type")
	}

	return nil
}

func (e *Engine) matchOrder(order *Order) {
	e.mu.Lock()

	// Cancellation can happen while the order is waiting
	// in pendingQueue. Since cancellation uses the same mutex,
	// this check makes ordering deterministic.
	if order.Status == StatusCanceled {
		e.mu.Unlock()
		return
	}

	market, exists := e.markets[order.EventID]

	if !exists {
		order.Status = StatusRejected
		e.mu.Unlock()
		return
	}

	book := market.Book(order.Outcome)

	if book == nil {
		order.Status = StatusRejected
		e.mu.Unlock()
		return
	}

	trades := e.matchAgainstBook(book, order)

	if order.RemainingQuantity == 0 {
		order.Status = StatusFilled
	} else if order.FilledQuantity > 0 {
		order.Status = StatusPartial

		// Remaining quantity rests on the book
		// only for limit orders.
		if order.OrderType == OrderTypeLimit {
			e.addToBook(book, order)
		}
	} else {
		// Nothing matched.
		if order.OrderType == OrderTypeMarket {
			// Market order disappears if no liquidity exists.
			order.Status = StatusCanceled
		} else {
			order.Status = StatusPending
			e.addToBook(book, order)
		}
	}

	e.mu.Unlock()

	// Never perform Redis I/O while holding the engine lock.
	for _, trade := range trades {
		if err := e.publishTrade(trade); err != nil {
			log.Printf(
				"failed to publish trade %s: %v",
				trade.ID,
				err,
			)
		}
	}
}

func (e *Engine) matchAgainstBook(
	book *OrderBook,
	incoming *Order,
) []*Trade {

	var trades []*Trade

	for incoming.RemainingQuantity > 0 {

		var resting *Order

		if incoming.Side == SideBuy {

			// Remove invalid/filled orders sitting at front.
			e.removeInvalidAsks(book)

			if len(book.Asks) == 0 {
				break
			}

			resting = book.Asks[0]

		} else {

			e.removeInvalidBids(book)

			if len(book.Bids) == 0 {
				break
			}

			resting = book.Bids[0]
		}

		if !canMatch(incoming, resting) {
			break
		}

		matchQuantity := min(
			incoming.RemainingQuantity,
			resting.RemainingQuantity,
		)

		if matchQuantity <= 0 {
			break
		}

		trade := buildTrade(
			incoming,
			resting,
			matchQuantity,
		)

		trades = append(trades, trade)

		// Update incoming.
		incoming.RemainingQuantity -= matchQuantity
		incoming.FilledQuantity += matchQuantity

		// Update resting.
		resting.RemainingQuantity -= matchQuantity
		resting.FilledQuantity += matchQuantity

		if resting.RemainingQuantity <= 0 {

			resting.RemainingQuantity = 0
			resting.Status = StatusFilled

			if incoming.Side == SideBuy {
				book.Asks = book.Asks[1:]
			} else {
				book.Bids = book.Bids[1:]
			}

		} else {
			resting.Status = StatusPartial
		}
	}

	return trades
}

func canMatch(
	incoming *Order,
	resting *Order,
) bool {

	// Outcome must always match.
	if incoming.Outcome != resting.Outcome {
		return false
	}

	// Market order matches any available price.
	if incoming.OrderType == OrderTypeMarket {
		return true
	}

	if incoming.Side == SideBuy {
		return resting.Price <= incoming.Price
	}

	if incoming.Side == SideSell {
		return resting.Price >= incoming.Price
	}

	return false
}

func (e *Engine) addToBook(
	book *OrderBook,
	order *Order,
) {
	if order.Side == SideBuy {

		book.Bids = append(book.Bids, order)

		sort.SliceStable(
			book.Bids,
			func(i, j int) bool {
				return book.Bids[i].Price >
					book.Bids[j].Price
			},
		)

		return
	}

	book.Asks = append(book.Asks, order)

	sort.SliceStable(
		book.Asks,
		func(i, j int) bool {
			return book.Asks[i].Price <
				book.Asks[j].Price
		},
	)
}

func (e *Engine) removeInvalidBids(book *OrderBook) {
	for len(book.Bids) > 0 {

		order := book.Bids[0]

		if order.Status == StatusFilled ||
			order.Status == StatusCanceled ||
			order.RemainingQuantity <= 0 {

			book.Bids = book.Bids[1:]
			continue
		}

		break
	}
}

func (e *Engine) removeInvalidAsks(book *OrderBook) {
	for len(book.Asks) > 0 {

		order := book.Asks[0]

		if order.Status == StatusFilled ||
			order.Status == StatusCanceled ||
			order.RemainingQuantity <= 0 {

			book.Asks = book.Asks[1:]
			continue
		}

		break
	}
}

func buildTrade(
	taker *Order,
	maker *Order,
	quantity float64,
) *Trade {

	trade := &Trade{
		ID:           uuid.NewString(),
		OrderID:      taker.ID,
		MatchOrderID: maker.ID,

		EventID: taker.EventID,
		Outcome: taker.Outcome,

		TakerSide: taker.Side,

		Quantity: quantity,

		// Trade executes at maker/resting price.
		Price: maker.Price,

		CreatedAt: time.Now().UTC(),
	}

	if taker.Side == SideBuy {
		trade.BuyerID = taker.UserID
		trade.SellerID = maker.UserID
	} else {
		trade.BuyerID = maker.UserID
		trade.SellerID = taker.UserID
	}

	return trade
}

func (e *Engine) handleCancelOrder(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		OrderID string `json:"orderId"`
		EventID string `json:"eventId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	if req.OrderID == "" {
		return failure("orderId is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	order, exists := e.orders[req.OrderID]

	if !exists {
		return failure(ErrOrderNotFound.Error())
	}

	if req.EventID != "" && order.EventID != req.EventID {
		return failure("order does not belong to event")
	}

	if order.Status == StatusFilled ||
		order.Status == StatusCanceled ||
		order.Status == StatusRejected {

		return failure(ErrOrderNotCancelable.Error())
	}

	market, exists := e.markets[order.EventID]

	if !exists {
		return failure(ErrEventNotFound.Error())
	}

	book := market.Book(order.Outcome)

	if order.Side == SideBuy {
		removeOrderFromSlice(
			&book.Bids,
			order.ID,
		)
	} else {
		removeOrderFromSlice(
			&book.Asks,
			order.ID,
		)
	}

	order.Status = StatusCanceled

	return successJSON(map[string]any{
		"orderId": order.ID,
		"status":  StatusCanceled,
	})
}

func removeOrderFromSlice(
	orders *[]*Order,
	orderID string,
) bool {

	items := *orders

	for i, order := range items {

		if order.ID != orderID {
			continue
		}

		copy(
			items[i:],
			items[i+1:],
		)

		items[len(items)-1] = nil

		*orders = items[:len(items)-1]

		return true
	}

	return false
}

func (e *Engine) handleGetDepth(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		EventID string `json:"eventId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	e.mu.RLock()

	market, exists := e.markets[req.EventID]

	if !exists {
		e.mu.RUnlock()
		return failure(ErrEventNotFound.Error())
	}

	depth := Depth{
		EventID: req.EventID,
	}

	depth.Yes.Bids =
		aggregateBids(market.YES.Bids)

	depth.Yes.Asks =
		aggregateAsks(market.YES.Asks)

	depth.No.Bids =
		aggregateBids(market.NO.Bids)

	depth.No.Asks =
		aggregateAsks(market.NO.Asks)

	e.mu.RUnlock()

	return successJSON(depth)
}

func aggregateBids(
	orders []*Order,
) []OrderBookEntry {

	levels := make(map[float64]float64)

	for _, order := range orders {

		if order.Status != StatusPending &&
			order.Status != StatusPartial {
			continue
		}

		if order.RemainingQuantity <= 0 {
			continue
		}

		levels[order.Price] +=
			order.RemainingQuantity
	}

	result := make(
		[]OrderBookEntry,
		0,
		len(levels),
	)

	for price, quantity := range levels {

		result = append(
			result,
			OrderBookEntry{
				Price:    price,
				Quantity: quantity,
				Total:    price * quantity,
			},
		)
	}

	sort.Slice(
		result,
		func(i, j int) bool {
			return result[i].Price >
				result[j].Price
		},
	)

	return result
}
func aggregateAsks(
	orders []*Order,
) []OrderBookEntry {

	levels := make(map[float64]float64)

	for _, order := range orders {

		if order.Status != StatusPending &&
			order.Status != StatusPartial {
			continue
		}

		if order.RemainingQuantity <= 0 {
			continue
		}

		levels[order.Price] +=
			order.RemainingQuantity
	}

	result := make(
		[]OrderBookEntry,
		0,
		len(levels),
	)

	for price, quantity := range levels {

		result = append(
			result,
			OrderBookEntry{
				Price:    price,
				Quantity: quantity,
				Total:    price * quantity,
			},
		)
	}

	sort.Slice(
		result,
		func(i, j int) bool {
			return result[i].Price <
				result[j].Price
		},
	)

	return result
}

func (e *Engine) handleGetOpenOrders(
	payload json.RawMessage,
) *redis.EngineResponse {

	var req struct {
		EventID string `json:"eventId"`
		UserID  string `json:"userId"`
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		return failure("invalid payload")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	orders := make([]*Order, 0)

	for _, order := range e.orders {

		if order.EventID != req.EventID {
			continue
		}

		if order.UserID != req.UserID {
			continue
		}

		if order.Status != StatusPending &&
			order.Status != StatusPartial {
			continue
		}

		copy := *order
		orders = append(orders, &copy)
	}

	sort.SliceStable(
		orders,
		func(i, j int) bool {
			return orders[i].CreatedAt.Before(
				orders[j].CreatedAt,
			)
		},
	)

	return successJSON(orders)
}

func (e *Engine) publishTrade(
	trade *Trade,
) error {

	client := e.redisManager.GetClient()

	data, err := json.Marshal(trade)
	if err != nil {
		return err
	}

	// Persistence consumer.
	if err := client.Publish(
		context.Background(),
		"trades",
		data,
	).Err(); err != nil {
		return err
	}

	// WebSocket consumer.
	if err := client.Publish(
		context.Background(),
		"ws:updates",
		data,
	).Err(); err != nil {
		return err
	}

	log.Printf(
		"Trade executed: %+v",
		trade,
	)

	return nil
}
func min(a, b float64) float64 {
	if a < b {
		return a
	}

	return b
}

func successJSON(value any) *redis.EngineResponse {
	data, err := json.Marshal(value)

	if err != nil {
		return &redis.EngineResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	return &redis.EngineResponse{
		Success: true,
		Data:    data,
	}
}

func failure(message string) *redis.EngineResponse {
	return &redis.EngineResponse{
		Success: false,
		Error:   message,
	}
}