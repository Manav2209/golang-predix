package websocket

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
)

type RedisSubscriber struct {
	client *redis.Client
	hub    *Hub
}

func NewRedisSubscriber(
	client *redis.Client,
	hub *Hub,
) *RedisSubscriber {

	return &RedisSubscriber{
		client: client,
		hub:    hub,
	}
}

func (s *RedisSubscriber) Run(
	ctx context.Context,
) error {

	pubsub := s.client.Subscribe(
		ctx,
		"ws:updates",
	)

	defer pubsub.Close()

	log.Println(
		"WS Redis subscriber started",
	)

	channel := pubsub.Channel()

	for {

		select {

		case <-ctx.Done():
			return ctx.Err()

		case message, ok := <-channel:

			if !ok {
				return nil
			}

			if err := s.handle(
				[]byte(message.Payload),
			); err != nil {

				log.Printf(
					"WS event error: %v",
					err,
				)
			}
		}
	}
}

func (s *RedisSubscriber) handle(
	message []byte,
) error {

	var envelope struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(
		message,
		&envelope,
	); err != nil {
		return err
	}

	switch envelope.Type {

	case "trade":
		return s.handleTrade(
			envelope.Data,
		)

	case "depth":
		return s.handleDepth(
			envelope.Data,
		)

	default:
		log.Printf(
			"unknown websocket event: %s",
			envelope.Type,
		)

		return nil
	}
}

func (s *RedisSubscriber) handleTrade(
	data []byte,
) error {

	var trade TradeData

	if err := json.Unmarshal(
		data,
		&trade,
	); err != nil {
		return err
	}

	message := TradeMessage{
		Type: "trade",
		Data: trade,
	}

	payload, err := json.Marshal(message)

	if err != nil {
		return err
	}

	s.hub.BroadcastEvent(
		trade.EventID,
		payload,
	)

	return nil
}
func (s *RedisSubscriber) handleDepth(
	data []byte,
) error {

	var incoming struct {
		EventID string    `json:"eventId"`
		Data    DepthData `json:"data"`
	}

	if err := json.Unmarshal(
		data,
		&incoming,
	); err != nil {
		return err
	}

	message := DepthMessage{
		Type: "depth",
		Data: incoming.Data,
	}

	payload, err := json.Marshal(message)

	if err != nil {
		return err
	}

	s.hub.BroadcastEvent(
		incoming.EventID,
		payload,
	)

	return nil
}