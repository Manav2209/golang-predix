package redis

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"time"
	"github.com/redis/go-redis/v9"
)

type MessageToEngine struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type EngineResponse struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type RedisManager struct {
	client    *redis.Client
	publisher *redis.Client
}

func NewRedisManager(addr, password string) *RedisManager {
	opts := &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	}
	return &RedisManager{
		client:    redis.NewClient(opts),
		publisher: redis.NewClient(opts),
	}
}

func (r *RedisManager) SendAndAwait(ctx context.Context, msg MessageToEngine) (*EngineResponse, error) {
	clientID := generateClientID()
	responseChan := make(chan *EngineResponse, 1)
	errChan := make(chan error, 1)

	sub := r.client.Subscribe(ctx, clientID)
	defer sub.Close()

	go func() {
		msgChan := sub.Channel()
		select {
		case redisMsg := <-msgChan:
			var resp EngineResponse
			if err := json.Unmarshal([]byte(redisMsg.Payload), &resp); err != nil {
				errChan <- err
				return
			}
			responseChan <- &resp
		case <-ctx.Done():
			errChan <- ctx.Err()
		}
	}()

	payload := map[string]interface{}{
		"clientId": clientID,
		"message":  msg,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	if err := r.publisher.LPush(ctx, "messages", data).Err(); err != nil {
		return nil, err
	}

	select {
	case resp := <-responseChan:
		return resp, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(5 * time.Second):
		return nil, errors.New("timeout waiting for engine response")
	}
}

func generateClientID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 20)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func (r *RedisManager) Close() error {
	if err := r.client.Close(); err != nil {
		return err
	}
	return r.publisher.Close()
}

// GetClient returns the underlying Redis client (for engine)
func (r *RedisManager) GetClient() *redis.Client {
	return r.client
}