package dbworker

import "time"

type EventType string

const (
	EventTradeExecuted EventType = "TRADE_EXECUTED"
)

type EventEnvelope struct {
	ID        string    `json:"id"`
	Type      EventType `json:"type"`
	CreatedAt time.Time `json:"createdAt"`
	Data      []byte    `json:"data"`
}