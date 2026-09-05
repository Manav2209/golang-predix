package dbworker

import "predix/internal/events"

// Aliases kept so existing references compile. Prefer the events package.
type EventType = events.EventType

const (
	EventTradeExecuted = events.EventTradeExecuted
	EventOrderCreated  = events.EventOrderCreated
	EventOrderStatus   = events.EventOrderStatus
	EventOrderCanceled = events.EventOrderCanceled
)

type EventEnvelope = events.EventEnvelope