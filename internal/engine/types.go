package engine

import (
	"sync"
	"time"
)

type Order struct {
	ID        string    `json:"id"`
	EventID   string    `json:"eventId"`
	UserID    string    `json:"userId"`
	OrderType string    `json:"orderType"` // LIMIT, MARKET
	Outcome   string    `json:"outcome"`   // YES, NO
	Side      string    `json:"side"`      // BUY, SELL
	Quantity  float64   `json:"quantity"`
	Price     float64   `json:"price"`
	Status    string    `json:"status"` // PENDING, FILLED, CANCELLED, PARTIAL, REJECTED
	CreatedAt time.Time `json:"createdAt"`
}

type Trade struct {
	ID        string    `json:"id"`
	OrderID   string    `json:"orderId"`
	EventID   string    `json:"eventId"`
	UserID    string    `json:"userId"`
	Side      string    `json:"side"`
	Quantity  float64   `json:"quantity"`
	Price     float64   `json:"price"`
	CreatedAt time.Time `json:"createdAt"`
}

type OrderBook struct {
	mu   sync.RWMutex
	Bids []*Order // sorted DESC by price
	Asks []*Order // sorted ASC by price
}

type OrderBookEntry struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
	Total    float64 `json:"total"`
}

type Depth struct {
	EventID string `json:"eventId"`
	Yes     struct {
		Bids []OrderBookEntry `json:"bids"`
		Asks []OrderBookEntry `json:"asks"`
	} `json:"yes"`
	No struct {
		Bids []OrderBookEntry `json:"bids"`
		Asks []OrderBookEntry `json:"asks"`
	} `json:"no"`
}