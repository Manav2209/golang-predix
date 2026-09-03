package engine

import "time"

const (
	SideBuy  = "BUY"
	SideSell = "SELL"

	OutcomeYes = "YES"
	OutcomeNo  = "NO"

	OrderTypeLimit  = "LIMIT"
	OrderTypeMarket = "MARKET"

	StatusPending  = "PENDING"
	StatusPartial  = "PARTIAL"
	StatusFilled   = "FILLED"
	StatusCanceled = "CANCELED"
	StatusRejected = "REJECTED"
)

type Order struct {
	ID        string    `json:"id"`
	EventID   string    `json:"eventId"`
	UserID    string    `json:"userId"`
	OrderType string    `json:"orderType"`
	Outcome   string    `json:"outcome"`
	Side      string    `json:"side"`

	// Quantity is the original requested quantity.
	Quantity float64 `json:"quantity"`

	// FilledQuantity tracks how much has been executed.
	FilledQuantity float64 `json:"filledQuantity"`

	// RemainingQuantity is what is still available to match.
	RemainingQuantity float64 `json:"remainingQuantity"`

	Price float64 `json:"price"`
	Status string  `json:"status"`

	CreatedAt time.Time `json:"createdAt"`
}

type Trade struct {
	ID string `json:"id"`

	// Incoming/taker order.
	OrderID string `json:"orderId"`

	// Existing/resting/maker order.
	MatchOrderID string `json:"matchOrderId"`

	EventID string `json:"eventId"`
	Outcome string `json:"outcome"`

	BuyerID  string `json:"buyerId"`
	SellerID string `json:"sellerId"`

	// BUY or SELL of the taker.
	TakerSide string `json:"takerSide"`

	Quantity float64 `json:"quantity"`
	Price    float64 `json:"price"`

	CreatedAt time.Time `json:"createdAt"`
}

type OrderBook struct {
	// Bids: highest price first.
	// Same-price orders remain FIFO.
	Bids []*Order

	// Asks: lowest price first.
	// Same-price orders remain FIFO.
	Asks []*Order
}

type Market struct {
	YES *OrderBook
	NO  *OrderBook
}

func NewMarket() *Market {
	return &Market{
		YES: &OrderBook{},
		NO:  &OrderBook{},
	}
}

func (m *Market) Book(outcome string) *OrderBook {
	switch outcome {
	case OutcomeYes:
		return m.YES
	case OutcomeNo:
		return m.NO
	default:
		return nil
	}
}

type OrderBookEntry struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
	Total    float64 `json:"total"`
}

type Depth struct {
	EventID string `json:"eventId"`

	Yes DepthSide `json:"yes"`
	No  DepthSide `json:"no"`
}

type DepthSide struct {
	Bids []OrderBookEntry `json:"bids"`
	Asks []OrderBookEntry `json:"asks"`
}