package dto

type OrderbookDepthResponse struct {
	Yes struct {
		Buy  []OrderBookEntry `json:"BUY"`
		Sell []OrderBookEntry `json:"SELL"`
	} `json:"YES"`
	No struct {
		Buy  []OrderBookEntry `json:"BUY"`
		Sell []OrderBookEntry `json:"SELL"`
	} `json:"NO"`
}

type OrderBookEntry struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
	Total    float64 `json:"total"`
}