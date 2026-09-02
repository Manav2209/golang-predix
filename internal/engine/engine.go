package engine

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	"predix/pkg/redis"

	"github.com/google/uuid"
)

type Engine struct {
	mu           sync.RWMutex
	orderbooks   map[string]*OrderBook // eventID → OrderBook
	orders       map[string]*Order     // orderID → Order
	pendingQueue chan *Order           // incoming orders (buffered)
	redisManager *redis.RedisManager
	wal          *WAL
	metrics      *Metrics
	shutdown     chan struct{}
}

func NewEngine(rm *redis.RedisManager) *Engine {
	wal, _ := NewWAL("data/wal.log") // directory must exist
	return &Engine{
		orderbooks:   make(map[string]*OrderBook),
		orders:       make(map[string]*Order),
		pendingQueue: make(chan *Order, 10000),
		redisManager: rm,
		wal:          wal,
		metrics:      NewMetrics(),
		shutdown:     make(chan struct{}),
	}
}

func (e *Engine) Start() {
	log.Println("🚀 Engine started. Waiting for orders...")
	go e.consumeMessages()
	go e.processOrders()
}

// Graceful shutdown: drain pending queue
func (e *Engine) Shutdown(ctx context.Context) {
	log.Println("Shutting down engine...")
	close(e.shutdown)
	// Wait for pending queue to drain (with timeout)
	for {
		select {
		case <-ctx.Done():
			log.Println("Shutdown timeout, forcing exit")
			return
		default:
			if len(e.pendingQueue) == 0 {
				log.Println("Pending queue drained.")
				e.wal.Close()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Consume messages from Redis (blocking)
func (e *Engine) consumeMessages() {
	client := e.redisManager.GetClient()
	for {
		select {
		case <-e.shutdown:
			return
		default:
			// Blocking pop from Redis list "messages"
			result, err := client.BRPop(context.Background(), 0, "messages").Result()
			if err != nil {
				log.Println("BRPop error:", err)
				continue
			}
			// result: [key, value]
			data := result[1]

			var payload struct {
				ClientID string          `json:"clientId"`
				Message  json.RawMessage `json:"message"`
			}
			if err := json.Unmarshal([]byte(data), &payload); err != nil {
				log.Println("Unmarshal payload error:", err)
				continue
			}

			var msg redis.MessageToEngine
			if err := json.Unmarshal(payload.Message, &msg); err != nil {
				log.Println("Unmarshal message error:", err)
				continue
			}

			log.Printf("📥 Received: %s (clientId: %s)", msg.Type, payload.ClientID)

			// Process
			var response *redis.EngineResponse
			switch msg.Type {
			case "CREATE_ORDER":
				response = e.handleCreateOrder(msg.Payload)
			case "CANCEL_ORDER":
				response = e.handleCancelOrder(msg.Payload)
			case "GET_DEPTH":
				response = e.handleGetDepth(msg.Payload)
			case "GET_OPEN_ORDERS":
				response = e.handleGetOpenOrders(msg.Payload)
			case "CREATE_EVENT":
				response = e.handleCreateEvent(msg.Payload)
			default:
				response = &redis.EngineResponse{Success: false, Error: "unknown message type"}
			}

			// Publish response back to client's unique channel
			respBytes, _ := json.Marshal(response)
			if err := client.Publish(context.Background(), payload.ClientID, respBytes).Err(); err != nil {
				log.Println("Publish error:", err)
			}
		}
	}
}

// Process orders from the pending queue (matching)
func (e *Engine) processOrders() {
	for order := range e.pendingQueue {
		e.matchOrder(order)
		e.metrics.OrdersProcessed.Inc()
	}
}

// ------------- Handlers -------------

func (e *Engine) handleCreateOrder(payload json.RawMessage) *redis.EngineResponse {
	var req struct {
		EventID   string  `json:"eventId"`
		UserID    string  `json:"userId"`
		OrderType string  `json:"orderType"`
		Outcome   string  `json:"outcome"`
		Side      string  `json:"side"`
		Quantity  float64 `json:"quantity"`
		Price     float64 `json:"price"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return &redis.EngineResponse{Success: false, Error: "invalid payload"}
	}

	order := &Order{
		ID:        uuid.New().String(),
		EventID:   req.EventID,
		UserID:    req.UserID,
		OrderType: req.OrderType,
		Outcome:   req.Outcome,
		Side:      req.Side,
		Quantity:  req.Quantity,
		Price:     req.Price,
		Status:    "PENDING",
		CreatedAt: time.Now(),
	}

	// Store in memory
	e.mu.Lock()
	e.orders[order.ID] = order
	e.mu.Unlock()

	// Write to WAL for recovery
	if e.wal != nil {
		e.wal.Write(order)
	}

	// Push to queue for matching
	e.pendingQueue <- order

	return &redis.EngineResponse{
		Success: true,
		Data:    json.RawMessage(`{"orderId":"` + order.ID + `","status":"PENDING"}`),
	}
}

func (e *Engine) handleCancelOrder(payload json.RawMessage) *redis.EngineResponse {
	var req struct {
		OrderID string `json:"orderId"`
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return &redis.EngineResponse{Success: false, Error: "invalid payload"}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	order, exists := e.orders[req.OrderID]
	if !exists {
		return &redis.EngineResponse{Success: false, Error: "order not found"}
	}
	if order.Status == "FILLED" || order.Status == "CANCELLED" {
		return &redis.EngineResponse{Success: false, Error: "order cannot be cancelled"}
	}

	// Remove from orderbook
	ob, exists := e.orderbooks[req.EventID]
	if exists {
		ob.mu.Lock()
		if order.Side == "BUY" {
			for i, o := range ob.Bids {
				if o.ID == order.ID {
					ob.Bids = append(ob.Bids[:i], ob.Bids[i+1:]...)
					break
				}
			}
		} else {
			for i, o := range ob.Asks {
				if o.ID == order.ID {
					ob.Asks = append(ob.Asks[:i], ob.Asks[i+1:]...)
					break
				}
			}
		}
		ob.mu.Unlock()
	}

	order.Status = "CANCELLED"
	return &redis.EngineResponse{Success: true, Data: json.RawMessage(`{"status":"CANCELLED"}`)}
}

func (e *Engine) handleGetDepth(payload json.RawMessage) *redis.EngineResponse {
	var req struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return &redis.EngineResponse{Success: false, Error: "invalid payload"}
	}
	e.mu.RLock()
	ob, exists := e.orderbooks[req.EventID]
	e.mu.RUnlock()

	if !exists {
		depth := Depth{EventID: req.EventID}
		data, _ := json.Marshal(depth)
		return &redis.EngineResponse{Success: true, Data: data}
	}

	ob.mu.RLock()
	defer ob.mu.RUnlock()

	depth := Depth{EventID: req.EventID}

	// YES side (we treat all orders as YES because outcome is separate? Actually we need to separate by outcome)
	// For simplicity, we combine all orders for this event regardless of outcome.
	// In a real exchange you'd separate YES/NO. We'll store outcome in order but for depth we can treat all.
	// To keep it simple, we'll just aggregate Bids and Asks from all outcomes.
	// Alternatively, you can filter by outcome. We'll leave as is for v0.

	bidsMap := make(map[float64]float64)
	asksMap := make(map[float64]float64)
	for _, o := range ob.Bids {
		if o.Status == "PENDING" || o.Status == "PARTIAL" {
			bidsMap[o.Price] += o.Quantity
		}
	}
	for _, o := range ob.Asks {
		if o.Status == "PENDING" || o.Status == "PARTIAL" {
			asksMap[o.Price] += o.Quantity
		}
	}

	for price, qty := range bidsMap {
		depth.Yes.Bids = append(depth.Yes.Bids, OrderBookEntry{Price: price, Quantity: qty})
	}
	sort.Slice(depth.Yes.Bids, func(i, j int) bool {
		return depth.Yes.Bids[i].Price > depth.Yes.Bids[j].Price
	})
	for price, qty := range asksMap {
		depth.Yes.Asks = append(depth.Yes.Asks, OrderBookEntry{Price: price, Quantity: qty})
	}
	sort.Slice(depth.Yes.Asks, func(i, j int) bool {
		return depth.Yes.Asks[i].Price < depth.Yes.Asks[j].Price
	})

	data, _ := json.Marshal(depth)
	return &redis.EngineResponse{Success: true, Data: data}
}

func (e *Engine) handleGetOpenOrders(payload json.RawMessage) *redis.EngineResponse {
	var req struct {
		EventID string `json:"eventId"`
		UserID  string `json:"userId"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return &redis.EngineResponse{Success: false, Error: "invalid payload"}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	var orders []*Order
	for _, o := range e.orders {
		if o.EventID == req.EventID && o.UserID == req.UserID {
			orders = append(orders, o)
		}
	}
	data, _ := json.Marshal(orders)
	return &redis.EngineResponse{Success: true, Data: data}
}

func (e *Engine) handleCreateEvent(payload json.RawMessage) *redis.EngineResponse {
	var req struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return &redis.EngineResponse{Success: false, Error: "invalid payload"}
	}
	e.mu.Lock()
	e.orderbooks[req.EventID] = &OrderBook{Bids: []*Order{}, Asks: []*Order{}}
	e.mu.Unlock()
	return &redis.EngineResponse{Success: true, Data: json.RawMessage(`{"status":"created"}`)}
}

// ---------- Matching Logic ----------
func (e *Engine) matchOrder(order *Order) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ob, exists := e.orderbooks[order.EventID]
	if !exists {
		order.Status = "REJECTED"
		return
	}
	ob.mu.Lock()
	defer ob.mu.Unlock()

	remaining := order.Quantity

	if order.Side == "BUY" {
		// Sort asks ascending by price
		sort.Slice(ob.Asks, func(i, j int) bool {
			return ob.Asks[i].Price < ob.Asks[j].Price
		})
		for i := 0; i < len(ob.Asks) && remaining > 0; i++ {
			ask := ob.Asks[i]
			if ask.Status == "FILLED" || ask.Status == "CANCELLED" {
				continue
			}
			if order.OrderType == "LIMIT" && ask.Price > order.Price {
				break
			}
			matchedQty := min(remaining, ask.Quantity)
			// Create trade
			trade := &Trade{
				ID:        uuid.New().String(),
				OrderID:   order.ID,
				EventID:   order.EventID,
				UserID:    order.UserID,
				Side:      "BUY",
				Quantity:  matchedQty,
				Price:     ask.Price,
				CreatedAt: time.Now(),
			}
			e.publishTrade(trade)
			remaining -= matchedQty
			ask.Quantity -= matchedQty
			if ask.Quantity <= 0 {
				ask.Status = "FILLED"
			} else {
				ask.Status = "PARTIAL"
			}
		}
	} else {
		// SELL: match against bids descending price
		sort.Slice(ob.Bids, func(i, j int) bool {
			return ob.Bids[i].Price > ob.Bids[j].Price
		})
		for i := 0; i < len(ob.Bids) && remaining > 0; i++ {
			bid := ob.Bids[i]
			if bid.Status == "FILLED" || bid.Status == "CANCELLED" {
				continue
			}
			if order.OrderType == "LIMIT" && bid.Price < order.Price {
				break
			}
			matchedQty := min(remaining, bid.Quantity)
			trade := &Trade{
				ID:        uuid.New().String(),
				OrderID:   order.ID,
				EventID:   order.EventID,
				UserID:    order.UserID,
				Side:      "SELL",
				Quantity:  matchedQty,
				Price:     bid.Price,
				CreatedAt: time.Now(),
			}
			e.publishTrade(trade)
			remaining -= matchedQty
			bid.Quantity -= matchedQty
			if bid.Quantity <= 0 {
				bid.Status = "FILLED"
			} else {
				bid.Status = "PARTIAL"
			}
		}
	}

	// Update order status
	if remaining == 0 {
		order.Status = "FILLED"
	} else if remaining < order.Quantity {
		order.Status = "PARTIAL"
		order.Quantity = remaining
		// Add remaining to orderbook
		if order.Side == "BUY" {
			ob.Bids = append(ob.Bids, order)
		} else {
			ob.Asks = append(ob.Asks, order)
		}
	} else {
		// No match, add full order to orderbook
		if order.Side == "BUY" {
			ob.Bids = append(ob.Bids, order)
		} else {
			ob.Asks = append(ob.Asks, order)
		}
	}
}

func (e *Engine) publishTrade(trade *Trade) {
	client := e.redisManager.GetClient()
	data, _ := json.Marshal(trade)
	// Publish to trade channel for DB service
	client.Publish(context.Background(), "trades", data)
	// Also to WebSocket updates
	client.Publish(context.Background(), "ws:updates", data)
	log.Printf("📊 Trade executed: %+v", trade)
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}