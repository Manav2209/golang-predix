package engine

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	OrdersProcessed prometheus.Counter
	OrderLatency    prometheus.Histogram
}

func NewMetrics() *Metrics {
	m := &Metrics{
		OrdersProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "engine_orders_processed_total",
			Help: "Total orders processed by the engine",
		}),
		OrderLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "engine_order_latency_seconds",
			Help:    "Order processing latency",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		}),
	}
	prometheus.MustRegister(m.OrdersProcessed, m.OrderLatency)
	return m
}