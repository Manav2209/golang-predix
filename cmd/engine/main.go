package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"predix/internal/engine"
	"predix/pkg/config"
	"predix/pkg/redis"
)

func main() {
	cfg := config.Load()

	// Connect to Redis
	redisManager := redis.NewRedisManager(cfg.RedisURL, "")
	defer redisManager.Close()

	eng := engine.NewEngine(redisManager)
	go eng.Start()

	log.Println("Orderbook Engine is running. Press Ctrl+C to stop.")

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	eng.Shutdown(ctx)
	log.Println("Engine stopped.")
}