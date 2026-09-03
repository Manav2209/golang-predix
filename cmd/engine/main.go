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

	redisManager := redis.NewRedisManager(
		cfg.RedisURL,
		"",
	)
	defer redisManager.Close()

	eng, err := engine.NewEngine(redisManager)
	if err != nil {
		log.Fatal("failed to create engine:", err)
	}

	eng.Start()

	log.Println("Orderbook engine is running.")

	sigChan := make(chan os.Signal, 1)

	signal.Notify(
		sigChan,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-sigChan

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	eng.Shutdown(ctx)
}