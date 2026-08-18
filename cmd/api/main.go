package main

import (
	"context"
	"log"
	"predix/internal/handler"
	"predix/internal/repository"
	"predix/internal/router"
	"predix/pkg/redis"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

	// PostgreSQL
	conn, err := pgxpool.New(ctx, "postgresql://neondb_owner:npg_8wNsl5kYKtIr@ep-spring-cell-ax0gjefl-pooler.c-4.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
	if err != nil {
		log.Fatal("DB connection failed:", err)
	}
	defer conn.Close()

	// Redis
	redisManager := redis.NewRedisManager("localhost:6379", "")
	defer redisManager.Close()

	queries := repository.New(conn)

	// Handler with dependencies
	h := handler.NewHandler(queries, redisManager)

	// Gin
	r := gin.Default()

	// Routes
	router.SetupRoutes(r, h)

	r.Run(":3000")
}