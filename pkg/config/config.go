package config

import "os"

type Config struct {
    DatabaseURL string
    RedisURL    string
    JWTSecret   string  // 👈 Added
    Port        string
}

func Load() Config {
    return Config{
        DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:mysecretpassword@localhost:5432/predix?sslmode=disable"),
        RedisURL:    getEnv("REDIS_URL", "localhost:6379"),
        JWTSecret:   getEnv("JWT_SECRET", ""), // No fallback for production
        Port:        getEnv("PORT", "3000"),
    }
}

func getEnv(key, fallback string) string {
    if val := os.Getenv(key); val != "" {
        return val
    }
    return fallback
}