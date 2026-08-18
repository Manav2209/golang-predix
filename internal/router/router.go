package router

import (
	"predix/internal/handler"
	"predix/internal/middleware"
	"github.com/gin-gonic/gin"
)

func SetupRoutes(r *gin.Engine, h *handler.Handler) {
	// Public routes
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "hello"})
	})

	// Auth routes
	r.POST("/signup", h.Signup)
	r.POST("/signin", h.Signin)

	// Protected routes
	auth := r.Group("/")
	auth.Use(middleware.AuthMiddleware())
	{
		auth.GET("/me", h.Me)

		// Events
		auth.GET("/events", h.GetEvents)
		auth.POST("/event", h.CreateEvent)
		auth.GET("/event/:id", h.GetEvent)

		// Orders
		auth.POST("/order", h.CreateOrder)
		auth.DELETE("/order/:orderId", h.DeleteOrder)

		// Orderbook
		auth.GET("/orderbook/:eventId", h.GetOrderbook)
		auth.GET("/orderbook/:eventId/depth", h.GetOrderbookDepth)

		// User
		auth.GET("/position", h.GetPosition)
		auth.GET("/balances", h.GetBalances)
	}
}