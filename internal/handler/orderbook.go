package handler

import "github.com/gin-gonic/gin"

func (h *Handler) GetOrderbook(c *gin.Context) {
	eventId := c.Param("eventId")
	c.JSON(200, gin.H{"message": "get orderbook for event: " + eventId})
}

func (h *Handler) GetOrderbookDepth(c *gin.Context) {
	eventId := c.Param("eventId")
	c.JSON(200, gin.H{"message": "get orderbook depth for event: " + eventId})
}