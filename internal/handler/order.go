package handler

import (
	"encoding/json"
	"predix/pkg/redis"
	"github.com/gin-gonic/gin"
)

func (h *Handler) CreateOrder(c *gin.Context) {
	var req struct {
		EventID string  `json:"eventId" binding:"required"`
		Side    string  `json:"side" binding:"required,oneof=YES NO"`
		Amount  float64 `json:"amount" binding:"required,gt=0"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	payload := map[string]interface{}{
		"eventId": req.EventID,
		"side":    req.Side,
		"amount":  req.Amount,
	}
	payloadBytes, _ := json.Marshal(payload)

	msg := redis.MessageToEngine{
		Type:    "CREATE_ORDER",
		Payload: payloadBytes,
	}

	resp, err := h.RedisManager.SendAndAwait(c.Request.Context(), msg)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	if !resp.Success {
		c.JSON(400, gin.H{"error": resp.Error})
		return
	}

	c.JSON(200, resp.Data)
}

func (h *Handler) DeleteOrder(c *gin.Context) {
	orderId := c.Param("orderId")
	c.JSON(200, gin.H{"message": "delete order: " + orderId})
}