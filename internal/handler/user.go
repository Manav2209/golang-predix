package handler

import "github.com/gin-gonic/gin"

func (h *Handler) GetPosition(c *gin.Context) {
	c.JSON(200, gin.H{"message": "get position"})
}

func (h *Handler) GetBalances(c *gin.Context) {
	c.JSON(200, gin.H{"message": "get balances"})
}