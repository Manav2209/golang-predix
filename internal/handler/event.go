package handler

import "github.com/gin-gonic/gin"

func (h *Handler) GetEvents(c *gin.Context) {
	c.JSON(200, gin.H{"message": "get events"})
}

func (h *Handler) CreateEvent(c *gin.Context) {
	c.JSON(200, gin.H{"message": "create event"})
}

func (h *Handler) GetEvent(c *gin.Context) {
	id := c.Param("id")
	c.JSON(200, gin.H{"message": "get event: " + id})
}