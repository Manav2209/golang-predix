package handler

import (
	"net/http"
	"predix/internal/dto"
	"predix/internal/repository"
	"predix/pkg/auth"
	"golang.org/x/crypto/bcrypt"
	"github.com/gin-gonic/gin"
)

func (h *Handler) Signup(c *gin.Context) {
	var req dto.SignupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	_, err := h.Queries.GetUserByEmail(c.Request.Context(), req.Email)
	if err == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "email already registered"})
		return
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}

	user, err := h.Queries.CreateUser(c.Request.Context(), repository.CreateUserParams{
		Email:        req.Email,
		PasswordHash: string(hashed),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create user"})
		return
	}

	resp := dto.SignupResponse{
		ID:        user.ID.String(),
		Email:     user.Email,
		CreatedAt: user.CreatedAt.Time.String(),
	}
	c.JSON(http.StatusCreated, resp)
}

func (h *Handler) Signin(c *gin.Context) {
	var req dto.SigninRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	user, err := h.Queries.GetUserByEmail(c.Request.Context(), req.Email)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	token, err := auth.GenerateToken(user.ID.String(), user.Email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate token"})
		return
	}

	resp := dto.SigninResponse{
		Token: token,
		User: dto.UserResponse{
			ID:    user.ID.String(),
			Email: user.Email,
		},
	}
	c.JSON(http.StatusOK, resp)
}

func (h *Handler) Me(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	// TODO: Get user from DB by ID
	c.JSON(http.StatusOK, gin.H{
		"user_id": userID,
	})
}