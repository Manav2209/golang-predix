package websocket

import (
	"net/http"

	"github.com/gorilla/websocket"
)

type Server struct {
	Hub *Hub

	upgrader websocket.Upgrader
}

func NewServer(hub *Hub) *Server {

	return &Server{
		Hub: hub,

		upgrader: websocket.Upgrader{

			ReadBufferSize:  1024,
			WriteBufferSize: 1024,

			CheckOrigin: func(
				r *http.Request,
			) bool {
				// Development version.
				// Restrict this in production.
				return true
			},
		},
	}
}

func (s *Server) Handle(
	w http.ResponseWriter,
	r *http.Request,
) {

	conn, err := s.upgrader.Upgrade(
		w,
		r,
		nil,
	)

	if err != nil {
		return
	}

	client := NewClient(
		s.Hub,
		conn,
	)

	s.Hub.Register(client)

	go client.WritePump()
	go client.ReadPump()
}