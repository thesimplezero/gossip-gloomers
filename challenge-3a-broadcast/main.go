package main

import (
	"encoding/json"
	"log"
	"strings"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:         n,
		ids:       make(map[int]struct{}),
		neighbors: make([]string, 0),
	}

	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	n *maelstrom.Node

	idsMu sync.RWMutex
	ids   map[int]struct{}

	neighborsMu sync.RWMutex
	neighbors   []string
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	messageVal, ok := body["message"]
	if !ok {
		return s.n.Reply(msg, map[string]any{
			"type": "error",
			"code": "invalid_request",
			"text": "missing 'message' field",
		})
	}

	message, ok := messageVal.(float64)
	if !ok {
		return s.n.Reply(msg, map[string]any{
			"type": "error",
			"code": "invalid_request",
			"text": "message must be a number",
		})
	}
	messageInt := int(message)

	s.idsMu.Lock()
	defer s.idsMu.Unlock()

	if _, exists := s.ids[messageInt]; exists {
		return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	}

	s.ids[messageInt] = struct{}{}

	s.neighborsMu.RLock()
	neighbors := make([]string, len(s.neighbors))
	copy(neighbors, s.neighbors)
	sender := msg.Src
	s.neighborsMu.RUnlock()

	msgToSend := map[string]any{"type": "broadcast", "message": messageInt}

	for _, neighbor := range neighbors {
		if !strings.HasPrefix(sender, "c") && neighbor == sender {
			continue
		}
		go func(nbr string) {
			if err := s.n.Send(nbr, msgToSend); err != nil {
				log.Printf("Error sending to %s: %v", nbr, err)
			}
		}(neighbor)
	}

	return s.n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) readHandler(msg maelstrom.Message) error {
	s.idsMu.RLock()
	messages := make([]int, 0, len(s.ids))
	for msg := range s.ids {
		messages = append(messages, msg)
	}
	s.idsMu.RUnlock()

	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.neighborsMu.Lock()
	s.neighbors = body.Topology[s.n.ID()]
	s.neighborsMu.Unlock()

	return s.n.Reply(msg, map[string]any{"type": "topology_ok"})
}
