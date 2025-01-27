package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type message struct {
	Type    string `json:"type"`
	Content string `json:"content,omitempty"`
}

// main function initializes the maelstrom node and handles the echo service
func main() {
	n := maelstrom.NewNode()
	defer n.Close()

	// Handle the echo message type
	n.Handle("echo", func(msg maelstrom.Message) error {
		var body message
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// Reply to the client with an echo_ok message
		return n.Reply(msg, message{
			Type:    "echo_ok",
			Content: body.Content,
		})
	})

	// Start the node
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
