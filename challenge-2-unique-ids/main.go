package main

import (
	"encoding/json"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	n *maelstrom.Node
	// Use node ID as high bits, counter as low bits
	nodeID  int64
	counter atomic.Int64
	nodeIDs map[string]int64 // Cache of node ID to numeric ID mapping
}

func NewNode(n *maelstrom.Node) *Node {
	return &Node{
		n:       n,
		nodeIDs: make(map[string]int64),
	}
}

func (n *Node) init() error {
	// Initialize our node ID based on position in node list
	topology, err := n.n.GetTopology()
	if err != nil {
		return err
	}

	// Assign each node a unique number from 0 to n-1
	var myID int64
	i := int64(0)
	for nodeID := range topology {
		n.nodeIDs[nodeID] = i
		if nodeID == n.n.ID() {
			myID = i
		}
		i++
	}
	n.nodeID = myID
	return nil
}

func (n *Node) generateID() int64 {
	// Get next counter value
	count := n.counter.Add(1)

	// Shift node ID into high bits, combine with counter in low bits
	// This ensures uniqueness across nodes without coordination
	return (n.nodeID << 32) | count
}

func main() {
	n := NewNode(maelstrom.NewNode())

	n.n.Handle("init", func(msg maelstrom.Message) error {
		return n.init()
	})

	n.n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := n.generateID()

		body["type"] = "generate_ok"
		body["id"] = id

		return n.n.Reply(msg, body)
	})

	if err := n.n.Run(); err != nil {
		panic(err)
	}
}
