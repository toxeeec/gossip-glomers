package main

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastMsg struct {
	dest string
	body map[string]any
}

func main() {
	n := maelstrom.NewNode()

	var ids []int
	var idsMu sync.RWMutex
	prevIdx := -1

	var neighbors []string

	msgs := make(chan broadcastMsg)

	ticker := time.NewTicker(100 * time.Millisecond)

	go func() {
		for range ticker.C {
			if prevIdx+1 == len(ids) {
				continue
			}
			idsMu.RLock()
			for _, nbor := range neighbors {
				msgs <- broadcastMsg{nbor, map[string]any{"type": "broadcast", "message": ids[prevIdx+1:]}}
			}
			prevIdx = len(ids) - 1
			idsMu.RUnlock()
		}
	}()

	go func() {
		for msg := range msgs {
			go func(msg broadcastMsg) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, err := n.SyncRPC(ctx, msg.dest, msg.body)
				if err != nil {
					msgs <- msg
				}
			}(msg)
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"]
		idsMu.Lock()
		switch message := message.(type) {
		case float64:
			if slices.Index(ids, int(message)) == -1 {
				ids = append(ids, int(message))
			}
		case []any:
			for _, id := range castSlice[float64](message) {
				id := int(id)
				if slices.Index(ids, id) == -1 {
					ids = append(ids, id)
				}
			}
		}
		idsMu.Unlock()
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		idsMu.RLock()
		b := map[string]any{"type": "read_ok", "messages": slices.Clone(ids)}
		idsMu.RUnlock()
		return n.Reply(msg, b)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		neighbors = getNeighbors(n.ID(), n.NodeIDs())
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	err := n.Run()
	ticker.Stop()
	close(msgs)
	if err != nil {
		log.Fatal(err)
	}
}

func getNeighbors(node string, nodes []string) []string {
	nodeIndex, _ := strconv.Atoi(node[1:])
	neighbors := make([]string, 0, 5)
	parentIndex := (nodeIndex - 1) / 4
	if parentIndex != nodeIndex {
		neighbors = append(neighbors, nodes[parentIndex])
	}
	childIndex := nodeIndex * 4
	for i := 1; i <= 4 && childIndex+i < len(nodes); i++ {
		neighbors = append(neighbors, nodes[childIndex+i])
	}
	return neighbors
}

func castSlice[T any](slice []any) []T {
	s := make([]T, 0, len(slice))
	for _, v := range slice {
		s = append(s, v.(T))
	}
	return s
}
