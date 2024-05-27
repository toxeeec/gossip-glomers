package main

import (
	"context"
	"encoding/json"
	"log"
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

	ids := make(map[float64]struct{})
	var idsMu sync.RWMutex

	var neighbors []string

	msgs := make(chan broadcastMsg)

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

		id := body["message"].(float64)
		idsMu.Lock()
		if _, ok := ids[id]; !ok {
			ids[id] = struct{}{}
			for _, nbor := range neighbors {
				if nbor == msg.Src {
					continue
				}
				msgs <- broadcastMsg{nbor, body}
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
		b := map[string]any{"type": "read_ok", "messages": sliceFromSet(ids)}
		idsMu.RUnlock()
		return n.Reply(msg, b)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		neighbors = getNeighbors(n.ID(), n.NodeIDs())
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	err := n.Run()
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

func sliceFromSet[T comparable](set map[T]struct{}) []T {
	s := make([]T, 0, len(set))
	for k := range set {
		s = append(s, k)
	}
	return s
}
