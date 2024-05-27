package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type topologyMsg struct {
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()

	ids := make(map[int]struct{})
	var idsMu sync.RWMutex

	var neighbors []string

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id := int(body["message"].(float64))
		idsMu.Lock()
		if _, ok := ids[id]; !ok {
			ids[id] = struct{}{}
			for _, nbor := range neighbors {
				n.Send(nbor, body)
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
		var t topologyMsg
		if err := json.Unmarshal(msg.Body, &t); err != nil {
			return err
		}

		neighbors = t.Topology[n.ID()]
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func sliceFromSet[T comparable](set map[T]struct{}) []T {
	s := make([]T, 0, len(set))
	for k := range set {
		s = append(s, k)
	}
	return s
}
