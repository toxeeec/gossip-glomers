package main

import (
	"cmp"
	"encoding/json"
	"log"
	"slices"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type message [2]int

func main() {
	n := maelstrom.NewNode()

	messages := make(map[string][]message)
	var messagesMu sync.RWMutex

	committedOffsets := make(map[string]int)
	var committedOffsetsMu sync.RWMutex

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		m := int(body["msg"].(float64))

		messagesMu.Lock()
		var offset int
		msgs, ok := messages[key]
		if ok {
			offset = msgs[len(msgs)-1][0] + 1
		}
		msgs = append(msgs, message{offset, m})
		messages[key] = msgs
		messagesMu.Unlock()
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)
		msgs := make(map[string][]message)

		messagesMu.RLock()
		for key, offset := range offsets {
			ms, ok := messages[key]
			if !ok {
				continue
			}
			i, ok := slices.BinarySearchFunc(ms, int(offset.(float64)), func(m message, off int) int {
				return cmp.Compare(m[0], off)
			})
			if !ok {
				continue
			}
			msgs[key] = ms[i:]
		}
		messagesMu.RUnlock()
		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)

		committedOffsetsMu.Lock()
		for key, offset := range offsets {
			offset := int(offset.(float64))
			committed := committedOffsets[key]
			if offset > committed {
				committed = offset
			}
			committedOffsets[key] = committed
		}
		committedOffsetsMu.Unlock()
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body["keys"].([]any)
		offsets := make(map[string]int, len(keys))

		committedOffsetsMu.RLock()
		for _, key := range keys {
			key := key.(string)
			committed, ok := committedOffsets[key]
			if !ok {
				continue
			}
			offsets[key] = committed
		}
		committedOffsetsMu.RUnlock()

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
