package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type message [2]int

func main() {
	n := maelstrom.NewNode()
	seqkv := maelstrom.NewSeqKV(n)
	linkv := maelstrom.NewLinKV(n)

	createMessage := func(key string, msg int) int {
		for {
			offset, err := linkv.ReadInt(context.Background(), fmt.Sprintf("%s:offset", key))
			var from any
			if err == nil {
				from = offset
				offset += 1
			}
			err = linkv.CompareAndSwap(context.Background(), fmt.Sprintf("%s:offset", key), from, offset, true)
			if err == nil || maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				seqkv.Write(context.Background(), fmt.Sprintf("%s:%d", key, offset), msg)
				return offset
			}
		}

	}

	readMessages := func(key string, offset int) (messages []message) {
		for {
			msg, err := seqkv.ReadInt(context.Background(), fmt.Sprintf("%s:%d", key, offset))
			if err != nil {
				break
			}
			messages = append(messages, message{offset, msg})
			offset += 1
		}
		return messages
	}

	getCommittedOffsets := func(keys []any) map[string]int {
		offsets := make(map[string]int)
		for _, key := range keys {
			key := key.(string)
			offset, err := linkv.ReadInt(context.Background(), key)
			if err == nil {
				continue
			}
			offsets[key] = offset
		}
		return offsets
	}

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		m := int(body["msg"].(float64))
		offset := createMessage(key, m)
		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)
		msgs := make(map[string][]message)
		for key, offset := range offsets {
			ms := readMessages(key, int(offset.(float64)))
			if ms != nil {
				msgs[key] = ms
			}
		}
		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)
		for key, offset := range offsets {
			linkv.Write(context.Background(), fmt.Sprintf("%s:committed_offset", key), offset)
		}
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body["keys"].([]any)
		offsets := getCommittedOffsets(keys)
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
