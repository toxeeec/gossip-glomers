package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type message [2]int

type sendOkMsg struct {
	Offset int `json:"offset"`
}

type pollOkMsg struct {
	Msgs map[string][]message `json:"msgs"`
}

type listCommittedOffsetsOkMsg struct {
	Offsets map[string]int `json:"offsets"`
}

func main() {
	n := maelstrom.NewNode()
	seqkv := maelstrom.NewSeqKV(n)
	var seqkvMu sync.Mutex

	getOwner := func(key string) string {
		keyId, _ := strconv.Atoi(key)
		ownerId := keyId % len(n.NodeIDs())
		return fmt.Sprintf("n%d", ownerId)
	}

	readMessages := func(offsets map[string]any) map[string][]message {
		msgs := make(map[string][]message, len(offsets))
		offsetsByOwner := make(map[string]map[string]int, len(n.NodeIDs()))
		for key, offset := range offsets {
			offset := int(offset.(float64))
			owner := getOwner(key)
			offsets, ok := offsetsByOwner[owner]
			if ok {
				offsets[key] = offset
			} else {
				offsets = map[string]int{key: offset}
			}
			offsetsByOwner[owner] = offsets
		}

		for owner, offsets := range offsetsByOwner {
			if owner == n.ID() {
				for key, offset := range offsets {
					var ms []message
					seqkv.ReadInto(context.Background(), fmt.Sprintf("%s:messages", key), &ms)
					i, ok := slices.BinarySearchFunc(ms, offset, func(msg message, off int) int {
						return cmp.Compare(msg[0], off)
					})
					if ok {
						msgs[key] = ms[i:]
					} else {
						msgs[key] = []message{}
					}
				}
			} else {
				b := map[string]any{"type": "poll", "offsets": offsets}
				msg, _ := n.SyncRPC(context.Background(), owner, b)
				var body pollOkMsg
				json.Unmarshal(msg.Body, &body)
				for key, ms := range body.Msgs {
					msgs[key] = ms
				}
			}
		}
		return msgs
	}

	createMessage := func(key string, msg int) (offset int) {
		owner := getOwner(key)

		if owner == n.ID() {
			seqkvMu.Lock()
			msgs := readMessages(map[string]any{key: 0.0})[key]
			if len(msgs) > 0 {
				offset = msgs[len(msgs)-1][0] + 1
			}
			msgs = append(msgs, message{offset, msg})
			seqkv.Write(context.Background(), fmt.Sprintf("%s:messages", key), msgs)
			seqkvMu.Unlock()
		} else {
			b := map[string]any{"type": "send", "key": key, "msg": msg}
			msg, _ := n.SyncRPC(context.Background(), owner, b)
			var body sendOkMsg
			json.Unmarshal(msg.Body, &body)
			offset = body.Offset
		}
		return offset
	}

	commitOffsets := func(offsets map[string]any) {
		offsetsByOwner := make(map[string]map[string]int, len(n.NodeIDs()))
		for key, offset := range offsets {
			offset := int(offset.(float64))
			owner := getOwner(key)
			offsets, ok := offsetsByOwner[owner]
			if ok {
				offsets[key] = offset
			} else {
				offsets = map[string]int{key: offset}
			}
			offsetsByOwner[owner] = offsets
		}

		for owner, offsets := range offsetsByOwner {
			if owner == n.ID() {
				seqkvMu.Lock()
				var committed map[string]int
				if err := seqkv.ReadInto(context.Background(), fmt.Sprintf("%s:committed_offsets", n.ID()), &committed); err != nil {
					committed = make(map[string]int, len(offsets))
				}
				for key, offset := range offsets {
					committed[key] = offset
				}
				seqkv.Write(context.Background(), fmt.Sprintf("%s:committed_offsets", n.ID()), committed)
				seqkvMu.Unlock()
			} else {
				body := map[string]any{"type": "commit_offsets", "offsets": offsets}
				n.Send(owner, body)
			}
		}
	}

	listCommittedOffsets := func(keys []any) map[string]int {
		offsets := make(map[string]int, len(keys))
		keysByOwner := make(map[string][]string, len(n.NodeIDs()))
		for _, key := range keys {
			key := key.(string)
			owner := getOwner(key)
			keys, ok := keysByOwner[owner]
			if ok {
				keys = append(keys, key)
			} else {
				keys = []string{key}
			}
			keysByOwner[owner] = keys
		}

		for owner, keys := range keysByOwner {
			if owner == n.ID() {
				var committed map[string]int
				seqkv.ReadInto(context.Background(), fmt.Sprintf("%s:committed_offsets", n.ID()), &committed)
				for _, key := range keys {
					offset, ok := committed[key]
					if ok {
						offsets[key] = offset
					}
				}
			} else {
				b := map[string]any{"type": "list_committed_offsets", "keys": keys}
				msg, _ := n.SyncRPC(context.Background(), owner, b)
				var body listCommittedOffsetsOkMsg
				json.Unmarshal(msg.Body, &body)
				for key, offset := range body.Offsets {
					offsets[key] = offset
				}
			}
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
		msgs := readMessages(offsets)
		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)
		commitOffsets(offsets)
		if msg.Src[0] == 'n' {
			return nil
		}
		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body["keys"].([]any)
		offsets := listCommittedOffsets(keys)
		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
