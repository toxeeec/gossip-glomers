package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	readLocalValue := func() (int, error) {
		return retry(3, func(ctx context.Context) (int, error) {
			val, err := kv.ReadInt(ctx, n.ID())
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				return 0, nil
			}
			return val, err
		})
	}

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		val, err := readLocalValue()
		if err != nil {
			return err
		}
		if _, err := retry(3, func(ctx context.Context) (struct{}, error) {
			return struct{}{}, kv.Write(ctx, n.ID(), delta+val)
		}); err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var sum int
		for _, node := range n.NodeIDs() {
			var val int
			var err error
			if node == n.ID() {
				val, err = readLocalValue()
			} else {
				val, err = retry(3, func(ctx context.Context) (int, error) {
					msg, err := n.SyncRPC(ctx, node, map[string]any{"type": "local"})
					if err != nil {
						return 0, err
					}
					var body map[string]any
					if err := json.Unmarshal(msg.Body, &body); err != nil {
						return 0, err
					}
					val = int(body["value"].(float64))
					return val, nil
				})
			}
			if err != nil {
				return err
			}
			sum += val
		}
		return n.Reply(msg, map[string]any{"type": "read_ok", "value": sum})
	})

	n.Handle("local", func(msg maelstrom.Message) error {
		val, err := readLocalValue()
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "local_ok", "value": val})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func retry[T any](attempts int, f func(ctx context.Context) (T, error)) (val T, err error) {
	timeout := 500 * time.Millisecond
	for range attempts {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		val, err = f(ctx)
		if err == nil {
			break
		}
		timeout *= 2
	}
	return val, err
}
