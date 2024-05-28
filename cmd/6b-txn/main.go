package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type operation struct {
	name  string
	key   int
	value *int
}

type transaction struct {
	Ops       []operation `json:"txn"`
	Timestamp time.Time   `json:"timestamp"`
}

type txnMsg struct {
	dest string
	body map[string]any
}

type entry struct {
	value     int
	timestamp time.Time
}

func main() {
	n := maelstrom.NewNode()

	kv := make(map[int]entry)
	var kvMu sync.RWMutex

	msgs := make(chan txnMsg)

	go func() {
		for msg := range msgs {
			go func(msg txnMsg) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, err := n.SyncRPC(ctx, msg.dest, msg.body)
				if err != nil {
					msgs <- msg
				}
			}(msg)
		}
	}()

	n.Handle("txn", func(msg maelstrom.Message) error {
		var txn transaction
		if err := json.Unmarshal(msg.Body, &txn); err != nil {
			return err
		}

		if txn.Timestamp.IsZero() {
			txn.Timestamp = time.Now()
		}

		for i, op := range txn.Ops {
			if op.name == "r" {
				kvMu.RLock()
				val, ok := kv[op.key]
				kvMu.RUnlock()
				if ok {
					op.value = &val.value
					txn.Ops[i] = op
				}
			} else {
				kvMu.Lock()
				e, ok := kv[op.key]
				if !ok || txn.Timestamp.After(e.timestamp) {
					kv[op.key] = entry{*op.value, txn.Timestamp}
				}
				kvMu.Unlock()
			}
		}

		if msg.Src[0] != 'n' {
			for _, node := range n.NodeIDs() {
				if node == n.ID() {
					continue
				}
				t := transaction{Timestamp: txn.Timestamp}
				for _, op := range txn.Ops {
					if op.name == "w" {
						t.Ops = append(t.Ops, op)
					}
				}
				msgs <- txnMsg{node, map[string]any{"type": "txn", "txn": t.Ops, "timestamp": t.Timestamp}}
			}
		}

		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": txn.Ops})
	})

	err := n.Run()
	close(msgs)
	if err != nil {
		log.Fatal(err)
	}
}

func (o *operation) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{o.name, o.key, o.value})
}

func (o *operation) UnmarshalJSON(data []byte) error {
	var op []any
	if err := json.Unmarshal(data, &op); err != nil {
		return err
	}

	o.name = op[0].(string)
	o.key = int(op[1].(float64))
	val := op[2]
	switch val := val.(type) {
	case float64:
		v := int(val)
		o.value = &v
	}
	return nil
}
