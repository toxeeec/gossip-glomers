package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type operation struct {
	name  string
	key   int
	value *int
}

type txnMsg struct {
	Txn []operation `json:"txn"`
}

func main() {
	n := maelstrom.NewNode()

	kv := make(map[int]int)
	var kvMu sync.RWMutex

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body txnMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for i, op := range body.Txn {
			if op.name == "r" {
				kvMu.RLock()
				val, ok := kv[op.key]
				kvMu.RUnlock()
				if ok {
					op.value = &val
					body.Txn[i] = op
				}
			} else {
				kvMu.Lock()
				kv[op.key] = *op.value
				kvMu.Unlock()
			}
		}
		return n.Reply(msg, map[string]any{"type": "txn_ok", "txn": body.Txn})
	})

	if err := n.Run(); err != nil {
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
	case nil:
		o.value = nil
	}
	return nil
}
