package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	id := 0

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		b := map[string]any{"type": "generate_ok", "id": fmt.Sprintf("%d%s", id, n.ID())}
		id += 1
		return n.Reply(msg, b)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
