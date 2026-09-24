package node

import "encoding/json"

// Command is a store operation that gets replicated through raft.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Operation names used in Command.Op.
const (
	OpSet = "set"
	OpDel = "del"
)

// Encode turns a command into bytes for the raft log.
func Encode(c Command) ([]byte, error) {
	return json.Marshal(c)
}

// Decode parses bytes from the raft log back into a command.
func Decode(data []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(data, &c)
	return c, err
}
