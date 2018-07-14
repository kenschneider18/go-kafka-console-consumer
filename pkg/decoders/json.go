package decoders

import (
	"encoding/json"
	"errors"

	"github.com/sirupsen/logrus"
)

// JSONDecoder implements the Decoder interface
// and is used to decode Kafka messages to JSON
type JSONDecoder struct {
	Log *logrus.Logger
}

// ValidateSchemas returns nil since JSON has no defined
// schema
func (j *JSONDecoder) ValidateSchemas(schemas string) error {
	return nil
}

// Decode takes a sarama consumermessage
func (j *JSONDecoder) Decode(msg []byte) (interface{}, error) {
	j.Log.Infof("Decoding JSON message...")
	// Any valid JSON is more than 1 byte in length
	if length := len(msg); length < 1 {
		return nil, errors.New("Invalid JSON, length < 1")
	}

	return json.RawMessage(msg), nil
}
