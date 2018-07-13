package decoders

import (
	"encoding/json"
	"errors"

	"github.com/Shopify/sarama"
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
func (j *JSONDecoder) Decode(msg *sarama.ConsumerMessage) (interface{}, error) {
	j.Log.Infof("Decoding JSON message...\n")
	// Any valid JSON is more than 1 byte in length
	if length := len(msg.Value); length < 1 {
		return nil, errors.New("Invalid JSON, length < 1")
	}

	return json.RawMessage(msg.Value), nil
}
