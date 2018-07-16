package decoders

import (
	"github.com/vmihailenco/msgpack"
)

// MsgPackDecoder decodes kafka messages written in
// MsgPack
type MsgPackDecoder struct{}

// ValidateSchemas returns nil since schemas are not
// required for MsgPack
func (m *MsgPackDecoder) ValidateSchemas(schemas string) error {
	return nil
}

// Decode returns a decoded MessagePack message
func (m *MsgPackDecoder) Decode(msg []byte) (interface{}, error) {
	var decoded map[string]interface{}
	err := msgpack.Unmarshal(msg, &decoded)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}
