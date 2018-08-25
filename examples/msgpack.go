package main

import (
	"github.com/vmihailenco/msgpack"
)

// MsgPackDecoder decodes kafka messages written in
// MsgPack
type msgPackDecoder string

// Decoder variable that will be linked
// in the main program
var Decoder msgPackDecoder

// ValidateSchemas returns nil since schemas are not
// required for MsgPack
func (m *msgPackDecoder) ValidateSchemas(schemas string) error {
	return nil
}

// Decode returns a decoded MessagePack message
func (m *msgPackDecoder) Decode(msg []byte) (interface{}, error) {
	var decoded map[string]interface{}
	err := msgpack.Unmarshal(msg, &decoded)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}
