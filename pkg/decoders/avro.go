package decoders

import (
	"io/ioutil"
	"strings"

	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
)

const (
	// ErrReadingSchemaWrapper wraps errors returned while reading the schema file
	ErrReadingSchemaWrapper = "error reading schema %s"
	// ErrCreatingCodecWrapper wraps errors returned while creating the go-avro codec
	ErrCreatingCodecWrapper = "error creating codec for schema %s"
	// ErrDecodingMessageWrapper wraps errors returned decoding the message
	ErrDecodingMessageWrapper = "error decoding message"
)

var (
	// ErrInvalidSchema denotes that the schema file is not in the correct format
	ErrInvalidSchema = errors.New("invalid schema, schemas must be .avsc files")
	// ErrNoCodec denotes that Decode has been called, but no go-avro codec has been created for it
	ErrNoCodec = errors.New("could not find codec. Was ValidateSchemas called yet?")
	// ErrAssertingType denotes that one or more fields failed type assertion
	ErrAssertingType = errors.New("could not decode message, type assertion failed")
)

// AvroDecoder implements the decoder interface
// and can should be able to decode messages for
// most schemas
type AvroDecoder struct {
	Converter Converter
	codec     *goavro.Codec
}

// Converter is an interface type for converting individual
// fields inside of a decoded Avro message to more easily
// readable types. For example, if a field is []byte but is supposed to
// be displayed as JSON, it needs to be converted to
// json.RawMessage so it can be printed properly.
type Converter interface {
	ConvertFields(record map[string]interface{}) error
}

// ValidateSchemas takes in a single Avro schema, validates the
// schema and initializes an go-avro codec to process messages
func (a *AvroDecoder) ValidateSchemas(schemas string) error {
	if !strings.HasSuffix(schemas, ".avsc") {
		return ErrInvalidSchema
	}

	schemaBytes, err := ioutil.ReadFile(schemas)
	if err != nil {
		return errors.Wrapf(err, ErrReadingSchemaWrapper, schemas)
	}

	a.codec, err = goavro.NewCodec(string(schemaBytes))
	if err != nil {
		return errors.Wrapf(err, ErrCreatingCodecWrapper, schemas)
	}

	return nil
}

// Decode takes in an Avro message's value and uses the codec
// created in ValidateSchemas to decode the message
func (a *AvroDecoder) Decode(msg []byte) (interface{}, error) {
	if a.codec == nil {
		return nil, ErrNoCodec
	}

	native, _, err := a.codec.NativeFromBinary(msg)
	if err != nil {
		return nil, errors.Wrapf(err, ErrDecodingMessageWrapper)
	}

	casted, ok := native.(map[string]interface{})
	if !ok {
		return nil, ErrAssertingType
	}

	// Type assert on specific fields so they're
	// printed properly by json.Marshal
	castFields(casted)

	// If a converter is passed in, use it to further
	// decode fields.
	if a.Converter != nil {
		err = a.Converter.ConvertFields(casted)
		if err != nil {
			return nil, err
		}
	}

	return casted, nil
}

// Convert any nested struct values to
// a marshallable format
func castFields(casted map[string]interface{}) {
	for _, value := range casted {
		switch value.(type) {
		case map[string]interface{}:
			castFields(value.(map[string]interface{}))
		case []interface{}:
			elements := value.([]interface{})
			for _, element := range elements {
				switch element.(type) {
				case map[string]interface{}:
					castFields(element.(map[string]interface{}))
				}
			}
		}
	}
}
