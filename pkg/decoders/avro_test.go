package decoders_test

import (
	"errors"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/linkedin/goavro"

	"github.com/kenschneider18/go-kafka-consumer/pkg/decoders"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	testConverter string
)

var (
	errConvertingFailed = errors.New("test error")
)

const (
	invalidExtension       = "test.json"
	errInvalidExtensionMsg = "invalid schema, schemas must be .avsc files"
	fakePath               = "fake_path.avsc"
	errReadingSchemaMsg    = "error reading schema fake_path.avsc: open fake_path.avsc: no such file or directory"
	pathToInvalidSchema    = "../../etc/tests/invalid_schema.avsc"
	errInvalidSchemaMsg    = "error creating codec for schema ../../etc/tests/invalid_schema.avsc: Record \"com.example.FullName\" field 3 ought to be valid Avro named type: unknown type name: \"notAType\""
	pathToTestSchema       = "../../etc/tests/test_schema.avsc"
	errDecodingMsg         = "error decoding message: cannot decode binary record \"com.example.FullName\" field \"firstName\": cannot decode binary string: cannot decode binary bytes: short buffer"
)

func TestValidateSchemasInvalidFileExtension(t *testing.T) {
	decoder := &decoders.AvroDecoder{}

	err := decoder.ValidateSchemas(invalidExtension)

	require.NotNil(t, err)
	assert.Equal(t, errInvalidExtensionMsg, err.Error())
}

func TestValidateSchemasFileNotFound(t *testing.T) {
	decoder := &decoders.AvroDecoder{}

	err := decoder.ValidateSchemas(fakePath)

	require.NotNil(t, err)
	assert.Equal(t, errReadingSchemaMsg, err.Error())
}

func TestValidateSchemasInvlidSchema(t *testing.T) {
	decoder := &decoders.AvroDecoder{}

	err := decoder.ValidateSchemas(pathToInvalidSchema)

	require.NotNil(t, err)
	assert.Equal(t, errInvalidSchemaMsg, err.Error())
}

func TestDecodeNoCodec(t *testing.T) {
	decoder := &decoders.AvroDecoder{}

	decoded, err := decoder.Decode([]byte(""))

	require.Nil(t, decoded)
	require.NotNil(t, err)
	assert.Equal(t, decoders.ErrNoCodec, err)
}

func TestDecodeErrDecodingMsg(t *testing.T) {
	decoder := &decoders.AvroDecoder{}

	err := decoder.ValidateSchemas(pathToTestSchema)
	require.Nil(t, err)

	decoded, err := decoder.Decode([]byte(""))

	require.Nil(t, decoded)
	require.NotNil(t, err)
	assert.Equal(t, errDecodingMsg, err.Error())
}

func TestDecodeConverterFailure(t *testing.T) {
	schemaBytes, err := ioutil.ReadFile(pathToTestSchema)
	require.Nil(t, err)
	require.NotEmpty(t, schemaBytes)

	codec, err := goavro.NewCodec(string(schemaBytes))
	require.Nil(t, err)
	require.NotNil(t, codec)

	native := map[string]interface{}{
		"firstName": "first",
		"lastName":  "last",
		"json":      []byte("{\"testing\": 123, \"more\": \"isHere\"}"),
	}

	binary, err := codec.BinaryFromNative(nil, native)
	require.Nil(t, err)
	require.NotEmpty(t, binary)

	converter := testConverter("test")
	decoder := &decoders.AvroDecoder{
		Converter: &converter,
	}
	err = decoder.ValidateSchemas(pathToTestSchema)
	require.Nil(t, err)

	decoded, err := decoder.Decode(binary)
	require.NotNil(t, err)
	assert.Nil(t, decoded)
	assert.Equal(t, errConvertingFailed, err)
}

func TestDecodeSuccessNoConverter(t *testing.T) {
	schemaBytes, err := ioutil.ReadFile(pathToTestSchema)
	require.Nil(t, err)
	require.NotEmpty(t, schemaBytes)

	codec, err := goavro.NewCodec(string(schemaBytes))
	require.Nil(t, err)
	require.NotNil(t, codec)

	native := map[string]interface{}{
		"firstName": "first",
		"lastName":  "last",
		"json":      []byte("{}"),
	}

	binary, err := codec.BinaryFromNative(nil, native)
	require.Nil(t, err)
	require.NotEmpty(t, binary)

	decoder := &decoders.AvroDecoder{}
	err = decoder.ValidateSchemas(pathToTestSchema)
	require.Nil(t, err)

	decoded, err := decoder.Decode(binary)
	require.Nil(t, err)
	require.NotNil(t, decoded)

	casted, ok := decoded.(map[string]interface{})
	require.True(t, ok)
	require.NotNil(t, casted)

	eq := reflect.DeepEqual(native, casted)
	assert.True(t, eq)
}

func (t *testConverter) ConvertFields(record map[string]interface{}) error {
	return errConvertingFailed
}
