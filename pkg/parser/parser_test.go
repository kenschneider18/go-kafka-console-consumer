package parser_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/kenschneider18/go-kafka-consumer/pkg/parser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	testDecoder struct {
		shouldValidate bool
		shouldDecode   bool
	}

	testConsumer struct {
		Msgs   chan *sarama.ConsumerMessage
		Notifs chan *cluster.Notification
		Errs   chan error
	}
)

var (
	ErrTestValidateSchemas = errors.New("validate schemas test error")
	ErrTestDecodeFailed    = errors.New("decode test error")
	ErrTestErrs            = errors.New("errors channel error")
	loggedDecodeFailed     = fmt.Sprintf("Error decoding message: %s", ErrTestDecodeFailed.Error())
	loggedErrTestErrs      = fmt.Sprintf("Error: %s", ErrTestErrs.Error())
)

const (
	testHeaderKey      = "testHeaderKey"
	testHeaderValue    = "testHeaderValue"
	testJSONMsgValue   = `{"testMessage": "someJSON", "anotherTest": 1}`
	loggedJSONValue    = "Message:\n{\n    \"testMessage\": \"someJSON\",\n    \"anotherTest\": 1\n}"
	loggedNotification = "Rebalanced: &{Type:unknown Claimed:map[] Released:map[] Current:map[]}"
)

func TestNew(t *testing.T) {
	var consumer *cluster.Consumer
	decoder := &testDecoder{} // take advantage of false by default
	log, _ := test.NewNullLogger()

	parser, err := parser.New(consumer, "topic", "schemas", decoder, log)

	assert.Nil(t, parser)
	assert.NotNil(t, err)
	assert.Equal(t, ErrTestValidateSchemas, err)
}

func TestServeWithError(t *testing.T) {
	errs := make(chan error)
	defer close(errs)
	consumer := &testConsumer{
		Errs: errs,
	}
	decoder := &testDecoder{
		shouldValidate: true,
	}
	log, hook := test.NewNullLogger()

	parser, err := parser.New(consumer, "topic", "schemas", decoder, log)

	require.Nil(t, err)
	require.NotNil(t, parser)

	// Start the serve loop
	done := parser.Serve()

	// Send error on the errors channel
	errs <- ErrTestErrs
	time.Sleep(time.Duration(1) * time.Second)
	done <- struct{}{}

	logs := hook.AllEntries()
	require.Equal(t, 1, len(logs))
	assert.Equal(t, logrus.ErrorLevel, logs[0].Level)
	assert.Equal(t, loggedErrTestErrs, logs[0].Message)
}

func TestServeWithNotification(t *testing.T) {
	notifs := make(chan *cluster.Notification)
	defer close(notifs)
	consumer := &testConsumer{
		Notifs: notifs,
	}
	decoder := &testDecoder{
		shouldValidate: true,
	}
	log, hook := test.NewNullLogger()

	parser, err := parser.New(consumer, "topic", "schemas", decoder, log)

	require.Nil(t, err)
	require.NotNil(t, parser)

	// Start the serve loop
	done := parser.Serve()

	// Send notification on notifications
	// channel
	notifs <- &cluster.Notification{}
	time.Sleep(time.Duration(1) * time.Second)
	done <- struct{}{}

	logs := hook.AllEntries()
	require.Equal(t, 1, len(logs))
	assert.Equal(t, logrus.WarnLevel, logs[0].Level)
	assert.Equal(t, loggedNotification, logs[0].Message)
}

func TestServeWithGoodMessage(t *testing.T) {
	msgs := make(chan *sarama.ConsumerMessage)
	defer close(msgs)
	consumer := &testConsumer{
		Msgs: msgs,
	}
	decoder := &testDecoder{
		shouldValidate: true,
		shouldDecode:   true,
	}
	log, hook := test.NewNullLogger()

	parser, err := parser.New(consumer, "topic", "schemas", decoder, log)

	require.Nil(t, err)
	require.NotNil(t, parser)

	// Start the serve loop
	done := parser.Serve()

	// Send message on messages channel
	//
	msgs <- &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{
			&sarama.RecordHeader{
				Key:   []byte(testHeaderKey),
				Value: []byte(testHeaderValue),
			},
		},
		Offset: 0,
		Value:  []byte(testJSONMsgValue),
	}
	time.Sleep(time.Duration(1) * time.Second)
	done <- struct{}{}

	logs := hook.AllEntries()
	require.Equal(t, 4, len(logs))
	assert.Equal(t, logrus.InfoLevel, logs[0].Level)
	assert.Equal(t, "Offset: 0", logs[0].Message)
	assert.Equal(t, logrus.InfoLevel, logs[1].Level)
	assert.Equal(t, "Headers:", logs[1].Message)
	assert.Equal(t, logrus.InfoLevel, logs[2].Level)
	assert.Equal(t, fmt.Sprintf("\t%s: %s", testHeaderKey, testHeaderValue), logs[2].Message)
	assert.Equal(t, logrus.InfoLevel, logs[3].Level)
	assert.Equal(t, loggedJSONValue, logs[3].Message)
}

func TestServeDecodeFailure(t *testing.T) {
	msgs := make(chan *sarama.ConsumerMessage)
	defer close(msgs)
	consumer := &testConsumer{
		Msgs: msgs,
	}
	decoder := &testDecoder{
		shouldValidate: true,
		shouldDecode:   false,
	}
	log, hook := test.NewNullLogger()

	parser, err := parser.New(consumer, "topic", "schemas", decoder, log)

	require.Nil(t, err)
	require.NotNil(t, parser)

	// Start the serve loop
	done := parser.Serve()

	// Send message on messages channel
	//
	msgs <- &sarama.ConsumerMessage{
		Headers: []*sarama.RecordHeader{
			&sarama.RecordHeader{
				Key:   []byte(testHeaderKey),
				Value: []byte(testHeaderValue),
			},
		},
		Offset: 0,
		Value:  []byte(testJSONMsgValue),
	}
	time.Sleep(time.Duration(1) * time.Second)
	done <- struct{}{}

	logs := hook.AllEntries()
	require.Equal(t, 4, len(logs))
	assert.Equal(t, logrus.InfoLevel, logs[0].Level)
	assert.Equal(t, "Offset: 0", logs[0].Message)
	assert.Equal(t, logrus.InfoLevel, logs[1].Level)
	assert.Equal(t, "Headers:", logs[1].Message)
	assert.Equal(t, logrus.InfoLevel, logs[2].Level)
	assert.Equal(t, fmt.Sprintf("\t%s: %s", testHeaderKey, testHeaderValue), logs[2].Message)
	assert.Equal(t, logrus.ErrorLevel, logs[3].Level)
	assert.Equal(t, loggedDecodeFailed, logs[3].Message)
}

func (t *testDecoder) ValidateSchemas(schemas string) error {
	if t.shouldValidate {
		return nil
	}

	return ErrTestValidateSchemas
}

func (t *testDecoder) Decode(msg []byte) (interface{}, error) {
	if t.shouldDecode {
		return json.RawMessage(msg), nil
	}

	return nil, ErrTestDecodeFailed
}

func (t *testConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return t.Msgs
}

func (t *testConsumer) Notifications() <-chan *cluster.Notification {
	return t.Notifs
}

func (t *testConsumer) Errors() <-chan error {
	return t.Errs
}
