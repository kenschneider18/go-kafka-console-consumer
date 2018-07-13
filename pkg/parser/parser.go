package parser

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/sirupsen/logrus"
)

type (
	// Decoder is the interface used by go-kafka-consumer
	// to decode kafka messages into printable interfaces
	Decoder interface {
		// ValidateSchemas takes in the path(s) to schema files
		// and returns an error if the schemas are invalid. If
		// schemas are not required by the decoder implementation
		// return nil
		ValidateSchemas(schemas string) error

		// Decode takes in a Kafka message and returns an interface{}
		// which can be read by json.Marshal() and an error
		Decode(msg *sarama.ConsumerMessage) (interface{}, error)
	}

	// Parser consumes from a Kafka topic, calls
	// message decoders, and prints the message to
	// the console in JSON format
	Parser struct {
		consumer *cluster.Consumer
		topic    string
		decoder  Decoder
		log      *logrus.Logger
	}
)

// New intializes a new Parser struct
func New(consumer *cluster.Consumer, topic string, schemas string, decoder Decoder, log *logrus.Logger) (*Parser, error) {
	err := decoder.ValidateSchemas(schemas)
	if err != nil {
		return nil, err
	}

	return &Parser{
		consumer: consumer,
		decoder:  decoder,
		topic:    topic,
		log:      log,
	}, nil
}

// Serve calls a kafka consumer loop that will listen for
// messages, decode them, and print them to the console
func (p *Parser) Serve() chan struct{} {
	// This will allow the user to exit the loop with
	// Ctrl-C and shut down the consumer
	done := make(chan struct{}, 1)

	go func() {
		messageCount := 0
		for {
			select {
			case msg, more := <-p.consumer.Messages():
				if more {
					// Initial logging here
					p.log.Infof("Offset: %d\n", msg.Offset)
					p.log.Infof("Headers:\n")
					for _, header := range msg.Headers {
						if header != nil {
							p.log.Infof("\t%s: %s\n", string(header.Key), string(header.Value))
						}
					}

					// Use the passed decoder to read the message to a map
					data, err := p.decoder.Decode(msg)
					if err != nil {
						p.log.Errorf("Error decoding message: %s\n", err.Error())
					}

					// Print message as JSON
					p.printJSON(data)
				}
			case err, more := <-p.consumer.Errors():
				if more {
					p.log.Errorf("Error: %s\n", err.Error())
				}
			case notification, more := <-p.consumer.Notifications():
				if more {
					p.log.Warnf("Rebalanced: %+v\n", notification)
				}
			case <-done:
				p.log.Infof("Processed a total of %d messages.\n", messageCount)
				return
			}
		}
	}()

	return done
}

func (p *Parser) printJSON(data interface{}) {
	marshalled, err := json.MarshalIndent(data, "", "     ")
	if err != nil {
		p.log.Errorf("Could not process message: %s\n", err.Error())
		return
	}

	p.log.Infof("Message: %s\n", string(marshalled))
}
