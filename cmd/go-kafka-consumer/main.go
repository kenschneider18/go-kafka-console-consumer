package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/kenschneider18/go-kafka-consumer/pkg/decoders"
	"github.com/kenschneider18/go-kafka-consumer/pkg/parser"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

const (
	defaultConfigPath = "etc/config.yaml"
)

var (
	log            = logrus.New()
	errNoBrokers   = errors.New("at least one broker URL is required")
	errNoTopic     = errors.New("a topic is required")
	errNoSchemas   = errors.New("a schema is required for message type Avro")
	supportedTypes = []string{
		"avro",
		"msgpack",
		"json",
	}
)

func main() {
	// Read config from command line
	brokers := flag.String("bootstrap-server", "", "Comma separated Kafka Broker URLs")
	topic := flag.String("topic", "", "Topic name")
	groupID := flag.String("group", "", "Optional, pass the Kafka GroupId")
	fromBeginning := flag.Bool("fromBeginning", false, "Optional, if passed the program will start at the earliest offset")
	msgType := flag.String("type", "avro",
		fmt.Sprintf("Pass the supported type name here or the path to your plugin. Out of the box supported types are %s", strings.Join(supportedTypes, ", ")))
	schemas := flag.String("schemas", "", "If the message type uses schemas, pass them here.")

	err := checkArgs(brokers, topic, groupID, msgType, schemas)
	if err != nil {
		log.Fatalf("Could not validate args: %s\n", err.Error())
	}

	brokersSlice := strings.Split(*brokers, ",")

	// Create a new consumer, blocks until connection to brokers established
	consumer := newConsumer(brokersSlice, *topic, *groupID, *fromBeginning)

	decoder := getDecoder(*msgType)

	parser, err := parser.New(consumer, *topic, *schemas, decoder, log)
	if err != nil {
		log.Fatalf("Could not initialize parser: %s\n", err.Error())
	}
	done := parser.Serve()

	// Keep program running until the user
	// triggers a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	<-signals

	// Send a signal to done to trigger parser
	// shutdown
	done <- struct{}{}
}

func checkArgs(brokers, topic, groupID, msgType, schemas *string) error {
	if *brokers == "" {
		return errNoBrokers
	}

	if *topic == "" {
		return errNoTopic
	}

	if strings.EqualFold(*msgType, "avro") && *schemas == "" {
		return errNoSchemas
	}

	return nil
}

func getDecoder(msgType string) parser.Decoder {
	return &decoders.JSONDecoder{
		Log: log,
	}
}

func newConsumer(brokers []string, topic string, groupID string, fromBeginning bool) *cluster.Consumer {
	// Sarama cluster config
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Version = sarama.V0_11_0_0

	if fromBeginning {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if groupID == "" {
		groupID = uuid.NewV4().String()
	}

	// Sarama cluster accepts multiple topics,
	// this doesn't.
	topics := []string{
		topic,
	}

	var counter = 1.
	var consumer *cluster.Consumer
	var err error

	// Attempt to connect to brokers forever w/ exponential backoff
	for {
		consumer, err = cluster.NewConsumer(brokers, groupID, topics, config)
		if err == nil {
			break
		}

		backoff := 100 * time.Millisecond * time.Duration(math.Pow(2, counter))
		counter++
		log.Errorf("Unable to start consumer: %s\n", err.Error())
		log.Errorf("Backing off for %d ms...\n", backoff/time.Millisecond)
		time.Sleep(backoff)
	}

	return consumer
}
