package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"plugin"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/kenschneider18/go-kafka-console-consumer/pkg/decoders"
	"github.com/kenschneider18/go-kafka-console-consumer/pkg/parser"
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
	errNoType      = errors.New("a message type or path to type plugin is required")
	errNoSchemas   = errors.New("a schema is required for message type Avro")
	supportedTypes = []string{
		"avro",
		"msgpack",
		"json",
	}
)

type TLSConfigurator interface {
	GetConfig() (*tls.Config, error)
}

func main() {
	// Read config from command line
	brokers := flag.String("bootstrap-server", "", "Comma separated Kafka Broker URLs")
	topic := flag.String("topic", "", "Topic name")
	groupID := flag.String("group", "", "Optional, pass the Kafka GroupId")
	fromBeginning := flag.Bool("from-beginning", false, "Optional, if passed the program will start at the earliest offset")
	msgType := flag.String("type", "",
		fmt.Sprintf("Pass the supported type name here or the path to your plugin. Out of the box supported types are %s", strings.Join(supportedTypes, ", ")))
	schemas := flag.String("schemas", "", "If the message type uses schemas, pass them here.")
	converterPath := flag.String("converter", "", "Optional, pass a converter plugin to convert addition fields for avro messages")
	tlsConfigPath := flag.String("tls-configurator", "", "Optional, pass a tls plugin to grab your TLS configuration on the fly")
	clientCert := flag.String("client-cert", "", "Optional, pass client cert path for TLS")
	clientKey := flag.String("client-key", "", "Optional, pass client key path for TLS")
	caCert := flag.String("ca-cert", "", "Optional, pass CA cert path for TLS")

	flag.Parse()

	err := checkArgs(brokers, topic, groupID, msgType, schemas)
	if err != nil {
		log.Fatalf("Could not validate args: %s", err.Error())
	}

	tlsConfig, err := getTLSConfig(tlsConfigPath, clientCert, clientKey, caCert)
	if err != nil {
		log.Fatalf("Failed to create TLS config: %s", err.Error())
	}

	brokersSlice := strings.Split(*brokers, ",")

	// Create a new consumer, blocks until connection to brokers established
	consumer := newConsumer(brokersSlice, *topic, *groupID, *fromBeginning, tlsConfig)

	decoder := getDecoder(*msgType, *converterPath)

	parser, err := parser.New(consumer, *topic, *schemas, decoder, log)
	if err != nil {
		log.Fatalf("Could not initialize parser: %s", err.Error())
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

	if *msgType == "" {
		return errNoType
	}

	if strings.EqualFold(*msgType, "avro") && *schemas == "" {
		return errNoSchemas
	}

	return nil
}

func getDecoder(msgType, converterPath string) parser.Decoder {
	if msgType == "json" {
		return &decoders.JSONDecoder{
			Log: log,
		}
	} else if msgType == "msgpack" {
		return &decoders.MsgPackDecoder{}
	} else if msgType == "avro" {
		// Look to see if a converter has been passed
		var converter decoders.Converter
		if converterPath != "" {
			symConverter, err := loadPlugin(converterPath, "Converter")
			if err != nil {
				log.Fatalf("Error loading avro converter: %s\n", err.Error())
			}

			converter = symConverter.(decoders.Converter)
		}
		return &decoders.AvroDecoder{
			Converter: converter,
		}
	}

	symDecoder, err := loadPlugin(msgType, "Decoder")
	if err != nil {
		log.Fatalf("Error loading %s decoder: %s\n", msgType, err.Error())
	}

	// Assert Decoder variable conforms to the
	// parser.Decoder interface. Not bothering
	// to use safe method because the panic is
	// more descriptive.
	decoder := symDecoder.(parser.Decoder)
	// don't use 'ok' to check the assertion,
	// the message displayed by the panic is
	// more useful than a prettier error message here

	return decoder
}

func newConsumer(brokers []string, topic string, groupID string, fromBeginning bool, tlsConfig *tls.Config) *cluster.Consumer {
	// Sarama cluster config
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Version = sarama.V0_11_0_0

	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}

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
		log.Errorf("Unable to start consumer: %s", err.Error())
		log.Errorf("Backing off for %d ms...", backoff/time.Millisecond)
		time.Sleep(backoff)
	}

	return consumer
}

func getTLSConfig(tlsConfigPath, clientCertFile, clientKeyFile, caCertFile *string) (*tls.Config, error) {
	var tlsConfig *tls.Config
	var tlsSymbol plugin.Symbol
	var err error
	if *tlsConfigPath != "" {
		tlsSymbol, err = loadPlugin(*tlsConfigPath, "TLS")
		if err != nil {
			return nil, err
		}

		// If the plugin interface is wrong, panic because the
		// error message is more descriptive
		tlsConfigurator := tlsSymbol.(TLSConfigurator)

		// Get the TLS config however it's supposed to be done
		tlsConfig, err = tlsConfigurator.GetConfig()
		if err != nil {
			return nil, err
		}
	} else if *clientCertFile != "" && *clientKeyFile != "" && *caCertFile != "" {
		tlsConfig, err = newTLSConfig(*clientCertFile, *clientKeyFile, *caCertFile)
		if err != nil {
			return nil, err
		}
	}

	return tlsConfig, nil
}

func newTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}

func loadPlugin(pluginPath, symbolName string) (plugin.Symbol, error) {
	plug, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, err
	}

	symbol, err := plug.Lookup(symbolName)
	if err != nil {
		return nil, err
	}

	return symbol, nil
}
