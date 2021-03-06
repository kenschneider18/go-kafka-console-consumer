[![Go Report Card](https://goreportcard.com/badge/github.com/kenschneider18/go-kafka-console-consumer)](https://goreportcard.com/report/github.com/kenschneider18/go-kafka-console-consumer)

# go-kafka-console-consumer

go-kafka-console-consumer is an extendable Kafka consumer written in Go that consumes from a given topic and writes the messages to the console as JSON.

There is out of the box support for a few encodings, if your encoding isn't supported you can write your own and pass it in.

### Rationale
In several projects I've worked on, I've found the ability to look and see what's sitting in Kafka to be incredibly useful. Apache saw the usefulness in this too so bundled with Kafka is the script `kafka-console-consumer.sh`. Unfortuantely, if you send the messages encoded in any way the consumer will still spit out messages but they likely won't be readable. This program aims to solve that.

As of this writing there's an open [issue](https://issues.apache.org/jira/browse/KAFKA-2526) to fix this functionality in `kafka-console-consumer.sh`. It could be working now, but since I work mostly in Go anyway I haven't tried it. If you've already written SerDes in Java it's worth trying before you rewrite them in Go.


## Installation
A pre-requisite for running this program is having Go installed on your computer and your `GOPATH` correctly set up.

Because of how this program is packaged (the main package is located in cmd/go-kafka-console-consumer) your `go get` will be a look a little different. See below.

If you don't plan on editing the code and just want to have the consumer available to run from your shell, run this command

```sh
go get github.com/kenschneider18/go-kafka-console-consumer/...
```

This will download the program's source code, compile it, and install the binary on your system's `PATH`.

If you don't want to install the binary right off the bat, clone the repository:

```sh
git clone https://github.com/kenschneider18/go-kafka-console-consumer.git
```

Should you decide to install the binary later you can do so by running the below command from the project's root directory

```sh
go install ./...
```

## Usage

The possible arguments for the program are:

```
  -bootstrap-server (required)
  		Kafka broker URL
  -from-beginning
  		By default the program starts from the latest offset,
  		passing this will start it from the earliest offset
  		 (if you pass a group ID this may not behave
  		 as expected, see `group` below)
  -schemas string
    	If the message type you pass requires schemas,
    	pass them here (The included Avro decoder only
    	supports one schema, custom decoders may take multiple)
  -topic string (required)
    	Kafka topic to consume from
  -type string (required)
    	Either pass a supported type or pass a path to a
		custom decoder
    	Default support:
    		avro
    		msgpack
    		json

*** Experimental ***
  -converter string (only for use with message type `avro`)
  		Optionally pass the path of a compiled plugin
		that will perform additional parsing
  -group string
  		Optionally pass a group ID for your consumer.
  		If the consumer group has connected to Kafka
  		before the broker will ignore requests to start at
  		the earliest offset and will instead consume
  		from the earliest offset not yet read by the
  		group. As far as I'm aware
  		there is no way around this.
```

*if you're really used to the bundled `kafka-console-consumer` passing the arguments with `--` is also supported.

Examples:

With the binary installed in your `PATH`

```
go-kafka-console-consumer -bootstrap-server localhost:9092 -topic test -type msgpack -from-beginning
```

From the project root directory

```
go run cmd/go-kafka-console-consumer/main.go -bootstrap-server localhost:9092 -topic test -type avro -schemas /path/to/schema.avsc
```

### Default Supported Encodings

By default `go-kafka-console-consumer` supports:

- Apache Avro passed as `avro`
- MessagePack passed as `msgpack`
- JSON passed as `json`

## Extendability

This program is written to be extended with Go plugins, if you haven't worked with plugins before here's a good article about them

https://medium.com/learning-the-go-programming-language/writing-modular-go-programs-with-plugins-ec46381ee1a9

### System Requirements

By itself this program will run on any machine capable of running Go binaries (though I have yet to test it on a Windows machine). However, plugins are still new so there's limited support for them.

Operating System: Linux or macOS
Go version: 1.8+

### Writing a Plugin

To work with `go-kafka-console-consumer` a plugin must implement this interface

```go
	Decoder interface {
		ValidateSchemas(schemas string) error
		Decode([]byte) (interface{}, error)
	}
```
and have a variable exposed called `Decoder` that is an instance of that implementation. The package name for any Go plugin must be `main`.

`ValidateSchemas` will be passed an unparsed string containing the schema(s). Perform any validation logic on the schema(s) here, if schemas aren't required create a function that returns `nil`.

`Decode` will be passed the `Value` of the Kafka message. This function ***MUST*** return an interface that can be marshalled into JSON using Go's standard library. (Ideally, the entire Kafka message would be passed but we're limited by Go plugins. This will be fixed in a future update.)

An example of the MessagePack decoder as a plugin can be found in the examples directory.

#### Converter Plugins (Experimental)

Like decoder plugins, converters must implement an interface to work

```go
	type Converter interface {
		ConvertFields(record map[string]interface{}) error
	}
```

Converter plugins must expose an instance with the variable name `Converter`.

`ConvertFields` will be passed the decoded record as a `map[string]interface{}`. This function should parse the record and type assert fields as necessary so they can be better represented in the console. I will be adding an example converter soon.

#### Compiling plugins

```
go build -buildmode=plugin -o /path/to/a/plugin.so /path/to/a/plugin/source.go
```

### Running with Plugins

Instead of passing one of default types, pass the path to a compiled plugin.

```
go-kafka-console-consumer -bootstrap-server localhost:9092 -topic test -type /path/to/a/plugin.so -from-beginning
```

Assuming the plugin is implemented correctly it should work just like that!
