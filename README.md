# go-kafka-consumer

go-kafka-consumer is an extendable Kafka console consumer written in golang that consumes from a given topic and writes the messages to the console in as JSON.

There is out of the box support for several encodings, if your encoding is not supported you can write a custom decoder and pass it as an argument to this program and use that instead.

### Rationale
In several projects I've worked on, I've found the ability to look and see what's sitting in Kafka to be incredibly useful. Apache saw the usefulness in this too so bundled with Kafka is the script `kafka-console-consumer.sh`. Unfortuantely, if you send the messages encoded in any way the bundled console consumer will still spit out messages but they likely won't be readable. This console consumer aims to fix that.



## Installation
A pre-requisite for running this program is having the Go programming language installed on your computer and your `GOPATH` correctly set up.

Because of how this program is packaged (the main package is located in cmd/go-kafka-consumer) your `go get` will have to look like this

If you don't plan on editing the code and just want to have the consumer available to run from your shell, run this command:

```sh
go get github.com/kenschneider18/go-kafka-consumer/...
```

This will download the program's source code, compile it, and install the binary on your system's `PATH`.

If you don't want to install the binary right off the bat, clone the repository:

```sh
git clone https://github.com/kenschneider18/go-kafka-consumer.git
```

Should you decide to install the binary later you can do so by running the below command from the project's root directory

```sh
go install ./...
```

## Usage

No matter which installation method is used the required arguments for the program are:

```
  -bootstrap-server
  		Comma separated list of broker URLs
  -from-beginning
  		By default the program starts from the latest offset,
  		passing this will make the program start from
  		the earliest offset
  		 (if you pass a group ID this may not behave
  		 as expected). I'm also aware this doesn't
  		 match the format of bootstrap-server, this
  		 is the way kafka-console-consumer works and
  		 I chose to match it.
  -schemas string
    	If the message type you pass requires schemas,
    	pass them here (The standard Avro decoder only
    	supports one schema, custom decoders may take multiple).
  -topic string
    	Kafka topic to consume from
  -type string
    	Either pass the message type here for supported
    	types or pass a path to the plugin
    	Default support:
    		avro
    		msgpack
    		json
    	
*** Coming soon ***
  -group string
  		Optionally pass a group ID for your consumer.
  		If the consumer group has connected to Kafka
  		before Kafka will ignore requests to start at
  		the earliest offset and will instead consume
  		from the earliest offset not yet read by the
  		passed consumer group. As far as I'm aware
  		there is no way around this.
```

*if you're really used to the bundled `kafka-console-consumer` passing the arguments with `--` is also supported.

Examples:

With the binary installed in your `PATH`

```
go-kafka-consumer -bootstrap-server localhost:9092 -topic test -type json -fromBeginning
```

From the project root directory

```
go run cmd/go-kafka-consumer/main.go -bootstrap-server localhost:9092 -topic test -type avro -schemas /path/to/schema.avsc
```

### Default Supported Encodings

By default `go-kafka-consumer` supports:

- Apache Avro passed as `avro`
- MessagePack passed as `msgpack`
- JSON passed as `json`

## Extendability

This program is written to be extended with Go plugins, if you haven't worked with plugins before here's a good article about them:

https://medium.com/learning-the-go-programming-language/writing-modular-go-programs-with-plugins-ec46381ee1a9

### System Requirements

By itself this program will run on any machine capable of running Go program (though I have yet to test it on a Windows machine). However, plugins are still new so there's limited support for them.

Operating System: Linux or macOS
Go version: 1.8+

### Writing a Plugin

To work with `go-kafka-consumer` a plugin must implement this interface

```go
	Decoder interface {
		ValidateSchemas(schemas string) error
		Decode([]byte) (interface{}, error)
	}
```
and have a variable exposed called `Decoder` that is an instance of that implementation. The package for the plugin must also be `main`.

`ValidateSchemas` will be passed an unparsed string containing the schema(s). Perform any validation logic on the schema(s) here, if schemas aren't required create a function that returns `nil`.

`Decode` will be passed the `Value` of the Kafka message. This function ***MUST*** return an interface that can be marshalled into JSON using Go's standard library. (Ideally, the entire Kafka message would be passed but we're limited by Go plugins. This will be fixed in a future update)

An example of the MessagePack decoder as a plugin can be found here: https://github.com/kenschneider18/kafka-decoders

#### Compiling plugins

```
go build -buildmode=plugin -o /path/to/a/plugin.so /path/to/a/plugin/source.go
```

### Running with Plugins

Instead of passing one of default types, instead pass the path to a compiled plugin.

```
go-kafka-consumer -bootstrap-server localhost:9092 -topic test -type /path/to/a/plugin.so -fromBeginning
```

Assuming the plugin is implemented correctly it should work just like that!
