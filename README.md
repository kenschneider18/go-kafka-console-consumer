# go-kafka-consumer

go-kafka-consumer is an extendable Kafka console consumer written in golang that consumes from a given topic and writes the messages to the console in as JSON.

There is out of the box support for several encodings, if your encoding is not supported you can write a custom decoder and pass it as an argument to this program and use that instead.

### Rationale
In several projects I've worked on, I've found the ability to look and see what's sitting in Kafka to be incredibly useful. Apache saw the usefulness in this too so bundled with Kafka is the `kafka-console-consumer.sh` script. Unfortuantely, if you send the messages encoded in any way the bundled console consumer will still spit out messages but they likely won't be human readable. This console consumer aims to fix that.



