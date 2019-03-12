[![Build Status](https://travis-ci.org/redBorder/k2http.svg?branch=master)](https://travis-ci.org/redBorder/k2http)
[![Go Report Card](https://goreportcard.com/badge/github.com/redBorder/k2http)](https://goreportcard.com/report/github.com/redBorder/k2http)

# K2HTTP2

k2http is a application that forwards messages from kafka 0.9+ to an HTTP
endpoint.

## Installing

To install this application ensure you have the `GOPATH` environment variable
set and **[glide](https://glide.sh/)** installed.

```bash
curl https://glide.sh/get | sh
```

And then:

1. Clone this repo and cd to the project

    ```bash
    git clone https://github.com/wizzie-io/k2http.git && cd k2http
    ```
2. Install dependencies and compile

    ```bash
    make
    ```
3. Install on desired directory

    ```bash
    prefix=/usr/local make install
    ```

## Usage

```
Usage of k2http:
  --config string
        Config file
  --debug
        Show debug info
  --version
        Print version info
```

To run `k2http` just execute the following command:

```bash
k2http --config path/to/config/file
```

## Config file
### Example config file

```yaml
pipeline:
  queue: 1000
  backoff: 10
  retries: 3

limiter:
  max_messages: 5000
  # max_bytes: 5242880            # Max bytes per second

kafka:
  broker: "localhost:9092"
  consumergroup: "k2http"
  topics: '*'

batch:
  workers: 1
  size: 1000
  timeoutMillis: 100
  deflate: false

http:
  workers: 1
  url: "http://localhost:8888"
  insecure: false
```

### Config options
name | docker name | description
-----|-------------|------------
pipeline.queue|PIPELINE_QUEUE|Max internal queue size
pipeline.backoff|PIPELINE_BACKOFF|Time to wait between retries in seconds
pipeline.retries|PIPELINE_RETRIES|Number of retries on fail (-1 not limited)
limiter.max.messages|LIMITER_MAX_MESSAGES|Max messages per second
kafka.brokers|KAFKA_BROKERS|Comma separated list of kafka brokers
kafka.consumer.group|KAFKA_CONSUMER_GROUP|Consumer group ID
kafka.topics|KAFKA_TOPICS|Kafka topics to listen (list, or `*` for all but __consumer_offset). Space sepparated topics in docker env variable.
messageParameters|HTTP_POST_PARAMS|HTTP Post parameters (each one as `- key: 'value'`). Space sepparated key:value in docker environment var.
kafka.offsetReset|KAFKA_OFFSET_RESET|Offset reset if no offset saved in broker. Values: [`smallest`, `earliest`, `beginning`, `old` or `oldest`], for reset at first kafka message in partition, and [`largest`, `latest`, `end`, `newest` or `new`] for last message (default behavior).
batch.workers|BATCH_WORKERS|Number of workers
batch.size|BATCH_SIZE|Max messages per batch
batch.timeout.ms|BATCH_TIMEOUT_MS|Max time to wait for send a batch
batch.deflate|BATCH_DEFLATE|Use deflate to compress batches
http.workers|HTTP_WORKERS|# Number of workers, one connection per worker
http.url|HTTP_ENDPOINT|Url to send messages. Topic will be add at url end
http.insecure|HTTP_INSECURE|# Skip SSSL verification

## Using with Docker

You can use the application with Docker. First you need to compile as usual and then generate the docker image:

```bash
make
docker build -t k2http-docker .
```

You can then use the app inside a docker container:

```bash
docker run --rm k2http-docker --version
```

You need to specify all configs environment variables. See example config file
for good default values.
