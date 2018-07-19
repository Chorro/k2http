// Copyright (C) 2018, Wizzie S.L.
//
// Authors: Diego Fernández Barrera <bigomby@gmail.com>
//          Eugenio Pérez Martín <eupm90@gmail.com>
//
// This file is part of k2http.
//
// k2http is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// k2http is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with k2http.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"compress/zlib"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/sirupsen/logrus"
	"github.com/x-cray/logrus-prefixed-formatter"
)

const (
	defaultQueueSize = 10000
	defaultWorkers   = 1
	defaultRetries   = 0
	defaultBackoff   = 2
)

// Logger is the main logger object
var Logger = logrus.New()
var logger *logrus.Entry

var (
	configFilename *string
	debug          *bool
	version        string
)

func init() {
	configFilename = flag.String("config", "", "Config file")
	debug = flag.Bool("debug", false, "Show debug info")
	versionFlag := flag.Bool("version", false, "Print version info")

	flag.Parse()

	if *versionFlag {
		displayVersion()
		os.Exit(0)
	}

	if len(*configFilename) == 0 {
		fmt.Println("No config file provided")
		flag.Usage()
		os.Exit(0)
	}

	Logger.Formatter = new(prefixed.TextFormatter)

	// Show debug info if required
	if *debug {
		Logger.Level = logrus.DebugLevel
	}

	if *debug {
		go func() {
			Logger.Debugln(http.ListenAndServe("localhost:6060", nil))
		}()
	}
}

func main() {
	// Initialize logger
	logger = Logger.WithFields(logrus.Fields{
		"prefix": "k2http",
	})

	batchConfig := loadBatchConfig()
	KafkaConfig := loadKafkaConfig()

	// Initialize kafka
	kafka := &KafkaConsumer{
		limiter:     loadLimiterConfig(),
		Config:      &KafkaConfig,
		batchConfig: &batchConfig,
	}

	// Wait for ctrl-c to close the consumer
	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)
	go func() {
		<-ctrlc
		kafka.Close()
	}()

	// Start getting messages
	kafka.Start()
}

func displayVersion() {
	fmt.Println("K2HTTP VERSION:\t\t", version)
}

func loadConfig(filename, component string) (config map[string]interface{}, err error) {
	generalConfig := make(map[string]interface{})
	config = make(map[string]interface{})

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	yaml.Unmarshal([]byte(data), &generalConfig)
	if err != nil {
		return
	}

	for k, v := range generalConfig[component].(map[interface{}]interface{}) {
		config[k.(string)] = v
	}

	return
}

func loadConfigDuration(cfg interface{},
	defaultDuration time.Duration,
	logger *logrus.Entry) (d time.Duration) {
	if cfg == nil {
		d = defaultDuration
		return
	}

	if interval, ok := cfg.(string); ok {
		var parse_err error
		if d, parse_err = time.ParseDuration(interval); parse_err != nil {
			d = defaultDuration
			logger.WithFields(logrus.Fields{
				"error":            parse_err,
				"string":           interval,
				"default interval": defaultDuration,
			}).Warning("Unparseable string as duration, setting default interval")
		}
	} else if interval, ok := cfg.(int); ok {
		d = time.Duration(interval) * time.Second
	} else {
		logger.Fatal("Unparseable duration")
	}

	return
}

// Using deflate here for maintain old config format
func loadBatchConfig() (config batcherConfig) {
	batchConfig, err := loadConfig(*configFilename, "batch")
	if err != nil {
		logger.Fatal(err)
	}

	config.limiterConfig.interval = loadConfigDuration(batchConfig["timeoutMillis"],
		time.Second,
		logger.WithField("field", "batch.timeoutMillis")) * (time.Millisecond / time.Second)
	var ok bool
	if config.limiterConfig.maxMessages, ok = batchConfig["size"].(int); !ok {
		logger.Fatal("Invalid 'size' option")
	}

	deflate, _ := batchConfig["deflate"].(bool)

	if deflate {
		config.writerBuilder = func(w io.Writer) io.Writer { return zlib.NewWriter(w) }
	} else {
		config.writerBuilder = func(w io.Writer) io.Writer { return w }
	}

	logger.WithFields(map[string]interface{}{
		"timeoutMillis": config.limiterConfig.interval,
		"size":          config.limiterConfig.maxMessages,
		"deflate":       deflate,
	}).Info("Batch config")

	return
}

func loadHTTPConfig() (url string) {
	var ok bool
	httpConfig, err := loadConfig(*configFilename, "http")
	if err != nil {
		logger.Fatal(err)
	}

	url, ok = httpConfig["url"].(string)
	if !ok {
		logger.Fatal("Invalid 'url' option")
	}

	logger.WithFields(map[string]interface{}{
		"url": url,
	}).Info("HTTP config")

	return
}

func loadLimiterConfig() limiter {
	defaultLimiterInterval := time.Second
	limiterConfigYaml, err := loadConfig(*configFilename, "limiter")
	if err != nil {
		logger.Fatal(err)
	}

	config := limiterConfig{lock: true}
	if maxMessages, ok := limiterConfigYaml["max_messages"].(int); ok {
		config.maxMessages = maxMessages
	}
	if maxBytes, ok := limiterConfigYaml["max_bytes"].(int); ok {
		config.maxBytes = maxBytes
	}
	config.interval = loadConfigDuration(limiterConfigYaml["interval"],
		defaultLimiterInterval, logger)

	logger.WithFields(map[string]interface{}{
		"max_messages": config.maxMessages,
		"max_bytes":    config.maxBytes,
	}).Info("Limiter config")

	return NewLimiter(&config)
}

func loadKafkaConfig() KafkaConfig {
	kafkaConfig, err := loadConfig(*configFilename, "kafka")
	if err != nil {
		logger.Fatal(err)
	}

	config := KafkaConfig{}
	deflate := false

	batchConfig, err := loadConfig(*configFilename, "batch")
	config.consumerGroupConfig = cluster.NewConfig()
	if err == nil {
		var ok bool
		if deflate, ok = batchConfig["deflate"].(bool); !ok {
			deflate = false
		}
	}
	if *debug {
		sarama.Logger = logger.WithField("prefix", "kafka-consumer")
	}

	config.consumerGroupConfig.Group.Mode = cluster.ConsumerModePartitions
	if consumerGroup, ok := kafkaConfig["consumergroup"].(string); ok {
		config.consumergroup = consumerGroup
		config.consumerGroupConfig.ClientID = consumerGroup
	} else {
		config.consumergroup = "k2http"
		config.consumerGroupConfig.ClientID = "k2http"
	}
	if broker, ok := kafkaConfig["broker"].(string); ok {
		config.brokers = strings.Split(broker, ",")
	} else {
		logger.Fatal("Invalid 'broker' option")
	}
	if topics, ok := kafkaConfig["topics"]; ok {
		switch topics.(type) {
		case string:
			config.allTopics = topics.(string) == "*"
		default:
			for _, topic := range topics.([]interface{}) {
				config.topics = append(config.topics, topic.(string))
			}
		}
	} else {
		logger.Fatal("Invalid 'topic' option ", ok)
	}

	config.httpEndpoint = loadHTTPConfig()

	config.httpPostParameters = http.Header{}
	if nil != kafkaConfig["messageParameters"] {
		for _, param := range kafkaConfig["messageParameters"].([]interface{}) {
			for key, val := range param.(map[interface{}]interface{}) {
				if strval, ok := val.(string); ok {
					config.httpPostParameters.Add(key.(string), strval)
				} else if slcval, ok := val.([]interface{}); ok {
					for _, vval := range slcval {
						config.httpPostParameters.Add(key.(string), vval.(string))
					}
				}
			}
		}
	}

	if deflate {
		config.httpPostParameters.Add("Content-Encoding", "deflate")
	}

	if version := kafkaConfig["version"]; nil != version {
		switch v := version.(string); v {
		case "0_8_2_0":
			config.consumerGroupConfig.Version = sarama.V0_8_2_0
		case "0_8_2_1":
			config.consumerGroupConfig.Version = sarama.V0_8_2_1
		case "0_8_2_2":
			config.consumerGroupConfig.Version = sarama.V0_8_2_2
		case "0_9_0_0":
			config.consumerGroupConfig.Version = sarama.V0_9_0_0
		case "0_9_0_1":
			config.consumerGroupConfig.Version = sarama.V0_9_0_1
		case "0_10_0_0":
			config.consumerGroupConfig.Version = sarama.V0_10_0_0
		case "0_10_0_1":
			config.consumerGroupConfig.Version = sarama.V0_10_0_1
		case "0_10_1_0":
			config.consumerGroupConfig.Version = sarama.V0_10_1_0
		case "0_10_2_0":
			config.consumerGroupConfig.Version = sarama.V0_10_2_0
		default:
			logger.Fatal("Invalid kafka version value ", v)
		}
	}

	config.consumerGroupConfig.Config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.consumerGroupConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	if nil != kafkaConfig["offsetReset"] {
		switch kafkaConfig["offsetReset"].(string) {
		case "smallest", "earliest", "beginning", "old", "oldest":
			config.consumerGroupConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		case "largest", "latest", "end", "newest", "new":
			// Nothing to do
		default:
			logger.Error("Unknown offsetReset, falling back to \"newest\"")
		}
	}

	// config.consumerGroupConfig.Consumer.MaxProcessingTime = 5 * time.Second

	return config
}
