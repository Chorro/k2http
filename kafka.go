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
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/sirupsen/logrus"
)

// KafkaConfig stores the configuration for the Kafka source
type KafkaConfig struct {
	topics              []string // Topics where listen for messages
	brokers             []string // Brokers to connect
	consumergroup       string   // ID for the consumer
	consumerGroupConfig *cluster.Config

	httpPostParameters http.Header
	httpEndpoint       string
}

type KafkaConsumer struct {
	closed      chan struct{}
	Config      *KafkaConfig // Cofiguration after the parsing
	batchConfig *batcherConfig

	limiter limiter
}

// Note: this does not keep ordering!
func removeKafkaInternalTopics(topics *[]string) {
	// No need for these topics
	var delete_elements int

	for i := len(*topics) - 1; i >= 0; i-- {
		topic := (*topics)[i]
		if strings.HasPrefix(topic, "__") {
			// Replace it with the last one.
			(*topics)[i] = (*topics)[len(*topics)-delete_elements-1]

			// mark for deletion
			delete_elements++
		}
	}

	// Chop off the deleted.
	if delete_elements > 0 {
		(*topics) = (*topics)[:len(*topics)-delete_elements]
	}
}

func equal(slice1 []string, slice2 []string) bool {
	// Populate map with s1 values
	if len(slice1) != len(slice2) {
		return false
	}

	sort.Strings(slice1)
	sort.Strings(slice2)

	for i, s2Val := range slice2 {
		if slice1[i] != s2Val {
			return false
		}
	}

	return true
}

func sendKafkaBatch(buf bytes.Buffer,
	httpClient *http.Client,
	httpEndpoint string,
	httpHeaders http.Header,
	logger *logrus.Entry) bool {

	httpRequest, _ := http.NewRequest("POST",
		httpEndpoint,
		&buf)

	httpRequest.Header = httpHeaders
	res, err := httpClient.Do(httpRequest)
	if err != nil {
		logger.WithField("HTTP client error", err).Warnf(err.Error())
		return false
	}

	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()

	if res.StatusCode >= 400 {
		logger.WithField("HTTP Response", res.Status)
		return false
	}

	logger.Debugf("Sent message: %v", buf)

	return true
}

func kafkaConsumerMainLoop(k *KafkaConsumer,
	consumer *cluster.Consumer,
	logger *logrus.Entry,
	httpClient *http.Client) {

	var workers []KafkaPartitionConsumer

mainLoop:
	for {
		select {
		case part, ok := <-consumer.Partitions():
			if !ok {
				logger.Debug("partitions channel closed")
				break mainLoop
			}

			// start a separate goroutine to consume messages
			workers = append(workers, KafkaPartitionConsumer{
				LimiterTick: make(chan time.Time),
				consumer:    part,
			})

			httpEndpoint := k.Config.httpEndpoint + "/" + part.Topic()
			partitionLogger := logger.WithField("endpoint", httpEndpoint)

			processMessage := func(lastBatchMessage *sarama.ConsumerMessage,
				batch bytes.Buffer) bool {

				sendOk := sendKafkaBatch(batch,
					httpClient,
					httpEndpoint,
					k.Config.httpPostParameters,
					partitionLogger)

				if sendOk {
					consumer.MarkOffset(lastBatchMessage, "")
				}

				return sendOk
			}

			workers[len(workers)-1].Spawn(&k.limiter,
				logger,
				k.batchConfig,
				processMessage)

		case t := <-k.limiter.TickerChannel():
			k.limiter.Reset()
			for _, w := range workers {
				w.LimiterTick <- t
			}

		case <-k.closed:
			break mainLoop
		}
	}

	return
}

// Start starts reading messages from kafka and pushing them to the pipeline
func (k *KafkaConsumer) Start() {
	k.closed = make(chan struct{})

	logger = Logger.WithFields(logrus.Fields{
		"prefix": "k2http",
	})

	httpClient := http.DefaultClient

	k.Config.consumerGroupConfig.Metadata.RefreshFrequency = 30 * time.Second
	consumer, err := cluster.NewConsumer(k.Config.brokers,
		k.Config.consumergroup,
		k.Config.topics,
		k.Config.consumerGroupConfig)
	if err != nil {
		logger.Error("Failed to start consumer: ", err)
		return
	}

	logger.
		WithField("brokers", k.Config.brokers).
		WithField("consumergroup", k.Config.consumergroup).
		WithField("topics", k.Config.topics).
		Info("Started consumer")

	defer consumer.Close()

	kafkaConsumerMainLoop(k, consumer, logger, httpClient)
}

// Close closes the connection with Kafka
func (k *KafkaConsumer) Close() {
	logger.Info("Terminating... Press ctrl+c again to force exit")
	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt)
	go func() {
		<-ctrlc
		logger.Fatal("Forced exit")
	}()

	k.closed <- struct{}{}

	<-time.After(5 * time.Second)
}
