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
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/sirupsen/logrus"
)

// KafkaConsumer get messages from multiple kafka topics
type KafkaPartitionConsumer struct {
	LimiterTick chan time.Time

	consumer cluster.PartitionConsumer
}

func (this *KafkaPartitionConsumer) mainLoop(limiter *limiter,
	logger *logrus.Entry,
	batchConfig *batcherConfig,
	msgCallback func(*sarama.ConsumerMessage, bytes.Buffer) bool) {

	var messageInQueue, lastMessage *sarama.ConsumerMessage
	kafkaMessageChannel := this.consumer.Messages()
	var messagesRead, bytesRead int

	tBatcher := NewBatcher(batchConfig)

	logger = logger.WithFields(logrus.Fields{
		"ctx":       "consumerWorker",
		"topic":     this.consumer.Topic(),
		"partition": this.consumer.Partition(),
	})

	logger.Info("Consumer started")

	// Try to fire the message batch. If ok, it will reset batch. Else,
	// it will retry until the end
	fireBatch := func(tBatcher *batcher) {
		tBatcher.Flush()
		for {
			callbackOk := msgCallback(lastMessage, tBatcher.Buf)
			if callbackOk {
				lastMessage = nil
				// Swap needed by process async nature
				tBatcher.Buf = bytes.Buffer{}
				break
			}
		}
	}

	processMessage := func(message *sarama.ConsumerMessage) (roomForMessage bool) {
		bytes := len(message.Value)

		// Does the limiter allow us?
		roomForMessage = limiter.AddMessage(bytes)
		if !roomForMessage {
			return
		}

		for {
			// Try to write in the batch
			roomInBatch := tBatcher.AddMessage(message.Value)
			if roomInBatch {
				lastMessage = message
				bytesRead += len(message.Value)
				messagesRead++
				break // all ok
			}

			fireBatch(tBatcher)
		}

		return
	}

mainLoop:
	for {
		select {
		// Can keep receiving messages
		case <-this.LimiterTick:
			if nil == messageInQueue ||
				(nil != messageInQueue && processMessage(messageInQueue)) {

				messageInQueue = nil
				kafkaMessageChannel = this.consumer.Messages()
			}

		// Batch ready to send
		case <-tBatcher.TickerChannel():
			fireBatch(tBatcher)

		case message, ok := <-kafkaMessageChannel:
			if !ok {
				logger.Debug("kafkaMessageChannel is not ok")
				break mainLoop
			}
			if message == nil {
				logger.Debug("nil message received")
				break mainLoop
			}

			if !processMessage(message) {
				messageInQueue = message
				kafkaMessageChannel = nil
			}
		}
	}

	logger.WithFields(logrus.Fields{
		"bytes":    bytesRead,
		"messages": messagesRead,
	}).Info("Consumer end")
}

func (this *KafkaPartitionConsumer) Spawn(limiter *limiter,
	logger *logrus.Entry,
	batcherConfig *batcherConfig,
	msgCallback func(*sarama.ConsumerMessage, bytes.Buffer) bool) {
	go this.mainLoop(limiter, logger, batcherConfig, msgCallback)
}
