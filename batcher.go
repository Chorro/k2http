// Copyright (C) 2018, Wizzie S.L.
//
// Authors: Diego Fernández Barrera <bigomby@gmail.com>
//          Eugenio Pérez Martín <eupm90@gmail.com>
//
// This file is part of k2http.
//
// k2http is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// k2http is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with k2http.  If not, see <https://www.gnu.org/licenses/lgpl-3.0.txt>.

package main

import (
	"bytes"
	"io"
	"time"
)

type Flushable interface {
	Flush() error
}

type Resettable interface {
	Reset() error
}

type batcherConfig struct {
	limiterConfig limiterConfig
	writerBuilder func(io.Writer) io.Writer
}

type batcher struct {
	config *batcherConfig
	Buf    bytes.Buffer

	limiter limiter
	writer  io.Writer
}

func NewBatcher(config *batcherConfig) *batcher {
	this := &batcher{
		limiter: NewLimiter(&config.limiterConfig),
	}

	this.writer = config.writerBuilder(&this.Buf)

	return this
}

// Add message add message in batch. Return true if added, or false if you
// need to flush the batch
//
// @param      bytes  Message bytes
//
// @return     True if you can keep sending, false otherwise
//
func (b *batcher) AddMessage(p []byte) bool {
	if !b.limiter.AddMessage(len(p)) {
		return false
	}

	if b.limiter.currentMessages == 0 {
		// Need to start timer
		b.limiter.Reset()
	}

	tot, err := b.writer.Write(p)
	if err != nil || tot < len(p) {
		return false
	}

	return true
}

func (b *batcher) Flush() {
	b.limiter.Stop()
	if w, ok := b.writer.(Flushable); ok {
		w.Flush()
	}
}

func (b *batcher) Reset() {
	if w, ok := b.writer.(Resettable); ok {
		w.Reset()
	}
	b.Buf.Reset()
}

func (b *batcher) TickerChannel() <-chan time.Time {
	return b.limiter.TickerChannel()
}
