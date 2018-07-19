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
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type limiterConfig struct {
	maxBytes, maxMessages int
	interval              time.Duration
	lock                  bool
}

type limiter struct {
	Config *limiterConfig

	lock                          sync.Mutex
	currentMessages, currentBytes int

	// Just for reuse
	timer *time.Timer
}

var limiter_logger *logrus.Entry

func NewLimiter(config *limiterConfig) (this limiter) {
	this = limiter{
		Config: config,
	}

	if this.Enabled() {
		this.timer = time.NewTimer(config.interval)
		// Stop the timer until needed
		if !this.timer.Stop() {
			<-this.timer.C
		}
	}

	// Initialize logs if needed
	if limiter_logger == nil {
		limiter_logger = logger.WithFields(logrus.Fields{
			"section": "limiter"})
	}

	return
}

func (l limiter) Enabled() bool {
	return l.Config.maxBytes > 0 || l.Config.maxMessages > 0
}

func (l limiter) wouldFull(bytes int) bool {
	return (l.Config.maxMessages > 0 && l.currentMessages+1 > l.Config.maxMessages) || (l.Config.maxBytes > 0 && l.currentBytes+bytes > l.Config.maxBytes)
}

func (l *limiter) Lock() {
	if l.Config.lock {
		l.lock.Lock()
	}
}

func (l *limiter) Unlock() {
	if l.Config.lock {
		l.lock.Unlock()
	}
}

// Add message add message into account of bytes/messages. Return true if can be
// sent, or false if you should wait. In that case, you should call to WaitTimer
// to know when you can keep sending.
//
// @param      bytes  Message bytes
//
// @return     True if you can keep sending, false otherwise
//
func (l *limiter) AddMessage(bytes int) bool {
	if !l.Enabled() {
		// You can always send
		return true
	}

	l.Lock()
	if l.wouldFull(bytes) {
		l.Unlock()
		limiter_logger.Debug("Can't send anymore, wait for the timer")
		return false
	}

	if l.currentMessages == 0 {
		l.timer.Reset(l.Config.interval)
	}

	l.currentMessages++
	l.currentBytes += bytes
	l.Unlock()

	limiter_logger.Debug("Allowing send")
	return true
}

func (l *limiter) Stop() {
	l.Lock()
	l.currentMessages = 0
	l.currentBytes = 0
	l.Unlock()

	if !l.timer.Stop() {
		select {
		case <-l.timer.C:
		default:
		}
	}
}

func (l *limiter) Reset() {
	l.Stop()
	l.timer.Reset(l.Config.interval)
}

func (l *limiter) TickerChannel() <-chan time.Time {
	if l.Enabled() {
		return l.timer.C
	} else {
		return make(chan time.Time)
	}
}
