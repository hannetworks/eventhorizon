// Copyright (c) 2014 - Max Ekman <max@looplab.se>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/mgo.v2/bson"

	eh "github.com/looplab/eventhorizon"
)

// ErrCouldNotMarshalEvent is when an event could not be marshaled into BSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshaled into a concrete type.
var ErrCouldNotUnmarshalEvent = errors.New("could not unmarshal event")

// EventBus is an event bus that notifies registered EventHandlers of
// published events.
type EventBus struct {
	handlers  map[eh.EventType]map[eh.EventHandler]bool
	observers map[eh.EventObserver]bool
	prefix    string
	pool      *redis.Pool
	conn      *redis.PubSubConn
	exit      chan struct{}
}

// NewEventBus creates a EventBus for remote events.
func NewEventBus(appID, server, password string) (*EventBus, error) {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return NewEventBusWithPool(appID, pool)
}

// NewEventBusWithPool creates a EventBus for remote events.
func NewEventBusWithPool(appID string, pool *redis.Pool) (*EventBus, error) {
	b := &EventBus{
		handlers:  make(map[eh.EventType]map[eh.EventHandler]bool),
		observers: make(map[eh.EventObserver]bool),
		prefix:    appID + ":events:",
		pool:      pool,
		exit:      make(chan struct{}),
	}

	// Add a patten matching subscription.
	b.conn = &redis.PubSubConn{Conn: b.pool.Get()}
	ready := make(chan struct{})
	go b.recv(ready)
	err := b.conn.PSubscribe(b.prefix + "*")
	if err != nil {
		b.Close()
		return nil, err
	}
	<-ready

	return b, nil
}

// PublishEvent publishes an event to all handlers capable of handling it.
func (b *EventBus) PublishEvent(event eh.Event) {
	// Handle the event if there is a handler registered.
	if handlers, ok := b.handlers[event.EventType()]; ok {
		for handler := range handlers {
			handler.HandleEvent(event)
		}
	}

	// Notify all observers about the event.
	b.notify(event)
}

// AddHandler adds a handler for a specific local event.
func (b *EventBus) AddHandler(handler eh.EventHandler, eventType eh.EventType) {
	// Create handler list for new event types.
	if _, ok := b.handlers[eventType]; !ok {
		b.handlers[eventType] = make(map[eh.EventHandler]bool)
	}

	// Add handler to event type.
	b.handlers[eventType][handler] = true
}

// AddObserver implements the AddObserver method of the EventHandler interface.
func (b *EventBus) AddObserver(observer eh.EventObserver) {
	b.observers[observer] = true
}

// Close exits the recive goroutine by unsubscribing to all channels.
func (b *EventBus) Close() {
	err := b.conn.PUnsubscribe()
	if err != nil {
		log.Printf("error: event bus close: %v\n", err)
	}
	<-b.exit
	err = b.conn.Close()
	if err != nil {
		log.Printf("error: event bus close: %v\n", err)
	}
}

func (b *EventBus) notify(event eh.Event) {
	conn := b.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		log.Printf("error: event bus publish: %v\n", err)
	}

	// Marshal event data.
	var data []byte
	var err error
	if data, err = bson.Marshal(event); err != nil {
		log.Printf("error: event bus publish: %v\n", ErrCouldNotMarshalEvent)
	}

	// Publish all events on their own channel.
	if _, err = conn.Do("PUBLISH", b.prefix+string(event.EventType()), data); err != nil {
		log.Printf("error: event bus publish: %v\n", err)
	}
}

func (b *EventBus) recv(ready chan struct{}) {
	for {
		switch n := b.conn.Receive().(type) {
		case redis.PMessage:
			// Extract the event type from the channel name.
			eventType := eh.EventType(strings.TrimPrefix(n.Channel, b.prefix))

			// Create an event of the correct type.
			event, err := eh.CreateEvent(eventType)
			if err != nil {
				log.Printf("error: event bus receive: %v\n", err)
				continue
			}

			// Manually decode the raw BSON event.
			data := bson.Raw{3, n.Data}
			if err := data.Unmarshal(event); err != nil {
				log.Printf("error: event bus receive: %v\n", ErrCouldNotUnmarshalEvent)
				continue
			}

			for o := range b.observers {
				o.Notify(event)
			}
		case redis.Subscription:
			switch n.Kind {
			case "psubscribe":
				close(ready)
			case "punsubscribe":
				if n.Count == 0 {
					close(b.exit)
					return
				}
			}
		case error:
			log.Printf("error: event bus receive: %v\n", n)
			close(b.exit)
			return
		}
	}
}
