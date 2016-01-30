// Copyright (c) 2015 - Max Persson <max@looplab.se>
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

// +build sql

package eventhorizon

import (
	"errors"
	"fmt"
	"time"

	"database/sql/driver"

	"encoding/json"

	"github.com/jinzhu/gorm"
)

// Error returned when the database could not be dialed.
var ErrCouldNotDialDB = errors.New("could not dial database")

// Error returned when no database session is set.
var ErrNoDBSession = errors.New("no database session")

// Error returned when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

// Error returned when an event is not registered.
var ErrEventNotRegistered = errors.New("event not registered")

// Error returned when an model is not set on a read repository.
var ErrModelNotSet = errors.New("model not set")

// Error returned when an event could not be marshaled into BSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// Error returned when an event could not be unmarshaled into a concrete type.
var ErrCouldNotUnmarshalEvent = errors.New("could not unmarshal event")

// Error returned when an aggregate could not be loaded.
var ErrCouldNotLoadAggregate = errors.New("could not load aggregate")

// Error returned when an aggregate could not be saved.
var ErrCouldNotSaveAggregate = errors.New("could not save aggregate")

// Error returned when an event does not implement the Event interface.
var ErrInvalidEvent = errors.New("invalid event")

// SqlEventStore implements an EventStore for gorm Sql .
type SqlEventStore struct {
	eventBus  EventBus
	db        gorm.DB
	factories map[string]func() Event
}

type sqlAggregateRecord struct {
	ID      UUID `gorm:"primary_key"`
	Version int
	// Type        string        `bson:"type"`
	// Snapshot    bson.Raw      `bson:"snapshot"`
}

type sqlEventRecord struct {
	ID        UUID   `gorm:"primary_key"`
	Version   int    `gorm:"primary_key;AUTO_INCREMENT:FALSE"`
	Type      string `sql:"size:128"`
	Timestamp time.Time
	Event     Event  `sql:"-"`
	Payload   string `sql:"size:2048"`
}

// for UUID to sql column,which is needed by gorm
func (u UUID) Value() (driver.Value, error) {
	return u.String(), nil
}

func (u *UUID) Scan(value interface{}) error {
	s, e := ParseUUID(string(value.([]byte)))
	*u = s
	return e
}

// NewSqlEventStore creates a new SqlEventStore.
func NewSqlEventStore(eventBus EventBus, driver, url string) (*SqlEventStore, error) {
	db, err := gorm.Open(driver, url)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	db.LogMode(true)
	db.AutoMigrate(&sqlAggregateRecord{})
	db.AutoMigrate(&sqlEventRecord{})

	s := &SqlEventStore{
		eventBus:  eventBus,
		factories: make(map[string]func() Event),
		db:        db,
	}
	return s, nil
}

// Save appends all events in the event stream to the database.
func (s *SqlEventStore) Save(events []Event) error {
	if len(events) == 0 {
		return ErrNoEventsToAppend
	}

	for _, event := range events {
		// Get an existing aggregate, if any.
		aggregateRecord := sqlAggregateRecord{}
		s.db.Where("ID = ?", event.AggregateID().String()).First(&aggregateRecord)

		isNew := false
		if aggregateRecord.ID.String() == "" {
			isNew = true
			aggregateRecord = sqlAggregateRecord{ID: event.AggregateID()}
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return ErrCouldNotMarshalEvent
		}

		aggregateRecord.Version = aggregateRecord.Version + 1
		// Create the event record with timestamp.
		eventRecord := sqlEventRecord{
			ID:        event.AggregateID(),
			Type:      event.EventType(),
			Version:   aggregateRecord.Version,
			Timestamp: time.Now(),
			Payload:   string(payload),
		}

		if isNew {
			s.db.Create(&aggregateRecord)
		} else {
			s.db.Save(&aggregateRecord)
		}
		// save event
		s.db.Create(&eventRecord)

		// Publish event on the bus.
		if s.eventBus != nil {
			s.eventBus.PublishEvent(event)
		}
	}

	return nil
}

// Load loads all events for the aggregate id from the database.
// Returns ErrNoEventsFound if no events can be found.
func (s *SqlEventStore) Load(id UUID) ([]Event, error) {
	var eventRecords []sqlEventRecord
	s.db.Where("ID = ?", id.String()).Find(&eventRecords)

	events := make([]Event, len(eventRecords))
	for i, eventRecord := range eventRecords {
		// Get the registered factory function for creating events.
		f, ok := s.factories[eventRecord.Type]
		if !ok {
			return nil, ErrEventNotRegistered
		}

		// Manually decode the payload string to event.
		event := f()
		if err := json.Unmarshal([]byte(eventRecord.Payload), &event); err != nil {
			return nil, ErrCouldNotUnmarshalEvent
		}
		if events[i], ok = event.(Event); !ok {
			return nil, ErrInvalidEvent
		}
	}

	return events, nil
}

// RegisterEventType registers an event factory for a event type. The factory is
// used to create concrete event types when loading from the database.
//
// An example would be:
//     eventStore.RegisterEventType(&MyEvent{}, func() Event { return &MyEvent{} })
func (s *SqlEventStore) RegisterEventType(event Event, factory func() Event) error {
	if _, ok := s.factories[event.EventType()]; ok {
		return ErrHandlerAlreadySet
	}

	s.factories[event.EventType()] = factory

	return nil
}
