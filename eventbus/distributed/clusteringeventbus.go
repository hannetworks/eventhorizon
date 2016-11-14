package distributed

import (
	eh "github.com/looplab/eventhorizon"
)

type ClusteringEventBus struct {
	terminal  EventBusTerminal
	observers map[eh.EventObserver]bool
}

// NewEventBus creates a EventBus.
func NewEventBus() *ClusteringEventBus {
	b := &ClusteringEventBus{
		terminal: &RabbitMqttEBT{
			handlers:      make(map[eh.EventType]map[eh.EventHandler]bool),
			topicStrategy: &EventDefaultTopicStrategy{},
			eventParse:    &JsonEventParse{},
			eventRoute:    &EventRoutingStrategy{"domain"},
			configs:       Config{broker: "tcp://localhost:1883", username: "guest", password: "guest", cleansession: false},
		},
		observers: make(map[eh.EventObserver]bool),
	}
	return b
}

func (b *ClusteringEventBus) PublishEvent(event eh.Event) {

	b.terminal.Publish(event)
	// Notify all observers about the event.
	for o := range b.observers {
		o.Notify(event)
	}
}

// AddHandler implements the AddHandler method of the EventHandler interface.
func (b *ClusteringEventBus) AddHandler(handler eh.EventHandler, eventType eh.EventType) {
	b.terminal.AddHandler(handler, eventType)
}

// AddObserver implements the AddObserver method of the EventHandler interface.
func (b *ClusteringEventBus) AddObserver(observer eh.EventObserver) {
	b.observers[observer] = true
}
