package distributed

import (
	eh "github.com/looplab/eventhorizon"
)

// DistributedCommandBus is a command bus that handles commands with the
// distributed CommandHandlers
type DistributedCommandBus struct {
	connector CommandBusConnector
}

// NewCommandBus creates a default CommandBus.
func NewDistributedCommandBus(domain string) *DistributedCommandBus {
	b := &DistributedCommandBus{
		connector: &RabbitMQTTCBC{
			handlers:      make(map[eh.CommandType]eh.CommandHandler),
			topicStrategy: &DefaultTopicStrategy{},
			commandParse:  &JsonCommandParse{},
			routeStrategy: &StaticRoutingStrategy{domain: domain},
			configs:       Config{broker: "tcp://localhost:1883", username: "guest", password: "guest", cleansession: false},
		},
	}
	return b
}

// creates a custome CommandBus.
func NewCustomDistributedCommandBus(connector CommandBusConnector) *DistributedCommandBus {
	b := &DistributedCommandBus{
		connector: connector,
	}
	return b
}

// HandleCommand handles a command with a handler capable of handling it.
func (b *DistributedCommandBus) HandleCommand(command eh.Command) error {

	return b.connector.Send(command)
}

// SetHandler adds a handler for a specific command.
func (b *DistributedCommandBus) SetHandler(handler eh.CommandHandler, commandType eh.CommandType) error {

	return b.connector.Subscribe(handler, commandType)
}
