package distributed

import (
	eh "github.com/looplab/eventhorizon"
)

type CommandBusConnector interface {
	Send(eh.Command) error
	Subscribe(eh.CommandHandler, eh.CommandType) error
}
