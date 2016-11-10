package distributed

import (
	"log"
	"testing"
	"time"

	eh "github.com/looplab/eventhorizon"
	eventbus "github.com/looplab/eventhorizon/eventbus/local"
	eventstore "github.com/looplab/eventhorizon/eventstore/memory"
	"github.com/looplab/eventhorizon/examples/domain"
)

func TestExample(t *testing.T) {
	// Create the event store.
	log.Printf("step1")
	eventStore := eventstore.NewEventStore()

	// Create the event bus that distributes events.
	log.Println("step2")
	eventBus := eventbus.NewEventBus()
	eventBus.AddObserver(&domain.Logger{})

	// Create the aggregate repository.
	repository, err := eh.NewEventSourcingRepository(eventStore, eventBus)
	if err != nil {
		log.Fatalf("could not create repository: %s", err)
	}

	// Create the aggregate command handler.
	log.Println("step3")
	handler, err := eh.NewAggregateCommandHandler(repository)
	handler.SetAggregate(domain.InvitationAggregateType, domain.CreateInviteCommand)
	rbc := NewCustomDistributedCommandBus(&RabbitMQTTCBC{
		handlers:      make(map[eh.CommandType]eh.CommandHandler),
		topicstrategy: &DefaultTopicStrategy{},
		commandparse:  &JsonCommandParse{},
		routStrategy:  &StaticRoutingStrategy{domain: "domain"},
		configs:       Config{broker: "tcp://localhost:1883", username: "guest", password: "guest", cleansession: false},
	})
	rbc.SetHandler(handler, domain.CreateInviteCommand)
	log.Println("step4")
	time.Sleep(time.Millisecond * 5000)
	athenaID := eh.NewUUID()
	rbc.HandleCommand(&domain.CreateInvite{InvitationID: athenaID, Name: "Athena", Age: 42})

	log.Println("end")
	for {
		time.Sleep(time.Millisecond * 5000)
		log.Println("continue")
	}

}
