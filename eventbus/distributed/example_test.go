package distributed

import (
	"log"
	"testing"
	"time"

	eh "github.com/looplab/eventhorizon"
	commandbus "github.com/looplab/eventhorizon/commandbus/local"

	eventstore "github.com/looplab/eventhorizon/eventstore/memory"
	"github.com/looplab/eventhorizon/examples/domain"
	readrepository "github.com/looplab/eventhorizon/readrepository/memory"
)

func TestExample(t *testing.T) {
	// Create the event store.
	log.Printf("step1")
	eventStore := eventstore.NewEventStore()

	// Create the event bus that distributes events.
	log.Println("step2")
	eventBus := NewEventBus()
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
	rbc := commandbus.NewCommandBus()
	rbc.SetHandler(handler, domain.CreateInviteCommand)

	invitationRepository := readrepository.NewReadRepository()
	invitationProjector := domain.NewInvitationProjector(invitationRepository)
	eventBus.AddHandler(invitationProjector, domain.InviteCreatedEvent)

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
