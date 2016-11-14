package distributed

import (
	"log"

	MQTT "github.com/eclipse/paho.mqtt.golang"

	eh "github.com/looplab/eventhorizon"
)

type EventBusTerminal interface {
	Publish(eh.Event) error
	AddHandler(eh.EventHandler, eh.EventType) error
}

type RabbitMqttEBT struct {
	handlers      map[eh.EventType]map[eh.EventHandler]bool
	topicStrategy EventTopicStrategy
	eventParse    EventParse
	eventRoute    EventRoute
	configs       Config
}

type Config struct {
	broker       string
	id           string
	cleansession bool
	username     string
	password     string
}

var eventListenFlag = false

func (b *RabbitMqttEBT) initOpts() *MQTT.ClientOptions {
	opts := MQTT.NewClientOptions()
	if b.configs.broker != "" {
		opts.AddBroker(b.configs.broker)
	} else {
		panic("broker can not be null")
	}
	if b.configs.id != "" {
		opts.SetClientID(b.configs.id)
	} else {
		opts.SetClientID(string(eh.NewUUID()))
	}
	if b.configs.cleansession {
		opts.SetCleanSession(true)
	} else {
		opts.SetCleanSession(false)
	}
	if b.configs.username != "" {
		opts.SetUsername(b.configs.username)
	}
	if b.configs.password != "" {
		opts.SetPassword(b.configs.password)
	}

	return opts
}

func (b *RabbitMqttEBT) Publish(event eh.Event) error {
	opts := b.initOpts()
	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	msg, err := b.eventParse.Encode(event)
	if err != nil {
		return err
	}
	log.Println("routekey : " + string(b.eventRoute.GetRoutingKey(event)))
	token := client.Publish(string(b.eventRoute.GetRoutingKey(event)), byte(0), false, msg)
	token.Wait()

	client.Disconnect(250)
	return nil

}

func (b *RabbitMqttEBT) AddHandler(handler eh.EventHandler, eventType eh.EventType) error {
	// Create list for new event types.
	if _, ok := b.handlers[eventType]; !ok {
		b.handlers[eventType] = make(map[eh.EventHandler]bool)
	}
	b.handlers[eventType][handler] = true
	if !eventListenFlag {
		go b.connectServer()
	}
	return nil

}

func (b *RabbitMqttEBT) connectServer() {

	opts := b.initOpts()
	choke := make(chan [2]string)

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		log.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", msg.Topic(), string(msg.Payload()))
		choke <- [2]string{msg.Topic(), string(msg.Payload())}
	})
	subclient := MQTT.NewClient(opts)
	if token := subclient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Println("route : " + string(b.eventRoute.GetTopicPattern()))

	if token := subclient.Subscribe(string(b.eventRoute.GetTopicPattern()), 0, nil); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	b.listening(choke)
}

func (b *RabbitMqttEBT) listening(choke chan [2]string) {
	log.Println("start to listen ")
	for {
		incoming := <-choke
		et := b.topicStrategy.ParseTopic(incoming[0])
		if et == "" {
			continue
		} else {

			event, err := eh.CreateEvent(et)
			if err != nil {
				log.Fatal("get event origin type error" + string(err.Error()))
				continue
			} else {
				event, err = b.eventParse.Decode(incoming[1], event)
				if err != nil {
					log.Fatal("generate event instance error" + string(err.Error()))
					continue
				} else {
					//err = handler.HandleCommand(command)
					if handlers, ok := b.handlers[event.EventType()]; ok {
						for h := range handlers {
							h.HandleEvent(event)
						}
					}
					//						if err != nil {
					//							log.Fatal("commandhandle error : " + string(err.Error()))
					//						}
				}
			}

		}

	}
}
