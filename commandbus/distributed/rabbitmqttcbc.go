package distributed

import (
	"log"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	eh "github.com/looplab/eventhorizon"
)

var subscribflag bool = false

type RabbitMQTTCBC struct {
	handlers      map[eh.CommandType]eh.CommandHandler
	topicStrategy TopicStrategy
	commandParse  CommandParse
	routeStrategy CommandRoute
	configs       Config
}

type Config struct {
	broker       string
	id           string
	cleansession bool
	username     string
	password     string
}

func (rbmcbc *RabbitMQTTCBC) initOpts() *MQTT.ClientOptions {
	opts := MQTT.NewClientOptions()
	if rbmcbc.configs.broker != "" {
		opts.AddBroker(rbmcbc.configs.broker)
	} else {
		panic("broker can not be null")
	}
	if rbmcbc.configs.id != "" {
		opts.SetClientID(rbmcbc.configs.id)
	} else {
		opts.SetClientID(string(eh.NewUUID()))
	}
	if rbmcbc.configs.cleansession {
		opts.SetCleanSession(true)
	} else {
		opts.SetCleanSession(false)
	}
	if rbmcbc.configs.username != "" {
		opts.SetUsername(rbmcbc.configs.username)
	}
	if rbmcbc.configs.password != "" {
		opts.SetPassword(rbmcbc.configs.password)
	}

	return opts
}
func (rbmcbc *RabbitMQTTCBC) Send(command eh.Command) error {
	opts := rbmcbc.initOpts()
	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	msg, err := rbmcbc.commandParse.Encode(command)
	if err != nil {
		return err
	}
	log.Println("routekey : " + string(rbmcbc.routeStrategy.GetRoutingKey(command)))
	token := client.Publish(string(rbmcbc.routeStrategy.GetRoutingKey(command)), byte(0), false, msg)
	token.Wait()

	client.Disconnect(250)
	return nil

}
func (rbmcbc *RabbitMQTTCBC) Subscribe(commandHandler eh.CommandHandler, commandType eh.CommandType) error {

	if _, ok := rbmcbc.handlers[commandType]; ok {
		return eh.ErrHandlerAlreadySet
	}
	rbmcbc.handlers[commandType] = commandHandler
	if !subscribflag {
		go rbmcbc.connectServer()
		subscribflag = true
	}
	return nil

}

func (rbmcbc *RabbitMQTTCBC) connectServer() {

	opts := rbmcbc.initOpts()
	choke := make(chan [2]string)

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		log.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", msg.Topic(), string(msg.Payload()))
		choke <- [2]string{msg.Topic(), string(msg.Payload())}
	})
	subclient := MQTT.NewClient(opts)
	if token := subclient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Println("route : " + string(rbmcbc.routeStrategy.GetTopicPattern()))

	if token := subclient.Subscribe(string(rbmcbc.routeStrategy.GetTopicPattern()), 0, nil); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	rbmcbc.listening(choke)
}

func (rbmcbc *RabbitMQTTCBC) listening(choke chan [2]string) {
	log.Println("start to listen ")
	for {
		incoming := <-choke
		ct := rbmcbc.topicStrategy.ParseTopic(incoming[0])
		if ct == "" {
			continue
		} else {
			if handler, ok := rbmcbc.handlers[ct]; ok {
				command, err := eh.CreateCommand(ct)
				if err != nil {
					log.Fatal("get command origin type error" + string(err.Error()))
					continue
				} else {
					command, err = rbmcbc.commandParse.Decode(incoming[1], command)
					if err != nil {
						log.Fatal("generate commande instance error" + string(err.Error()))
						continue
					} else {
						err = handler.HandleCommand(command)
						if err != nil {
							log.Fatal("commandhandle error : " + string(err.Error()))
						}
					}
				}
			}
		}

	}
}
