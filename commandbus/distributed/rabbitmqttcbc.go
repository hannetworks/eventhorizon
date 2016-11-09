package distributed

import (
	"fmt"
	"log"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	eh "github.com/looplab/eventhorizon"
)

var subscribflag bool = false

type RabbitMQTTCBC struct {
	handlers      map[eh.CommandType]eh.CommandHandler
	topicstrategy TopicStrategy
	commandparse  CommandParse
	routStrategy  RoutingStrategy
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
	//broker := "tcp://localhost:1883"
	//id := string(eh.NewUUID())

	//opts := MQTT.NewClientOptions()
	//opts.AddBroker(broker)
	//opts.SetClientID(id)
	//	opts.SetUsername(*user)
	//	opts.SetPassword(*password)
	//opts.SetCleanSession(false)
	//	if *store != ":memory:" {
	//		opts.SetStore(MQTT.NewFileStore(*store))
	//	}

	//client := MQTT.NewClient(opts)
	return opts
}
func (rbmcbc *RabbitMQTTCBC) Send(command eh.Command) error {
	opts := rbmcbc.initOpts()
	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
		//panic(token.Error())
	}

	msg, err := rbmcbc.commandparse.Encode(command)
	if err != nil {
		return err
	}
	log.Println("routekey : " + string(rbmcbc.routStrategy.GetRoutingKey(command)))
	token := client.Publish(string(rbmcbc.routStrategy.GetRoutingKey(command)), byte(0), false, msg)
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
	}
	return nil

}

func (rbmcbc *RabbitMQTTCBC) connectServer() {
	//broker := "tcp://localhost:1883"
	//id := string(eh.NewUUID())

	//opts := MQTT.NewClientOptions()
	//opts.AddBroker(broker)
	//opts.SetClientID(id)
	//	opts.SetUsername(*user)
	//	opts.SetPassword(*password)
	//opts.SetCleanSession(false)
	//	if *store != ":memory:" {
	//		opts.SetStore(MQTT.NewFileStore(*store))
	//	}

	opts := rbmcbc.initOpts()
	choke := make(chan [2]string)

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("first RECEIVED TOPIC: %s MESSAGE: %s\n", msg.Topic(), string(msg.Payload()))
		choke <- [2]string{msg.Topic(), string(msg.Payload())}
	})
	subclient := MQTT.NewClient(opts)
	if token := subclient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Println("route : " + string(rbmcbc.routStrategy.GetTopicPattern()))

	if token := subclient.Subscribe(string(rbmcbc.routStrategy.GetTopicPattern()), 0, nil); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	rbmcbc.listening(choke)
}

func (rbmcbc *RabbitMQTTCBC) listening(choke chan [2]string) {

	for true {
		fmt.Println("listening ")
		incoming := <-choke
		fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		ct := rbmcbc.topicstrategy.ParseTopic(incoming[0])
		if ct == "" {
			continue
		} else {
			if handler, ok := rbmcbc.handlers[ct]; ok {
				//handler.HandleCommand(json.Unmarshal(incoming[1], eh.CreateCommand(ct)))
				command, err := eh.CreateCommand(ct)
				if err != nil {
					fmt.Println("error1")
					continue
				} else {
					command, err = rbmcbc.commandparse.Decode(incoming[1], command)
					fmt.Println("get command")
					if err != nil {
						fmt.Println("error2")
						continue
					} else {
						err = handler.HandleCommand(command)
						if err != nil {
							fmt.Println("commandhandle error : " + string(err.Error()))
						}
					}
				}
			}
		}

	}
}
