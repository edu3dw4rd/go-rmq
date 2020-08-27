package events

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go-rmq/rmq"

	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

type EventData struct {
	EventType   string      `json:"event_type,omitempty"`
	Data        interface{} `json:"data,omitempty"`
	PublishDate *time.Time  `json:"date,omitempty"`
}

func (data *EventData) FlatData() []byte {
	if data.PublishDate == nil {
		now := time.Now()
		data.PublishDate = &now
	}

	result, _ := json.Marshal(data)

	return result
}

// Publish publishes data to given exchange
func (data *EventData) Publish(exchangeName string) {
	go func() {
		connURL := rmq.GetRabbitURL()
		// fmt.Printf("CONNURL: %+v\n", connURL)

		defer func() {
			if v := recover(); v != nil {
				log.Println("Publisher got panic!! Recovering...", v)
			}
		}()

		// Create RMQ Connection
		conn, errConn := amqp.Dial(connURL)
		if errConn != nil {
			log.Printf("Failed to connect to RabbitMQ. Error: %+v\n", errConn.Error())
		}
		defer conn.Close()

		// Create RMQ Channel
		ch, errChan := conn.Channel()
		if errChan != nil {
			log.Printf("Failed to create RabbitMQ channel. Error: %+v\n", errChan)
		}
		defer ch.Close()

		// Declare fanout exchange
		err := ch.ExchangeDeclare(
			exchangeName, // name
			"fanout",     //type
			true,         //durable
			false,        //auto-deleted
			false,        // internal
			false,        //no-wait
			nil,          //arguments
		)

		if err != nil {
			log.Printf("Failed to declare an exchange. Error: %+v\n", err.Error())
		}

		// publish event
		err = ch.Publish(
			exchangeName, //exchange
			"",           // routing key
			false,        //mandatory
			false,        // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        data.FlatData(),
			},
		)

		if err != nil {
			log.Printf("Failed to publish to exchange [%s]. Error: %+v\n", exchangeName, err.Error())
		}

		fmt.Printf("Publishing to exchange [%s] finished...\n", exchangeName)
	}()
}

// PublishDelayExchange publishes given data to given exchange with given delay time in milliseconds
func (data *EventData) PublishDelayExchange(exchangeName string, delayMillis int64) {
	connURL := rmq.GetRabbitURL()

	client := cony.NewClient(
		cony.URL(connURL),
		cony.Backoff(cony.DefaultBackoff),
	)
	defer client.Close()

	args := amqp.Table{
		"x-delayed-type": "fanout",
	}

	// Declare exchange
	exchange := cony.Exchange{
		Name:       exchangeName,
		Kind:       "x-delayed-message",
		Durable:    true,
		AutoDelete: false,
		Args:       args,
	}
	client.Declare([]cony.Declaration{
		cony.DeclareExchange(exchange),
	})

	// Declare and register a publisher
	// with the cony client
	pbl := cony.NewPublisher(exchange.Name, "")
	client.Publish(pbl)

	// Launch a go routine and publish a message.
	// "Publish" is a blocking method this is why it
	// needs to be called in its own go routine.
	//

	headers := amqp.Table{
		"x-delay": delayMillis,
	}

	go pbl.Publish(amqp.Publishing{
		Body:    data.FlatData(),
		Headers: headers,
	})

	// Client loop sends out declarations(exchanges, queues, bindings
	// etc) to the AMQP server. It also handles reconnecting.
	go func() {
		for client.Loop() {
			select {
			case err := <-client.Errors():
				fmt.Printf("Client error: %v\n", err)
			case blocked := <-client.Blocking():
				fmt.Printf("Client is blocked %v\n", blocked)
			}
		}
	}()

	fmt.Printf("Publishing to delayed exchange [%s] finished...\n", exchangeName)
}
