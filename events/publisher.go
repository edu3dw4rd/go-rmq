package events

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/edu3dw4rd/rmq"

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
			exchangeName,        // name
			"x-delayed-message", //type
			true,                //durable
			false,               //auto-deleted
			false,               // internal
			false,               //no-wait
			amqp.Table{
				"x-delayed-type": "fanout",
			}, //arguments
		)

		if err != nil {
			log.Printf("Failed to declare an exchange. Error: %+v\n", err.Error())
		}

		headers := amqp.Table{
			"x-delay": delayMillis,
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
				Headers:     headers,
			},
		)

		if err != nil {
			log.Printf("Failed to publish to delay exchange [%s]. Error: %+v\n", exchangeName, err.Error())
		}

		fmt.Printf("Publishing to delay exchange [%s] finished...\n", exchangeName)
	}()
}
