package events

import (
	"log"

	"github.com/assembla/cony"
	"github.com/edu3dw4rd/rmq"
	json "github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

type Handler func(map[string]interface{})

func processConsumer(consumer *cony.Consumer, client *cony.Client, handler Handler) {
	defer func() {
		if v := recover(); v != nil {
			log.Printf("Subscriber got panic! Recovering consumer. %s", v)
		}
	}()

	select {
	case msg := <-consumer.Deliveries():
		var eventData map[string]interface{}
		json.Unmarshal(msg.Body, &eventData)

		handler(eventData)
	case err := <-consumer.Errors():
		log.Printf("Consumer error: %v\n", err)
	case err := <-client.Errors():
		log.Printf("Client error: %v\n", err)
	}
}

func Subscribe(exchangeName string, queueName string, handler Handler) {
	go func() {
		connURL := rmq.GetRabbitURL()

		client := cony.NewClient(
			cony.URL(connURL),
			cony.Backoff(cony.DefaultBackoff),
		)

		exchange := cony.Exchange{
			Name:       exchangeName,
			Kind:       "fanout",
			Durable:    true,
			AutoDelete: false,
		}

		queue := &cony.Queue{
			Name:       queueName,
			Durable:    true,
			AutoDelete: true,
			Exclusive:  false,
		}

		bind := cony.Binding{
			Queue:    queue,
			Exchange: exchange,
			Key:      "",
		}

		client.Declare([]cony.Declaration{
			cony.DeclareQueue(queue),
			cony.DeclareExchange(exchange),
			cony.DeclareBinding(bind),
		})

		// register consumer
		consumer := cony.NewConsumer(
			queue,
			cony.AutoAck(),
		)
		client.Consume(consumer)

		log.Printf("Listening on Rabbit MQ exchange [%s] for queue [%s]...", exchangeName, queueName)

		for client.Loop() {
			processConsumer(consumer, client, handler)
		}

	}()
}

func ParsePayloadData(body map[string]interface{}, out interface{}) error {

	if val, exist := body["data"]; exist {
		body = val.(map[string]interface{})
	}

	payloadMarsh, errMarshal := json.Marshal(body)
	if errMarshal != nil {
		return errMarshal
	}

	errUnmarshal := json.Unmarshal(payloadMarsh, out)

	if errUnmarshal != nil {
		return errUnmarshal
	}

	return nil
}

// SubscribeDelay subscribes to delayed exchange
func SubscribeDelay(exchangeName string, queueName string, handler Handler) {
	go func() {
		connURL := rmq.GetRabbitURL()

		client := cony.NewClient(
			cony.URL(connURL),
			cony.Backoff(cony.DefaultBackoff),
		)

		args := amqp.Table{
			"x-delayed-type": "fanout",
		}

		exchange := cony.Exchange{
			Name:       exchangeName,
			Kind:       "x-delayed-message",
			Durable:    true,
			AutoDelete: false,
			Args:       args,
		}

		queue := &cony.Queue{
			Name:       queueName,
			Durable:    true,
			AutoDelete: true,
			Exclusive:  false,
		}

		bind := cony.Binding{
			Queue:    queue,
			Exchange: exchange,
			Key:      "",
		}

		client.Declare([]cony.Declaration{
			cony.DeclareQueue(queue),
			cony.DeclareExchange(exchange),
			cony.DeclareBinding(bind),
		})

		// register consumer
		consumer := cony.NewConsumer(
			queue,
			cony.AutoAck(),
		)
		client.Consume(consumer)

		log.Printf("Listening on Rabbit MQ delayed exchange [%s] for queue [%s]...", exchangeName, queueName)

		for client.Loop() {
			processConsumer(consumer, client, handler)
		}

	}()
}
