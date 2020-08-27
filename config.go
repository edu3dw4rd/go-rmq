package rmq

import (
	"fmt"

	"github.com/edu3dw4rd/gohelper"
)

type RabbitMqConfig struct {
	host     string
	port     string
	user     string
	password string
}

func GetRabbitURL() (rabbitURL string) {
	rabbitConfig := RabbitMqConfig{
		host:     gohelper.GetEnv("RABBIT_HOST", "rabbitmq-management"),
		port:     gohelper.GetEnv("RABBIT_PORT", "5672"),
		user:     gohelper.GetEnv("RABBIT_USER", "guest"),
		password: gohelper.GetEnv("RABBIT_PASSWORD", "dXDJuYLsZ6JCpKwv"),
	}

	rabbitURL = fmt.Sprintf("amqp://%s:%s@%s:%s",
		rabbitConfig.user,
		rabbitConfig.password,
		rabbitConfig.host,
		rabbitConfig.port,
	)

	return
}
