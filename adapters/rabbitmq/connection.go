package rabbitmq

import (
	"context"
	"time"

	"github.com/case-management-suite/common/logger"
	"github.com/case-management-suite/common/utils"
	"github.com/case-management-suite/queue/api"
	amqp "github.com/rabbitmq/amqp091-go"
)

const RETRY_TIME = 2 * time.Second

func connectWithRetry(ctx context.Context, address string, retries int, channels []Channel, logger logger.Logger) (*amqp.Connection, *amqp.Channel, error) {
	var connected bool = false
	var connection *amqp.Connection
	var channel *amqp.Channel
	var err error
loop:
	for !connected && retries > 0 {
		connection, channel, err = connect(api.ConnectionInfo{Address: address}, channels, logger)
		if err == nil {
			break
		}
		select {
		case <-time.After(RETRY_TIME):
			logger.Debug().Interface("context", ctx.Value(utils.ServiceName)).Msg("Could not connect. Retrying...")
		case <-ctx.Done():
			logger.Debug().Msg("Could not connect. Context timed out")
			break loop
		}
		retries -= 1
	}
	return connection, channel, err
}

func connect(c api.ConnectionInfo, channels []Channel, logger logger.Logger) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(c.Address)
	if err != nil {
		return nil, nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	channel.Confirm(false)

	for _, ch := range channels {
		_, err = channel.QueueDeclare(
			ch,
			true,  // Durable
			false, // Delete when unused
			false, // Exclusive
			false, // No-wait
			nil,   // Arguments
		)
		if err != nil {
			logger.Printf("failed to declare stream queue: %v", err)
			return nil, nil, err
		}
	}
	return conn, channel, nil
}
