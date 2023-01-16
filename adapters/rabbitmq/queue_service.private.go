package rabbitmq

import (
	"context"
	"time"

	"github.com/case-management-suite/common/utils"
	"github.com/case-management-suite/queue/api"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

func (c *QueueService) connect(ctx context.Context) error {
	connection, channel, err := connectWithRetry(ctx, c.addr, 3, c.channels, c.Logger)
	if err != nil {
		return err
	}
	c.changeConnection(connection, channel)
	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the channel listeners to reflect this.
func (c *QueueService) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	c.connection = connection
	c.channel = channel
	c.notifyClose = make(chan *amqp.Error)
	c.notifyConfirm = make(chan amqp.Confirmation)
	c.channel.NotifyClose(c.notifyClose)
	c.channel.NotifyPublish(c.notifyConfirm)
	c.Logger.Debug().Msg("Reconnected")
	c.isConnected = true
}

// handleReconnect will wait for a connection error on
// notifyClose, and then continuously attempt to reconnect.
func (c *QueueService) handleReconnect() {
	for c.alive {
		// c.isConnected = false
		t := time.Now()
		c.Logger.Info().Caller().Str("address", c.addr).Msg("Attempting to connect to rabbitMQ")
		var retryCount int

		for !c.isConnected {
			err := c.connect(context.Background())
			if !c.alive {
				return
			}
			select {
			case <-c.done:
				return
			case <-time.After(reconnectDelay + time.Duration(retryCount)*time.Second):
				c.Logger.Error().Err(err).Msg("disconnected from rabbitMQ and failed to connect")
				retryCount++
			}
		}
		c.Logger.Printf("Connected to rabbitMQ in: %vms", time.Since(t).Milliseconds())
		select {
		case <-c.done:
			return
		case <-c.notifyClose:
		}
	}
}

func (c *QueueService) waitForConnection(ctx context.Context, channel api.Channel) {
	for {
		if c.isConnected {
			c.Logger.Debug().Str("channel", channel).Msg("Connected to listener")
			break
		}
		time.Sleep(1 * time.Second)
		c.Logger.Debug().Interface("context", ctx.Value(utils.ServiceName)).Msg("Waiting for a connection")
	}
}

func (c *QueueService) registerConsumer(channel Channel, consumerName string) (<-chan amqp.Delivery, error) {
	msgs, err := c.channel.Consume(
		channel,
		consumerName, // Consumer
		false,        // Auto-Ack
		false,        // Exclusive
		false,        // No-local
		false,        // No-Wait
		nil,          // Args
	)
	return msgs, err
}

type QueueConsumerContext struct {
	ctx      context.Context
	channel  Channel
	consumer string
	msgs     <-chan amqp.Delivery
}

func (c *QueueService) mapMessagesChannel(queueCtx QueueConsumerContext) api.QueueConsumerOutput {
	consumer := queueCtx.consumer
	channel := queueCtx.channel
	ctx := queueCtx.ctx
	msgs := queueCtx.msgs

	outch := make(chan api.Delivery, 1)
	errch := make(chan error, 1)
	donech := make(chan bool, 1)
	recch := make(chan bool, 1)

	go func() {
		defer c.Logger.Info().Msg("Stopping mapping")
		for {
			// if c.isConnected {
			// 	// c.Logger.Debug().Str("cname", consumer).Str("channel", channel).Msg("Waiting for messages...")
			// }
			select {
			case <-ctx.Done():
				c.Logger.Debug().Str("cname", consumer).Str("channel", channel).Msg("Context cancelled")
				donech <- true
				return
			case msg, ok := <-msgs:

				if !ok {
					c.Logger.Debug().Msg("could not fetch message. Waiting trying...")
					time.Sleep(1 * time.Second)
					nmsgs, err := c.registerConsumer(channel, consumer)
					if err == nil {
						log.Debug().Bool("isConnected", c.isConnected).Msg("No errors registering consumer")
						msgs = nmsgs
						recch <- true
					} else {
						c.Logger.Debug().Err(err).Msg("Could not re-register consumer")
					}
					continue
				}
				parsed, err := NewAmqpDelivery(msg)

				if err != nil {
					c.Logger.Debug().Err(err).Msg("Error handing event")
					errch <- err
					return
				}
				outch <- parsed
			case <-c.notifyClose:
				c.Logger.Debug().Str("cname", consumer).Str("channel", channel).Msg("Notify close. Requeueing pending mesagges")

			innerFor:
				for {
					select {
					case pending := <-outch:
						log.Debug().Msg("Requeueueueue")
						pending.Nack(true)
					case <-donech:
						continue
					default:
						break innerFor
					}
				}
				c.Logger.Debug().Str("cname", consumer).Str("channel", channel).Msg("Requeuing succesful")

				donech <- true
			}

		}
	}()

	return api.QueueConsumerOutput{Out: outch, Err: errch, Done: donech, Reconnected: recch}
}

func preparePush(channel *amqp.Channel) (chan *amqp.Error, chan amqp.Confirmation) {
	notifyClose := make(chan *amqp.Error)
	notifyConfirm := make(chan amqp.Confirmation)
	channel.NotifyClose(notifyClose)
	channel.NotifyPublish(notifyConfirm)
	return notifyClose, notifyConfirm

}

func unsafePush(ctx context.Context, channel *amqp.Channel, delivery api.Delivery) error {
	return channel.PublishWithContext(
		ctx,
		"",                            // Exchange
		string(delivery.Meta.Channel), // Routing key
		false,                         // Mandatory
		false,                         // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        delivery.Body,
		},
	)
}
