package rabbitmq

import (
	"github.com/case-management-suite/queue/api"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AmqpDelivery struct {
	amqpDelivery amqp.Delivery
}

func (d AmqpDelivery) GetID() string {
	return d.amqpDelivery.MessageId
}

func (d AmqpDelivery) Ack() error {
	return d.amqpDelivery.Ack(false)
}

func (d AmqpDelivery) Reject(requeue bool) error {
	return d.amqpDelivery.Reject(requeue)
}

func (d AmqpDelivery) Nack(requeue bool) error {
	return d.amqpDelivery.Nack(false, requeue)
}

func NewAmqpDelivery(amqpDelivery amqp.Delivery) (api.Delivery, error) {
	return api.Delivery{Deliverable: AmqpDelivery{amqpDelivery: amqpDelivery}, Body: amqpDelivery.Body, Meta: api.QueueMeta{Channel: amqpDelivery.Exchange}}, nil
}
