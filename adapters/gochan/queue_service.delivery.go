package gochan

import (
	"github.com/case-management-suite/queue/api"
	"github.com/google/uuid"
)

func sendIfChannelAvailable[T NChannel](c *T, id string, value bool, t nchannelType) error {
	if c != nil {
		go func() {
			*c <- NChannelPayload{ID: id, Requeue: value, MsgType: t}
		}()
	}
	return nil
}

func (c ChanDelivery) GetID() string {
	return c.ID
}
func (c ChanDelivery) Ack() error {
	return sendIfChannelAvailable(c.ResponseChannel, c.ID, false, AckMsg)
}
func (c ChanDelivery) Nack(value bool) error {
	return sendIfChannelAvailable(c.ResponseChannel, c.ID, value, NackMsg)
}
func (c ChanDelivery) Reject(value bool) error {
	return sendIfChannelAvailable(c.ResponseChannel, c.ID, value, NackMsg)
}

func NewChanDelivery(consumerOutput *Channel, body []byte) api.Delivery {
	return api.Delivery{Deliverable: ChanDelivery{
		ID:              uuid.NewString(),
		ResponseChannel: &consumerOutput.ResponseChannel,
	}, Body: body}
}
