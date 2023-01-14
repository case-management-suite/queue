package api

import "context"

type ConnectionInfo struct {
	Address string
}

type Channel = string

type QueueMeta struct {
	Channel Channel
}

type Delivery struct {
	Meta QueueMeta
	Body []byte
	Deliverable
}

type Deliverable interface {
	GetID() string
	Ack() error
	Reject(requeue bool) error
	Nack(requeue bool) error
}

type QueueConsumerOutput struct {
	Out         <-chan Delivery
	Err         <-chan error
	Done        <-chan bool
	Reconnected <-chan bool
	Close       chan bool
}

type QueueService interface {
	Send(cxt context.Context, channel Channel, p []byte, retries int) error
	GetConnectionInfo() ConnectionInfo
	Listen(channel Channel, cancelContext context.Context) (*QueueConsumerOutput, error)
	Connect(ctx context.Context, channels []string) error
	Close() error
	PurgeAllChannels() (int, error)
}
