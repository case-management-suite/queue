package rabbitmq

type QueueEvent struct {
	Job  string `json:"job"`
	UUID string `json:"uuid"`
	Data string `json:"data"`
}

type QueueEventHandler = func([]byte) error

type ConnectionInfo struct {
	Address string
}

type Channel = string

type ChannelManager interface {
	GetAllChannels() []Channel
}
