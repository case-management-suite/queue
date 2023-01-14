package gochan

type nchannelType byte

const (
	AckMsg nchannelType = iota
	NackMsg
	RejectMsg
)

type NChannelPayload struct {
	MsgType nchannelType
	ID      string
	Requeue bool
}
type NChannel chan NChannelPayload

type ChanDelivery struct {
	ID              string
	ResponseChannel *NChannel
}
