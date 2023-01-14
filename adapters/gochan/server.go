package gochan

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/case-management-suite/queue/api"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var ErrNotConnected error = errors.New("not connected")

type ChanClient[T any] struct {
	Channel ClientChannel[T]
}

type ClientChannel[T any] chan T

type ServerChannel chan chan Channel

type ChannelDefinition struct {
	Name      string
	Broadcast bool
}

type Channel struct {
	api.QueueConsumerOutput
	isConnected     bool
	Consumer        string
	Out             ClientChannel[api.Delivery]
	Err             ClientChannel[error]
	Done            ClientChannel[bool]
	Reconnected     ClientChannel[bool]
	Close           ClientChannel[bool]
	ResponseChannel NChannel
}

func (s *Channel) CloseChannels(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Debug().Interface("panic", r).Msg("panic while closing channels")
		}
	}()

	select {
	case s.Done <- true:
	default:
	}

	s.isConnected = false

	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
		close(s.Out)
		close(s.Err)
		close(s.Done)
		close(s.ResponseChannel)
		log.Debug().Msg("Channels closed")
	}()
}

type ChanServer struct {
	IsStarted bool
	Channels  map[string]ChannelDefinition
	Clients   map[string][]Channel
	Log       zerolog.Logger
	// RegisterChannel ServerChannel
}

func (s *ChanServer) Connect(ctx context.Context, channels []ChannelDefinition) {
	s.Clients = map[string][]Channel{}
	s.Channels = map[string]ChannelDefinition{}
	for _, cd := range channels {
		s.Channels[cd.Name] = cd
	}

	s.IsStarted = true
}

func makeChannel(consumer string, ackCh NChannel) *Channel {
	if consumer == "" {
		consumer = uuid.NewString()
	}
	outch := make(chan api.Delivery)
	errch := make(chan error, 10)
	donech := make(chan bool, 10)
	out := Channel{
		Consumer:            consumer,
		isConnected:         true,
		QueueConsumerOutput: api.QueueConsumerOutput{Out: outch, Err: errch, Done: donech},
		Out:                 outch,
		Done:                donech,
		Err:                 errch,
		ResponseChannel:     make(NChannel),
	}
	return &out
}

func (s *ChanServer) Listen(ctx context.Context, channel string, clientName string) (*Channel, error) {
	if !s.IsStarted {
		return nil, ErrNotConnected
	}
	if _, ok := s.Channels[channel]; !ok {
		return nil, fmt.Errorf("channel is not registered, %s", channel)
	}
	ch := makeChannel(clientName, make(NChannel))
	registeredChannels, ok := s.Clients[channel]
	if ok {
		s.Clients[channel] = append(registeredChannels, *ch)
	} else {
		s.Clients[channel] = []Channel{*ch}
	}
	return ch, nil
}

func (s *ChanServer) Push(ctx context.Context, channel string, delivery api.Delivery) error {

	if !s.IsStarted {
		return ErrNotConnected
	}
	var err error
	defer func() {
		if a := recover(); a != nil {
			err = fmt.Errorf("panic: %e", a)
		}
	}()
	if _, ok := s.Channels[channel]; !ok {
		return fmt.Errorf("channel is not registered, %s", channel)
	}
	allAck := make(NChannel)
	clients, ok := s.Clients[channel]
	if ok {
		for _, c := range clients {
			go func(c Channel) {
				consumer := c.Consumer
				delivery = NewChanDelivery(&c, delivery.Body)
				zerolog.Ctx(ctx).Info().Str("client", "").Str("consumer", consumer).Msg("Pushing to client")
				for {
					select {
					case <-allAck:
						return
					case c.Out <- delivery:
					case <-ctx.Done():
					}

					select {
					case <-ctx.Done():
						return
					case <-allAck:
						return
					case msg := <-c.ResponseChannel:
						switch msg.MsgType {
						case AckMsg:
							allAck <- msg
							return
						case NackMsg, RejectMsg:
							if !msg.Requeue {
								allAck <- msg
								return
							}
						}
					}
				}
			}(c)
		}
	}
	return err
}

func (s *ChanServer) Stop() {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	s.IsStarted = false
	for k, v := range s.Clients {
		for _, c := range v {
			c.CloseChannels(ctx)
			s.Log.Debug().Str("client", k).Str("channel", c.Consumer).Msg("Client closed")
		}
	}
}
