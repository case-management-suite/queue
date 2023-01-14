package gochan

import (
	"context"
	"errors"

	"github.com/case-management-suite/common/config"
	"github.com/case-management-suite/common/ctxutils"
	"github.com/case-management-suite/queue/api"
	"github.com/rs/zerolog"
)

type ChannelMap map[string]*Channel

type StubQueue struct {
	SimulareConnectionError bool
	isConnected             bool
	Server                  ChanServer
}

func (s *StubQueue) Send(ctx context.Context, channel api.Channel, p []byte, retries int) error {
	if !s.isConnected {
		return errors.New("not connected")
	}
	if s.SimulareConnectionError {
		return errors.New("connection error")
	}
	return s.Server.Push(ctx, channel, api.Delivery{Body: p})
	// return sendWithAck(ctx, ch, p, 3)
}
func (s *StubQueue) GetConnectionInfo() api.ConnectionInfo {
	return api.ConnectionInfo{Address: "chan"}
}
func (s *StubQueue) Listen(channel api.Channel, cancelContext context.Context) (*api.QueueConsumerOutput, error) {
	if s.SimulareConnectionError {
		return nil, errors.New("connection error")
	}
	ch, err := s.Server.Listen(cancelContext, channel, "")
	if err != nil {
		return nil, err
	}
	out := api.QueueConsumerOutput{Out: ch.Out, Err: ch.Err, Done: ch.Done, Reconnected: ch.Reconnected, Close: ch.Close}
	return &out, nil
}
func (s *StubQueue) Connect(ctx context.Context, channels []string) error {
	ctxutils.DecorateContext(ctx, ctxutils.ContextDecoration{Name: "GoChanQueueService"})
	s.Server = ChanServer{Log: *zerolog.Ctx(ctx)}
	defs := []ChannelDefinition{}

	for _, v := range channels {
		defs = append(defs, ChannelDefinition{Name: v})
	}

	s.Server.Connect(ctx, defs)
	s.isConnected = true
	return nil
}
func (s *StubQueue) Close() error {
	s.Server.Stop()
	s.isConnected = false
	return nil
}
func (s *StubQueue) PurgeAllChannels() (int, error) {
	return 0, nil
}

func NewStubQueueService(conf config.QueueConnectionConfig, logConfig config.LogConfig) api.QueueService {
	s := StubQueue{}
	return &s
}

func NewStubQueueServiceForTests(conf config.QueueConnectionConfig, simulateConnectionError bool) api.QueueService {
	s := StubQueue{SimulareConnectionError: simulateConnectionError}
	return &s
}
