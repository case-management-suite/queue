package rabbitmq

import (
	"context"
	"errors"

	"os"
	"runtime"
	"sync"
	"time"

	"github.com/case-management-suite/common/config"
	"github.com/case-management-suite/common/ctxutils"
	"github.com/case-management-suite/common/service"
	"github.com/case-management-suite/queue/api"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueService struct {
	addr          string
	connection    *amqp.Connection
	channel       *amqp.Channel
	done          chan os.Signal
	notifyClose   chan *amqp.Error
	notifyConfirm chan amqp.Confirmation
	isConnected   bool
	alive         bool
	threads       int
	wg            *sync.WaitGroup
	channels      []string
	service.ServiceUtils
}

func (c *QueueService) Connect(ctx context.Context, channels []string) error {
	ctxutils.DecorateContext(ctx, ctxutils.ContextDecoration{Name: "RabbitMQQueueService"})
	c.channels = channels
	if err := c.connect(ctx); err != nil {
		c.isConnected = false
		return err
	}
	c.isConnected = true
	go c.handleReconnect()
	return nil
}

// Close will cleanly shutdown the channel and connection after there are no messages in the system.
func (c *QueueService) Close() error {
	if !c.isConnected {
		return nil
	}
	c.alive = false
	c.Logger.Debug().Msg("Waiting for current messages to be processed...")

	// c.wg.Wait()
	err := c.channel.Close()
	if err != nil {
		return err
	}
	err = c.connection.Close()
	if err != nil {
		return err
	}
	c.isConnected = false
	c.Logger.Info().Msg("gracefully stopped rabbitMQ connection")
	return nil
}

func (c *QueueService) Listen(channel api.Channel, cancelContext context.Context) (*api.QueueConsumerOutput, error) {
	c.waitForConnection(cancelContext, channel)
	// mu := sync.Mutex{}

	c.wg.Add(c.threads)
	c.Logger.Debug().Int("threads", c.threads).Msg("Starting threads")
	cname := uuid.NewString()
	msgs, err := c.registerConsumer(channel, cname)
	if err != nil {
		// TODO: mark as non-registered
		return nil, err
	}
	out := c.mapMessagesChannel(QueueConsumerContext{ctx: cancelContext, channel: channel, consumer: cname, msgs: msgs})
	return &out, nil
	// for i := 1; i <= c.threads; i++ {
	// 	cname := c.consumerName(cname, i)
	// 	c.consumerNames = append(c.consumerNames, cname)
	// }

	// outchan := make(chan api.Delivery, c.threads)
	// errchan := make(chan error, c.threads)
	// donechan := make(chan bool, c.threads)

	// for _, cname := range c.consumerNames {
	// 	msgs, err := c.registerConsumer(channel, cname)
	// 	if err != nil {
	// 		// TODO: mark as non-registered
	// 		continue
	// 	}

	// 	out := c.mapMessagesChannel(QueueConsumerContext{ctx: cancelContext, channel: channel, consumer: cname, msgs: msgs})
	// 	select {
	// 	case o := <-out.Out:
	// 		outchan <- o
	// 	case err := <-out.Err:
	// 		errchan <- err
	// 	case d := <-out.Done:
	// 		donechan <- d
	// 	}
	// }

	// return api.QueueConsumerOutput{Out: outchan, Err: errchan, Done: donechan}
}

func (c *QueueService) Send(cxt context.Context, channel Channel, p []byte, retries int) error {
	connection, ch, err := connectWithRetry(cxt, c.addr, 3, c.channels, c.Logger)
	if err != nil {
		c.Logger.Err(err).Msg("Failed to push to queue")
	}

	defer connection.Close()
	defer ch.Close()

	for {
		retries := 20
		c.Logger.Debug().Str("channel", channel).Msg("Pushing...")
		notifyError, notifyConfirm := preparePush(ch)
		err := unsafePush(cxt, ch, api.Delivery{Meta: api.QueueMeta{Channel: api.Channel(channel)}, Body: p})
		if err != nil {
			if err == ErrDisconnected {
				continue
			}
			return err
		}

		if retries == 0 {
			return errors.New("exceeded push retries")
		}

		retries -= 1

		select {
		case confirm := <-notifyConfirm:
			if confirm.Ack {
				c.Logger.Debug().Msg("Pushed aknowledged")
				return nil
			}
		case <-time.After(resendDelay):
		case err := <-notifyError:
			return err
		}
	}
}

func (c *QueueService) GetConnectionInfo() api.ConnectionInfo {
	return api.ConnectionInfo{Address: c.addr}
}

func (c *QueueService) PurgeAllChannels() (int, error) {
	conn, err := amqp.Dial(c.addr)
	if err != nil {
		c.Logger.Printf("failed to dial rabbitMQ server: %v", err)
		return 0, err
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		c.Logger.Printf("failed connecting to channel: %v", err)
		return 0, err
	}
	defer channel.Close()
	channel.Confirm(false)

	allPurged := 0
	for _, ch := range c.channels {
		purged, err := channel.QueuePurge(ch, false)
		if err != nil {
			c.Logger.Printf("failed to purge stream queue: %v", err)
			return allPurged, err
		}
		allPurged += purged
	}

	return allPurged, nil
}

func NewQueueService(queueConfig config.QueueConnectionConfig, su service.ServiceUtils) api.QueueService {
	threads := runtime.GOMAXPROCS(0)
	if numCPU := runtime.NumCPU(); numCPU > threads {
		threads = numCPU
	}
	// threads = 1

	goChan := make(chan os.Signal, 1)
	// l := log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).Level(queueConfig.LogLevel).With().Logger()
	// logConfig.Logger.ConfigForService("QueueService", queueConfig.LogLevel)

	r := QueueService{
		addr:         queueConfig.Address,
		threads:      threads,
		done:         goChan,
		alive:        true,
		wg:           &sync.WaitGroup{},
		ServiceUtils: su,
	}

	return &r
}
