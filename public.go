package queue

import (
	"github.com/case-management-suite/common/config"
	"github.com/case-management-suite/queue/adapters/gochan"
	"github.com/case-management-suite/queue/adapters/rabbitmq"
	"github.com/case-management-suite/queue/api"
)

type QueueServiceConstructor = func(config.QueueConnectionConfig, config.LogConfig) api.QueueService

func QueueServiceFactory(t config.QueueType) QueueServiceConstructor {
	switch t {
	case config.RabbitMQ:
		return rabbitmq.NewQueueService
	case config.GoChannels:
		return gochan.NewStubQueueService
	default:
		return func(qcc config.QueueConnectionConfig, lc config.LogConfig) api.QueueService {
			panic("Unimplemented QueueType")
		}
	}
}
