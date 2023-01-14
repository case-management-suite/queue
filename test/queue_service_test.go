package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/case-management-suite/common/config"
	"github.com/case-management-suite/common/utils"
	"github.com/case-management-suite/queue"
	"github.com/case-management-suite/testutil"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var tests = []struct {
	adapter     config.QueueType
	integration bool
}{
	{adapter: config.RabbitMQ, integration: true},
	{adapter: config.GoChannels},
}

type TestData struct {
	OtherID string
	Index   int
}

func (t TestData) string() string {
	return fmt.Sprintf("TestData {OtherID : %s, Index: %d}", t.OtherID, t.Index)
}

func TestSendAndReceiveN(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: false})
	for _, testData := range tests {
		t.Run(fmt.Sprintf("TestSendAndReceiveN (%s)", testData.adapter), func(t *testing.T) {

			channel := "testchannel3"
			appConfig := config.NewLocalAppConfig()
			newQueueService := queue.QueueServiceFactory(testData.adapter)
			service := newQueueService(appConfig.RulesServiceConfig.QueueConfig, appConfig.LogConfig)
			defer func() {
				log.Info().Msg("Closing service...")
				defer service.Close()
			}()

			service.PurgeAllChannels()

			startCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			startCtx = utils.DecorateContext(startCtx, "TestSendAndReceiveN")
			defer cancel()
			// sch := make(chan []byte, 13)
			err := service.Connect(startCtx, []string{channel})
			if err != nil {
				if *integration {
					log.Info().Msg("Could not connect to the queue. Integration tests are enabled")
					t.FailNow()
				} else if testData.integration {
					log.Info().Msg("Could not connect to the queue. Skipping test")
					t.Skip()
				}
			}

			out, err := service.Listen(channel, startCtx)
			if err != nil {
				log.Error().Err(err).Msg("Error while starting listener")
				t.Fail()
			}

			reduce, current, done := testutil.CounterSeqUtil(10, func(i int) {
				id2 := uuid.NewString()

				p := TestData{OtherID: id2, Index: i}
				log.Debug().Str("payload", p.string()).Msg("Sending...")
				dataStr, err := json.Marshal(p)
				if err != nil {
					log.Error().Err(err)
				}

				service.Send(startCtx, channel, dataStr, 5)
				log.Debug().Msg("Sending message")
			})

		testloop:
			for {
				log.Debug().Msg("Loop...")
				select {
				case res := <-out.Out:
					d := TestData{}
					json.Unmarshal(res.Body, &d)
					log.Debug().Str("payload", d.string()).Msg("Received")
					count, err := reduce(d.Index)
					testutil.AssertNilError(err, t)
					log.Debug().Int("count", count).Msg("Add to count")
					res.Ack()
					if done() {
						break testloop
					}
				case <-out.Done:
					log.Error().Int("count", current()).Msg("Was done before getting all messages")
					t.FailNow()
				case err := <-out.Err:
					log.Error().Err(err).Int("count", current()).Msg("Received an error from consumer")
					t.FailNow()
				}
			}
			log.Debug().Msg("end")
		})
	}
}

func TestSendAndReceiveNWithTimeout(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: false})
	for _, testData := range tests {
		t.Run(fmt.Sprintf("TestSendAndReceiveN (%s)", testData.adapter), func(t *testing.T) {

			channel := "testchannel3"
			appConfig := config.NewLocalAppConfig()
			newQueueService := queue.QueueServiceFactory(testData.adapter)
			service := newQueueService(appConfig.RulesServiceConfig.QueueConfig, appConfig.LogConfig)

			defer func() {
				log.Info().Msg("Closing service...")
				defer service.Close()
			}()

			startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			startCtx = utils.DecorateContext(startCtx, "TestSendAndReceiveNWithTimeout")

			defer cancel()
			// sch := make(chan []byte, 13)
			err := service.Connect(startCtx, []string{channel})
			if err != nil {
				if *integration {
					log.Info().Msg("Could not connect to the queue. Integration tests are enabled")
					t.FailNow()
				} else {
					log.Info().Msg("Could not connect to the queue. Skipping test")
					t.Skip()
				}
			}

			out, err := service.Listen(channel, startCtx)
			if err != nil {
				log.Error().Err(err).Msg("Error while starting listener")
				t.Fail()
			}

			reduce, current, done := testutil.CounterSeqUtil(10, func(i int) {
				id2 := uuid.NewString()

				p := TestData{OtherID: id2, Index: i}
				// log.Debug().Str("payload", p.string()).Msg("Sending...")
				dataStr, err := json.Marshal(p)
				if err != nil {
					log.Error().Err(err)
				}

				service.Send(startCtx, channel, dataStr, 5)
				log.Debug().Msg("Sending message")
			})

			count := 0

			service.Close()

		testloop:
			for {
				select {
				case res := <-out.Out:
					d := TestData{}
					json.Unmarshal(res.Body, &d)
					err := res.Ack()
					if err == nil {
						log.Debug().Str("payload", d.string()).Msg("Received")
						_, err = reduce(d.Index)
						testutil.AssertNilError(err, t)
						log.Debug().Int("count", d.Index).Msg("Add to count")
					} else {
						log.Error().Err(err).Msg("Could not ACK")
					}

					if done() {
						break testloop
					} else {
						log.Debug().Int("count_left", current()).Bool("isDone", done()).Msg("Expected messages left")
					}
				case <-time.After(3 * time.Second):
					err := service.Connect(startCtx, []string{channel})
					if err != nil {
						log.Error().Err(err).Caller().Msg("Error reconnecting")
					} else {
						log.Debug().Caller().Msg("Reconnect!")
					}
					out, err = service.Listen(channel, startCtx)
					testutil.AssertNilError(err, t)
				case err := <-out.Err:
					log.Error().Caller().Err(err).Int("count", count).Msg("Received an error from consumer")
					t.FailNow()
				case <-out.Reconnected:
					log.Info().Caller().Msg("Reconnected. Success!")
				case <-startCtx.Done():
					t.FailNow()
					return
				}

			}
		})
	}
}
