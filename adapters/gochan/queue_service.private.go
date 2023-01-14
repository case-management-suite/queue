package gochan

import (
	"context"
	"fmt"
	"time"
)

const CHANNEL_SIZE = 100

func sendWithAck(ctx context.Context, out *Channel, body []byte, retries int) error {
	var err error
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("panic: %v", x)
		}
	}()
	delivery := NewChanDelivery(out, body)
	// id := delivery.GetID()

	// requeue := func(requeue bool) {
	// 	if requeue {
	// 		go func() {
	// 			select {
	// 			case out.Out <- delivery:
	// 				zerolog.Ctx(ctx).Debug().Msg("Re-queued")
	// 			// case <-time.After(time.Second):
	// 			case <-ctx.Done():
	// 				return
	// 			}
	// 		}()
	// 	}
	// }

	select {
	case <-ctx.Done():
	case <-out.Done:
	case out.Out <- delivery:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("failed to send: Timeout")
	}

	return err
}
