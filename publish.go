package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/go-errors/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (s *service) Publish(ctx context.Context, queue string, msg Publishing) error {
	return s.getPublishChannel().PublishWithContext(ctx, "", queue, false, false, msg)
}

func (s *service) getPublishChannel() *amqp.Channel {
	s.publishChannelLock.Lock()
	defer s.publishChannelLock.Unlock()

	if s.publishChannel == nil {
		err := s.openPublishChannel()
		if err != nil {
			panic(errors.Wrap(err, 0))
		}
	}

	return s.publishChannel
}

func (s *service) handleClosedPublishChannel() error {
	s.publishChannelLock.Lock()
	defer s.publishChannelLock.Unlock()

	s.getConnection()

	var err error

	for retryCount := 0; retryCount < 3; retryCount += 1 {
		secondsToWait := retryCount + 1
		msg := fmt.Sprintf("RabbitMQ publish channel is closed. Trying to reopen in %d seconds...", secondsToWait)

		if retryCount > 0 {
			msg = fmt.Sprintf("RabbitMQ publish channel is closed. Trying to reopen in %d seconds... (%d)", secondsToWait, retryCount)
		}

		Logger.Warn().Msg(msg)
		time.Sleep(time.Duration(secondsToWait) * time.Second)

		err2 := s.openPublishChannel()
		if err2 != nil {
			err = errors.Wrap(err2, 0)
		} else {
			Logger.Warn().Msg("Reopened the RabbitMQ publish channel!")

			return nil
		}
	}

	return errors.Wrap(err, 0)
}

func (s *service) openPublishChannel() error {
	ch, err := s.getConnection().Channel()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	s.publishChannel = ch

	go func() {
		if <-s.publishChannel.NotifyClose(make(chan *amqp.Error)) != nil {
			err2 := s.handleClosedPublishChannel()
			if err2 != nil {
				panic(err2)
			}
		}
	}()

	return nil
}
