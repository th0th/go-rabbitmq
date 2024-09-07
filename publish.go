package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/go-errors/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *service) Publish(exchange string, key string, mandatory bool, immediate bool, msg Publishing) error {
	return r.getPublishChannel().PublishWithContext(context.Background(), exchange, key, mandatory, immediate, msg)
}

func (r *service) getPublishChannel() *amqp.Channel {
	r.publishChannelLock.Lock()
	defer r.publishChannelLock.Unlock()

	if r.publishChannel == nil {
		err := r.openPublishChannel()
		if err != nil {
			panic(errors.Wrap(err, 0))
		}
	}

	return r.publishChannel
}

func (r *service) handleClosedPublishChannel() error {
	r.publishChannelLock.Lock()
	defer r.publishChannelLock.Unlock()

	r.getConnection()

	var err error

	for retryCount := 0; retryCount < 3; retryCount += 1 {
		secondsToWait := retryCount + 1
		msg := fmt.Sprintf("RabbitMQ publish channel is closed. Trying to reopen in %d seconds...", secondsToWait)

		if retryCount > 0 {
			msg = fmt.Sprintf("RabbitMQ publish channel is closed. Trying to reopen in %d seconds... (%d)", secondsToWait, retryCount)
		}

		Logger.Warn().Msg(msg)
		time.Sleep(time.Duration(secondsToWait) * time.Second)

		err2 := r.openPublishChannel()
		if err2 != nil {
			err = errors.Wrap(err2, 0)
		} else {
			Logger.Warn().Msg("Reopened the RabbitMQ publish channel!")

			return nil
		}
	}

	return errors.Wrap(err, 0)
}

func (r *service) openPublishChannel() error {
	ch, err := r.getConnection().Channel()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	r.publishChannel = ch

	go func() {
		if <-r.publishChannel.NotifyClose(make(chan *amqp.Error)) != nil {
			err2 := r.handleClosedPublishChannel()
			if err2 != nil {
				panic(err2)
			}
		}
	}()

	return nil
}
