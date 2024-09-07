package rabbitmq

import (
	"fmt"
	"time"

	"github.com/go-errors/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *service) Consume(queues []string, deliveryChannel chan<- amqp.Delivery) error {
	if r.consumeChannelMap == nil {
		r.consumeChannelMap = map[string]chan<- amqp.Delivery{}
	}

	for _, queue := range queues {
		r.consumeChannelMap[queue] = deliveryChannel
	}

	err := r.startConsuming()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	return nil
}

func (r *service) startConsuming() error {
	errs := []error{}

	for queue, c := range r.consumeChannelMap {
		deliveryChannel, err := r.getConsumeChannel().Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			errs = append(errs, errors.Wrap(err, 0))
		} else {
			channel := c
			go func() {
				for delivery := range deliveryChannel {
					channel <- delivery
				}
			}()
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (r *service) getConsumeChannel() *amqp.Channel {
	r.consumeChannelLock.Lock()
	defer r.consumeChannelLock.Unlock()

	if r.consumeChannel == nil {
		err := r.openConsumeChannel()
		if err != nil {
			panic(errors.Wrap(err, 0))
		}
	}

	return r.consumeChannel
}

func (r *service) handleClosedConsumeChannel() error {
	r.getConnection()

	r.consumeChannelLock.Lock()
	defer r.consumeChannelLock.Unlock()

	var err error

	for retryCount := 0; retryCount < 3; retryCount += 1 {
		secondsToWait := retryCount + 1
		msg := fmt.Sprintf("RabbitMQ consume channel is closed. Trying to reopen in %d seconds...", secondsToWait)

		if retryCount > 0 {
			msg = fmt.Sprintf("RabbitMQ consume channel is closed. Trying to reopen in %d seconds... (%d)", secondsToWait, retryCount)
		}

		Logger.Warn().Msg(msg)
		time.Sleep(time.Duration(secondsToWait) * time.Second)

		err2 := r.openConsumeChannel()
		if err2 != nil {
			err = errors.Wrap(err2, 0)
		} else {
			Logger.Warn().Msg("Reopened the RabbitMQ consume channel!")

			return nil
		}
	}

	if err != nil {
		return errors.Wrap(err, 0)
	}

	return nil
}

func (r *service) openConsumeChannel() error {
	ch, err := r.getConnection().Channel()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	r.consumeChannel = ch

	err = r.consumeChannel.Qos(r.consumePrefetchCount, 0, false)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	go func() {
		if <-r.consumeChannel.NotifyClose(make(chan *amqp.Error)) != nil {
			err2 := r.handleClosedConsumeChannel()
			if err2 != nil {
				panic(err2)
			}

			err2 = r.startConsuming()
			if err2 != nil {
				panic(err2)
			}
		}
	}()

	return nil
}
