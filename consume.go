package rabbitmq

import (
	"fmt"
	"time"

	"github.com/go-errors/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (s *service) Consume(queues []string, deliveryChannel chan<- amqp.Delivery) error {
	if s.consumeChannelMap == nil {
		s.consumeChannelMap = map[string]chan<- amqp.Delivery{}
	}

	for _, queue := range queues {
		s.consumeChannelMap[queue] = deliveryChannel
	}

	err := s.startConsuming()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	return nil
}

func (s *service) startConsuming() error {
	errs := []error{}

	for queue, c := range s.consumeChannelMap {
		deliveryChannel, err := s.getConsumeChannel().Consume(queue, "", false, false, false, false, nil)
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

func (s *service) getConsumeChannel() *amqp.Channel {
	s.consumeChannelLock.Lock()
	defer s.consumeChannelLock.Unlock()

	if s.consumeChannel == nil {
		err := s.openConsumeChannel()
		if err != nil {
			panic(errors.Wrap(err, 0))
		}
	}

	return s.consumeChannel
}

func (s *service) handleClosedConsumeChannel() error {
	s.getConnection()

	s.consumeChannelLock.Lock()
	defer s.consumeChannelLock.Unlock()

	var err error

	for retryCount := 0; retryCount < 3; retryCount += 1 {
		secondsToWait := retryCount + 1
		msg := fmt.Sprintf("RabbitMQ consume channel is closed. Trying to reopen in %d seconds...", secondsToWait)

		if retryCount > 0 {
			msg = fmt.Sprintf("RabbitMQ consume channel is closed. Trying to reopen in %d seconds... (%d)", secondsToWait, retryCount)
		}

		Logger.Warn().Msg(msg)
		time.Sleep(time.Duration(secondsToWait) * time.Second)

		err2 := s.openConsumeChannel()
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

func (s *service) openConsumeChannel() error {
	ch, err := s.getConnection().Channel()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	s.consumeChannel = ch

	err = s.consumeChannel.Qos(s.consumePrefetchCount, 0, false)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	go func() {
		if <-s.consumeChannel.NotifyClose(make(chan *amqp.Error)) != nil {
			err2 := s.handleClosedConsumeChannel()
			if err2 != nil {
				panic(err2)
			}

			err2 = s.startConsuming()
			if err2 != nil {
				panic(err2)
			}
		}
	}()

	return nil
}
