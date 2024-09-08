package rabbitmq

import (
	"fmt"
	"time"

	"github.com/go-errors/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (s *service) getConnection() *amqp.Connection {
	s.connectionLock.Lock()
	defer s.connectionLock.Unlock()

	if s.connection == nil {
		err := s.openConnection()
		if err != nil {
			panic(errors.Wrap(err, 0))
		}
	}

	return s.connection
}

func (s *service) handleClosedConnection() error {
	s.connectionLock.Lock()
	defer s.connectionLock.Unlock()

	var err error

	for retryCount := 0; retryCount < 3; retryCount += 1 {
		secondsToWait := 2 * (retryCount + 1)
		msg := fmt.Sprintf("RabbitMQ connection is closed. Trying to reconnect in %d seconds...", secondsToWait)

		if retryCount > 0 {
			msg = fmt.Sprintf("RabbitMQ connection is closed. Trying to reconnect in %d seconds... (%d)", secondsToWait, retryCount)
		}

		Logger.Warn().Msg(msg)
		time.Sleep(time.Duration(secondsToWait) * time.Second)

		err2 := s.openConnection()
		if err2 != nil {
			err = errors.Wrap(err2, 0)
		} else {
			Logger.Warn().Msg("RabbitMQ reconnected!")
			return nil
		}
	}

	if err != nil {
		return errors.Wrap(err, 0)
	}

	return nil
}

func (s *service) openConnection() error {
	con, err := amqp.Dial(s.url)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	s.connection = con

	go func() {
		if <-s.connection.NotifyClose(make(chan *amqp.Error)) != nil {
			err2 := s.handleClosedConnection()
			if err2 != nil {
				panic(errors.Wrap(err2, 0))
			}
		}
	}()

	return nil
}
