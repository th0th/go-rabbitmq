package rabbitmq

import (
	"fmt"
	"time"

	"github.com/go-errors/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *service) getConnection() *amqp.Connection {
	r.connectionLock.Lock()
	defer r.connectionLock.Unlock()

	if r.connection == nil {
		err := r.openConnection()
		if err != nil {
			panic(errors.Wrap(err, 0))
		}
	}

	return r.connection
}

func (r *service) handleClosedConnection() error {
	r.connectionLock.Lock()
	defer r.connectionLock.Unlock()

	var err error

	for retryCount := 0; retryCount < 3; retryCount += 1 {
		secondsToWait := 2 * (retryCount + 1)
		msg := fmt.Sprintf("RabbitMQ connection is closed. Trying to reconnect in %d seconds...", secondsToWait)

		if retryCount > 0 {
			msg = fmt.Sprintf("RabbitMQ connection is closed. Trying to reconnect in %d seconds... (%d)", secondsToWait, retryCount)
		}

		Logger.Warn().Msg(msg)
		time.Sleep(time.Duration(secondsToWait) * time.Second)

		err2 := r.openConnection()
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

func (r *service) openConnection() error {
	con, err := amqp.Dial(r.url)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	r.connection = con

	go func() {
		if <-r.connection.NotifyClose(make(chan *amqp.Error)) != nil {
			err2 := r.handleClosedConnection()
			if err2 != nil {
				panic(errors.Wrap(err2, 0))
			}
		}
	}()

	return nil
}
