package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-errors/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

const deadLetterQueueTtl = 604800000

type Queue struct {
	Name string
	Ttl  time.Duration
}

type Publishing = amqp.Publishing

type Service interface {
	Consume(queues []string, deliveryChannel chan<- amqp.Delivery) error
	DeclareQueues(queues []Queue) error
	Publish(ctx context.Context, queue string, msg Publishing) error
}

type service struct {
	connection           *amqp.Connection
	connectionLock       sync.Mutex
	consumeChannel       *amqp.Channel
	consumeChannelLock   sync.Mutex
	consumeChannelMap    map[string]chan<- amqp.Delivery
	consumePrefetchCount int
	publishChannel       *amqp.Channel
	publishChannelLock   sync.Mutex
	url                  string
}

func New(consumePrefetchCount int, url string) (Service, error) {
	r := service{consumePrefetchCount: consumePrefetchCount, url: url}

	err := r.openConnection()
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	return &r, nil
}

func (r *service) DeclareQueues(queues []Queue) error {
	channel, err := r.getConnection().Channel()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	defer channel.Close()

	for _, queue := range queues {
		queueArgs := map[string]any{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": queue.GetDeadLetterQueueName(),
		}

		if queue.Ttl != 0 {
			queueArgs["x-message-ttl"] = queue.Ttl.Milliseconds()
		}

		_, err = channel.QueueDeclare(queue.Name, true, false, false, false, queueArgs)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		// dead-letter queue
		_, err = channel.QueueDeclare(
			queue.GetDeadLetterQueueName(),
			true,
			false,
			false,
			false,
			map[string]any{
				"x-message-ttl": deadLetterQueueTtl,
			},
		)
		if err != nil {
			return errors.Wrap(err, 0)
		}
	}

	return nil
}

func (q *Queue) GetDeadLetterQueueName() string {
	return fmt.Sprintf("%s.DL", q.Name)
}

var Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
