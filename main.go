package rabbitmq

import (
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

type Service interface {
	Consume(queues []string, deliveryChannel chan<- amqp.Delivery) error
	DeclareQueues(queues []Queue) error
	Publish(exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error
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

func (q *Queue) GetDeadLetterQueueName() string {
	return fmt.Sprintf("%s.DL", q.Name)
}

var Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
