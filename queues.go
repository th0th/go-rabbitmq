package rabbitmq

import (
	"github.com/go-errors/errors"
)

func (r *service) DeclareQueues(queues []Queue) error {
	channel, err := r.getConnection().Channel()
	if err != nil {
		return errors.Wrap(err, 0)
	}

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

	return channel.Close()
}
