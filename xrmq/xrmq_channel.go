package xrmq

import (
	"github.com/W1llyu/gdao/utils"
	"github.com/streadway/amqp"
)

type Channel struct {
	channel     *amqp.Channel
	ExchangeCtx ExchangeCtx
}

type ExchangeCtx struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func NewDefaultExchangeCtx() ExchangeCtx {
	return ExchangeCtx{
		Name:       "",
		Type:       "fanout",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

func (ch *Channel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return ch.channel.Publish(exchange, key, mandatory, immediate, msg)
}

func (ch *Channel) Receive(key string, callback interface{}) {
	q := ch.createQueue()
	ch.bindQueueToExchange(q, key)

	msg, err := ch.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	utils.Fatal(err, "Failed to register a consumer")
	for d := range msg {
		go callback.(func(string, string))(key, string(d.Body))
	}
}

func (ch *Channel) createQueue() amqp.Queue {
	q, err := ch.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	utils.WarnOnError(err, "Failed to declare a queue")
	return q
}

func (ch *Channel) bindQueueToExchange(q amqp.Queue, key string) {
	err := ch.channel.QueueBind(
		q.Name,
		key,
		ch.ExchangeCtx.Name,
		false,
		nil,
	)
	utils.WarnOnError(err, "Failed to bind a queue")
	utils.Infof("Bind to Exchange(%s) with key(%s)", ch.ExchangeCtx.Name, key)
}
