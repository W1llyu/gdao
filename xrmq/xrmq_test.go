package xrmq

import (
	"github.com/streadway/amqp"
	"testing"
)

func TestGetChannel(t *testing.T) {
	client := GetClient()
	ctx := ExchangeCtx{
		Name:       "logs_direct",
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
	channel := client.GetChannel(ctx)
	err := channel.Publish(
		"logs_direct",
		"info",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("testtest"),
		})
	if err != nil {
		t.Error("Publish Failed")
	}
}
