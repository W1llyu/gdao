package xrmq

import (
	"fmt"
	"github.com/W1llyu/gdao/config"
	"github.com/W1llyu/gdao/utils"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

/**
 * Client包含了和RabbitMQ的connection
 * 包含declare了不同exchange的多个channel
 */
type Client struct {
	Lock     sync.RWMutex
	Conn     *amqp.Connection
	Addr     string
	channels map[string]*Channel
}

var (
	clients = make(map[string]*Client)
	once    sync.Once
)

func GetClient() *Client {
	return GetNamedClient("default")
}

func GetNamedClient(name string) *Client {
	once.Do(initClients)
	client, ok := clients[name]
	if !ok {
		panic(fmt.Sprintf("Redis Pool named %s not Existed", name))
	}
	if client.Conn == nil {
		client.connect()
	}
	return client
}

func (client *Client) Close() error {
	return client.Conn.Close()
}

func (client *Client) GetChannel(ctx ExchangeCtx) *Channel {
	if _, ok := client.channels[ctx.Name]; !ok {
		client.Lock.Lock()
		defer client.Lock.Unlock()
		client.CreateChannel(ctx)
	}
	return client.channels[ctx.Name]
}

// 创建一个Channel并且declare Exchange
func (client *Client) CreateChannel(ctx ExchangeCtx) *Channel {
	ch, _ := client.Conn.Channel()
	err := ch.ExchangeDeclare(
		ctx.Name,
		ctx.Type,
		ctx.Durable,
		ctx.AutoDelete,
		ctx.Internal,
		ctx.NoWait,
		ctx.Args,
	)
	utils.WarnOnError(err, "Failed to declare an exchange")
	channel := &Channel{channel: ch, ExchangeCtx: ctx}
	client.channels[ctx.Name] = channel
	return channel
}

// 初始化Client
func initClients() {
	for k, v := range config.GetConf().RabbitMQ {
		clients[k] = &Client{
			Addr:     v.Addr,
			channels: make(map[string]*Channel),
		}
	}
}

// 阻塞直到建立连接成功
func (client *Client) connect() {
	client.Lock.Lock()
	defer client.Lock.Unlock()
	var err error
	for {
		client.Conn, err = amqp.Dial(client.Addr)
		if err != nil {
			utils.WarnOnError(err, fmt.Sprintf("Connect RabbitMQ %s, error, retrying...", client.Addr))
			time.Sleep(10 * time.Second)
		} else {
			break
		}
	}
}
