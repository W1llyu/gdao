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
	Conn     *amqp.Connection
	Addr     string
	channels map[string]*Channel
	active   bool
	cn       chan int
	initLock sync.RWMutex
	conLock  sync.RWMutex
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
		panic(fmt.Sprintf("RMQ Client named %s not Existed", name))
	}
	if client.Conn == nil {
		client.initLock.Lock()
		if client.Conn == nil {
			client.connect()
			go func() {
				for err := range client.Conn.NotifyClose(make(chan *amqp.Error)) {
					if err != nil {
						client.cn <- 1
						client.channels = make(map[string]*Channel)
						client.active = false
						client.connect()
					}
				}
			}()
		}
		client.initLock.Unlock()
	}
	return client
}

func (client *Client) Close() error {
	return client.Conn.Close()
}

func (client *Client) GetChannel(ctx ExchangeCtx) *Channel {
	if _, ok := client.channels[ctx.Name]; !ok {
		client.conLock.Lock()
		defer client.conLock.Unlock()
		client.CreateChannel(ctx)
	}
	return client.channels[ctx.Name]
}

// 创建一个Channel并且declare Exchange
func (client *Client) CreateChannel(ctx ExchangeCtx) *Channel {
	ch, err := client.Conn.Channel()
	utils.WarnOnError(err, "Failed to Create Channel")
	if err != nil {
		return nil
	}
	err = ch.ExchangeDeclare(
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

func (client *Client) Receive(ctx ExchangeCtx, key string, callback interface{}) {
	for {
		ch := client.GetChannel(ctx)
		if ch == nil {
			continue
		}
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
		utils.WarnOnError(err, "Failed to register a consumer")
		if err != nil {
			continue
		}
		for {
			select {
			case <-client.cn:
				goto ForEnd
			case d := <-msg:
				go callback.(func(string, string))(key, string(d.Body))
			}
		}
		ForEnd:
	}
}

// 初始化Client
func initClients() {
	for k, v := range config.GetConf().RabbitMQ {
		clients[k] = &Client{
			Addr:     v.Addr,
			channels: make(map[string]*Channel),
			active:   false,
			cn:       make(chan int),
		}
	}
}

// 阻塞直到建立连接成功
func (client *Client) connect() {
	client.conLock.Lock()
	defer client.conLock.Unlock()
	var err error
	for {
		client.Conn, err = amqp.Dial(client.Addr)
		if err != nil {
			utils.WarnOnError(err, fmt.Sprintf("Connect RabbitMQ %s, error, retrying...", client.Addr))
			time.Sleep(10 * time.Second)
		} else {
			client.active = true
			break
		}
	}
}
