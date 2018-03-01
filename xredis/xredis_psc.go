package xredis

import (
	"github.com/W1llyu/gdao/utils"
	"github.com/garyburd/redigo/redis"
)

/**
 * Client For Publish or Subscription 用于订阅发布
 */
type PubSubClient struct {
	conn redis.PubSubConn
}

func GetPubSubClient() *PubSubClient {
	return GetPool().GetPubSubClent()
}

func (psc *PubSubClient) Subscribe(channel ...interface{}) {
	psc.conn.Subscribe(channel...)
}

func (psc *PubSubClient) Receive(callback interface{}) {
	for {
		switch v := psc.conn.Receive().(type) {
		case redis.Message:
			data, err := redis.String(v.Data, nil)
			if err != nil {
				utils.Error(err, "cannot parse data")
			}
			go callback.(func(string, string))(v.Channel, data)
		case redis.Subscription:
			utils.Infof("Redis PubSubClient %s: %s %d\n", v.Kind, v.Channel, v.Count)
		case error:
			return
		}
	}
}

func (psc *PubSubClient) Close() error {
	return psc.conn.Close()
}
