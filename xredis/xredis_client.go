package xredis

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

type Client struct {
	conn redis.Conn
}

func GetClient() *Client {
	return GetPool().GetClient()
}

func GetNamedClient(name string) *Client {
	return GetNamedPool(name).GetClient()
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Err() error {
	return c.conn.Err()
}

func (c *Client) Hkeys(key string) ([]string, error) {
	return redis.Strings(c.conn.Do("HKEYS", key))
}

func (c *Client) Hget(key, field string) (string, error) {
	return redis.String(c.conn.Do("HGET", key, field))
}

func (c *Client) Hincrby(key, field string, value int) (int, error) {
	return redis.Int(c.conn.Do("HINCRBY", key, field, value))
}

func (c *Client) Hdel(key, field string) error {
	_, err := c.conn.Do("HDEL", key, field)
	return err
}

func (c *Client) Del(key string) error {
	_, err := c.conn.Do("DEL", key)
	return err
}

func (c *Client) Set(key, value string) error {
	_, err := c.conn.Do("SET", key, value)
	return err
}

func (c *Client) Get(key string) (string, error) {
	return redis.String(c.conn.Do("GET", key))
}

func (c *Client) Expire(key string, duration time.Duration) error {
	_, err := c.conn.Do("EXPIRE", key, duration.Seconds())
	return err
}

func (c *Client) Lpop(key string) (string, error) {
	return redis.String(c.conn.Do("LPOP", key))
}

func (c *Client) Consume(key string, callback interface{}) (){
	for {
		msg, _ := c.Lpop(key)
		if msg == "" {
			time.Sleep(1*time.Second)
			continue
		}
		go callback.(func(string, string))(key, msg)
	}
}
