package xredis

import (
	"fmt"
	"github.com/W1llyu/gdao/config"
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
)

var (
	// 支持同时连接多个redis实例
	pools = make(map[string]*Pool)
	once  sync.Once
)

type Pool struct {
	*redis.Pool
}

func GetPool() *Pool {
	return GetNamedPool("default")
}

func GetNamedPool(name string) *Pool {
	once.Do(initPools)
	pool, ok := pools[name]
	if !ok {
		panic(fmt.Sprintf("Redis Pool named %s not Existed", name))
	}
	return pool
}

func initPools() {
	for k, v := range config.GetConf().Redis {
		initPool(k, v)
	}
}

func initPool(name string, conf *config.RedisConf) {
	redisPool := &redis.Pool{
		MaxIdle:     conf.MaxIdle,
		MaxActive:   conf.MaxActive,
		IdleTimeout: conf.IdleTimeout.Duration,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", conf.Addr)
			if err != nil {
				return nil, err
			}
			if _, err := conn.Do("SELECT", conf.Database); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, nil
		},
		TestOnBorrow: func(conn redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := conn.Do("PING")
			return err
		},
	}
	pool := &Pool{redisPool}
	pools[name] = pool
}

func (p *Pool) GetClient() *Client {
	return &Client{p.Pool.Get()}
}

func (p *Pool) GetPubSubClent() *PubSubClient {
	return &PubSubClient{redis.PubSubConn{Conn: p.Pool.Get()}}
}

func (p *Pool) Close() error {
	return p.Pool.Close()
}
