package config

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/W1llyu/gdao/utils"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	Redis     map[string]*RedisConf
	RabbitMQ  map[string]*RmqConf
	Mysql map[string]*MySqlConf
}

type duration struct {
	time.Duration
}

type RedisConf struct {
	Addr        string
	Database    int
	MaxIdle     int      `toml:"max_idle"`
	MaxActive   int      `toml:"max_active"`
	IdleTimeout duration `toml:"idle_timeout"`
}

type RmqConf struct {
	Addr string
}

type MySqlConf struct {
	Addr string
	User string
	Password string
	DbName string `toml:"dbname"`
	MaxIdle int `toml:"max_idle"`
	MaxOpen int `toml:"max_open"`
}

var (
	cfg  *Config
	once sync.Once
	confPath = fmt.Sprintf("%s/config/gdao/config.toml", os.Getenv("GOPATH"))
)

func SetConfPath(path string) {
	confPath = path
}

func GetConf() *Config {
	once.Do(initConf)
	return cfg
}

func initConf() {
	LoadConf(&cfg, confPath)
}

func LoadConf(c interface{}, path string) {
	filePath, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	utils.Infof("parse toml file. filePath: %s\n", filePath)
	if _, err := toml.DecodeFile(filePath, c); err != nil {
		panic(err)
	}
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
