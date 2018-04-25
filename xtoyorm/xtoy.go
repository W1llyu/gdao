package xtoyorm

import (
	"fmt"
	"sync"
	"github.com/W1llyu/gdao/config"
	"github.com/bigpigeon/toyorm"
	"github.com/W1llyu/gdao/utils"
	_ "github.com/go-sql-driver/mysql"
)

var (
	once sync.Once
	toys = make(map[string]*toyorm.Toy)
)

func Get() *toyorm.Toy {
	return GetNamedDB("default")
}

func GetNamedDB(name string) *toyorm.Toy {
	once.Do(initDBs)
	toy, ok := toys[name]
	if !ok {
		panic(fmt.Sprintf("Db instance %s not Existed", name))
	}
	return toy
}

func connect(conf *config.MySqlConf) *toyorm.Toy {
	conStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", conf.User, conf.Password, conf.Addr, conf.DbName)
	toy, err := toyorm.Open("mysql", conStr)
	if err != nil {
		utils.Error(err, fmt.Sprintf("[ERROR] Cannot Connect to %s", conStr))
	}
	return toy
}

func initDBs() {
	for k, v := range config.GetConf().Mysql {
		toys[k] = connect(v)
	}
}