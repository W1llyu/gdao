package xgorm

import (
	"fmt"
	"sync"
	"github.com/W1llyu/gdao/config"
	"github.com/W1llyu/gdao/utils"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

var (
	once sync.Once
	dbs = make(map[string]*gorm.DB)
)

func Get() *gorm.DB {
	return GetNamedDB("default")
}

func GetNamedDB(name string) *gorm.DB {
	once.Do(initDBs)
	db, ok := dbs[name]
	if !ok {
		panic(fmt.Sprintf("Db instance %s not Existed", name))
	}
	err := db.DB().Ping()
	if err != nil {
		db = connect(config.GetConf().Mysql[name])
		dbs[name] = db
	}
	return db
}

func connect(conf *config.MySqlConf) *gorm.DB {
	conStr := fmt.Sprintf("%s:%s@(%s)/%s", conf.User, conf.Password, conf.Addr, conf.DbName)
	db, err := gorm.Open("mysql", fmt.Sprintf("%s?charset=utf8&parseTime=True&loc=Local", conStr))
	if err != nil {
		utils.Error(err, fmt.Sprintf("[ERROR] Cannot Connect to %s", conStr))
	} else {
		db.DB().SetMaxIdleConns(conf.MaxIdle)
		db.DB().SetMaxOpenConns(conf.MaxOpen)
	}
	return db
}

func initDBs() {
	for k, v := range config.GetConf().Mysql {
		dbs[k] = connect(v)
	}
}