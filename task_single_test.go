package mig

import (
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"runtime"
	"testing"
)

type User struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func TestTask_Do(t *testing.T) {
	var task = Task{
		Param: &Param{
			SelectNumber: 1,
			PoolNumber:   runtime.NumCPU(),
			SourceConn:   getDbClient(),
			SourceTable:  "user",
			Name:         "迁移用户",
		},
		Claim: Claim,
	}
	task.Do()
}

func Claim(rows []map[string]interface{}) {
	var d = make([]*User, 0, len(rows))
	for _, item := range rows {
		var tmp User
		err := mapstructure.Decode(&item, &tmp)
		if err != nil {
			log.Print(err.Error())
		}

		d = append(d, &tmp)
	}

	log.Println(getString(d))
}

func getDbClient() *gorm.DB {
	var Db *gorm.DB
	type DbConf struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Host     string `json:"host"`
	}
	cfg := DbConf{
		User:     "root",
		Password: "123456",
		Host:     "127.0.0.1:3306",
	}
	database := "test"
	addr := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&parseTime=True&loc=Local", cfg.User, cfg.Password, cfg.Host, database)
	mysqlConf := mysql.Config{
		DSN: addr,
	}
	gormConf := &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger:                 logger.Default.LogMode(logger.Error),
	}
	sqlDB, err := gorm.Open(mysql.New(mysqlConf), gormConf)
	if err != nil {
		panic("failed to connect database")
	}
	Db = sqlDB
	db, err := Db.DB()
	if nil != err {
		panic("failed to connect database")
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(5)

	err = db.Ping()
	if nil != err {
		panic("failed to connect database")
	}
	return sqlDB
}

// GetString 只能是map和slice
func getString(d interface{}) string {
	bytesD, err := json.Marshal(d)
	if err != nil {
		return fmt.Sprintf("%v", d)
	} else {
		return string(bytesD)
	}
}
