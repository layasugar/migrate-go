package mig

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"log"
	"runtime"
)

type User struct {
	Uid  int64  `json:"uid"`
	Name string `json:"name"`
}

type T1 struct{}

func PayOrderAddColumnSessionNo() {
	var t = TaskFulls{
		T:               new(T1),
		SubNumber:       32,
		TableNameFormat: "user_%d",
		Param: &Param{
			SelectNumber: 2000,
			PoolNumber:   runtime.NumCPU(),
			SourceConn:   getDbClient(),
			SourceTable:  "user",
			Name:         "迁移用户",
		},
	}

	t.Do()
	t.Scan()
}

func (t *T1) Claim() {
	var users []User
	t.Scan(&users)

	var d = make(map[string][]*models.NewPayOrder, 32)
	for _, item := range data {
		sessionNo := cache.GetSessionNoByCache(item.OrderNo)
		if sessionNo == "" {
			continue
		}
		item.ID = 0
		var tmp = models.NewPayOrder{
			PayOrder:  item,
			SessionNo: sessionNo,
		}
		t := utils.GetTableNameBySessionNo(sessionNo)
		d[t] = append(d[t], &tmp)
	}

	for table, item := range d {
		err := conn.PayClient.Clauses(clause.Insert{Modifier: "IGNORE"}).Table(table).Create(item).Error
		if err != nil {
			log.Printf("Claim err: %s", err.Error())
		}
	}
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
