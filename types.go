package mig

import (
	"database/sql"
	"github.com/panjf2000/ants/v2"
	"gorm.io/gorm"
	"sync"
	"time"
)

const (
	_defaultSort = "asc"
	_defaultPK   = "id"
)

// 基础参数
type NdParam struct {
	total     int             // 迁移总行数
	wait      *sync.WaitGroup // 等待协程结束
	err       chan error      // 迁移错误
	startTime time.Time       // 开始时间
	p         *ants.Pool      // 协程池
	dataChan  chan *sql.Rows  // 数据通道
}

// 自定义参数
type Param struct {
	PoolNumber     int      // 协程池数量
	SelectNumber   int64    // 批量查询量
	SourceConn     *gorm.DB // 源数据库连接
	Name           string   // 迁移名称
	SourceTable    string   // 迁移表名
	PrimaryKeyName string   // 主键名称
}

type dbId struct {
	Id int64 `gorm:"column:id" json:"id"`
}
