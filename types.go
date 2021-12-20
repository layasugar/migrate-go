package mig

import (
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
	"gorm.io/gorm"
	"sync"
	"time"
)

const (
	_defaultSort = "asc"
	_defaultPK   = "id"
)

// ndParam 基础数据
type ndParam struct {
	p         *ants.Pool                    // 协程池
	wait      *sync.WaitGroup               // 等待协程结束
	total     int64                         // 未迁移总数
	current   *atomic.Int64                 // 当前已迁移数
	counter   chan *atomic.Int64            // 计数器
	errChan   chan error                    // 迁移错误
	dataChan  chan []map[string]interface{} // 数据通道
	startTime time.Time                     // 开始时间
}

// Param 自定义参数
type Param struct {
	PoolNumber      int      // 协程池数量
	SelectNumber    int64    // 批量查询量
	SourceConn      *gorm.DB // 源数据库连接
	Name            string   // 迁移名称
	SourceTable     string   // 迁移表名
	PrimaryKeyName  string   // 主键名称
	PrimaryKeyValue int64    // 可以设置一个主键的初始值
	CreatedAtStart  string   // 2021-12-20 11:15:00
}

type dbId struct {
	Id int64 `gorm:"column:id" json:"id"`
}
