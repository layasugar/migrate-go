package mig

import (
	"database/sql"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"gorm.io/gorm"
	"runtime"
	"sync"
	"time"
)

const (
	orderByAsc = "asc"
)

// 任务数据处理接口
type TaskInterface interface {
	TaskClaim(rows *sql.Rows)
}

// 基本任务接口
type TaskBaseInterface interface {
	before()
	Do()
	after()
	getSourceData(table string)
}

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
	PoolNumber   int      // 协程池数量
	SelectNumber int64    // 批量查询量
	SourceConn   *gorm.DB // 源数据库连接
	TargetConn   *gorm.DB // 目标数据库连接
	SourceTable  string   // 迁移表名
	Name         string   // 迁移名称
}

type dbId struct {
	Id int64 `gorm:"column:id" json:"id"`
}

func (cp *Param) check() {
	// 协程池数量,默认cpu数量
	if cp.PoolNumber == 0 {
		cp.PoolNumber = runtime.NumCPU()
	}
	// 批量查询数量
	if cp.SelectNumber == 0 {
		cp.SelectNumber = 2000
	}
	// 源数据库连通性
	if cp.SourceConn == nil {
		panic("源库未连接")
	}
	// 目标数据库连通性
	if cp.TargetConn == nil {
		panic("目标库未连接")
	}
	if cp.SourceTable == "" {
		panic("迁移表名未配置")
	}
	if cp.Name == "" {
		cp.Name = "default task Name"
	}
}

func getFirstId(db *gorm.DB, table string) int64 {
	var firstData dbId
	err := db.Table(table).Order(orderBy).Limit(1).Find(&firstData).Error
	if err != nil {
		fmt.Printf("单条查询出错: err=%v", err.Error())
		return 0
	}
	return firstData.Id
}

func getSecondId(db *gorm.DB, table string, i int64) int64 {
	// 处理中间断断续续的id
	var thirdData dbId
	err := db.Table(table).Where("id >= ?", i).Order(orderBy).Limit(1).Find(&thirdData).Error
	if err != nil {
		fmt.Printf("单条查询出错: err=%v", err.Error())
		return 0
	}
	return thirdData.Id
}
