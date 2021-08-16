package mig

import (
	"fmt"
	"gorm.io/gorm"
	"runtime"
)

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
	// 需要迁移的表名
	if cp.SourceTable == "" {
		panic("迁移表名未配置")
	}
	// 任务名称
	if cp.Name == "" {
		cp.Name = "default task Name"
	}
	// 迁移表主键
	if cp.PrimaryKeyName == "" {
		cp.PrimaryKeyName = _defaultPK
	}
}

func getFirstId(db *gorm.DB, table string, pk string) int64 {
	var firstData dbId
	var orderBy = fmt.Sprintf("%s.%s %s", table, pk, _defaultSort)

	err := db.Table(table).Order(orderBy).Limit(1).Find(&firstData).Error
	if err != nil {
		fmt.Printf("单条查询出错: err=%v", err.Error())
		return 0
	}
	return firstData.Id
}

func getSecondId(db *gorm.DB, table string, pk string, i int64) int64 {
	var thirdData dbId
	var orderBy = fmt.Sprintf("%s.%s %s", table, pk, _defaultSort)
	var where = fmt.Sprintf("%s.%s >= %d", table, pk, i)

	err := db.Table(table).Where(where).Order(orderBy).Limit(1).Find(&thirdData).Error
	if err != nil {
		fmt.Printf("单条查询出错: err=%v", err.Error())
		return 0
	}
	return thirdData.Id
}
