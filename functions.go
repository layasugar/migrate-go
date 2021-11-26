package mig

import (
	"fmt"
	"go.uber.org/atomic"
	"gorm.io/gorm"
	"runtime"
	"strconv"
	"sync"
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

func GetFirstId(db *gorm.DB, table string, pk string) int64 {
	var firstData dbId
	var orderBy = fmt.Sprintf("%s.%s %s", table, pk, _defaultSort)

	err := db.Table(table).Order(orderBy).Limit(1).Find(&firstData).Error
	if err != nil {
		fmt.Printf("单条查询出错: err=%v", err.Error())
		return 0
	}
	return firstData.Id
}

func GetSecondId(db *gorm.DB, table string, pk string, i int64) int64 {
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

func Bar(current, count int64, l ...int64) string {
	var size int64
	if len(l) > 0 {
		if l[0] == 0 {
			size = 100
		} else {
			size = l[0]
		}
	} else {
		size = 100
	}

	if current == 0 {
		str := ""
		for i := int64(0); i < size; i++ {
			str += " "
		}
		return "[" + str + "] 0%"
	}

	if current >= count || count == 0 {
		str := ""
		for i := int64(0); i < size; i++ {
			str += "="
		}
		return "[" + str + "] 100%"
	}

	percent := int64((float64(current) / float64(count)) * 100)
	currentEqual := int64((float64(current) / float64(count)) * float64(size))
	str := ""
	for i := int64(0); i < size; i++ {
		if i < currentEqual {
			str += "="
		} else {
			str += " "
		}
	}
	return "[" + str + "] " + strconv.Itoa(int(percent)) + "%"
}

func Counter(w *sync.WaitGroup, c chan *atomic.Int64, count int64) {
	defer w.Done()
	for number := range c {
		current := number.Load()
		str := Bar(number.Load(), count, 50)
		fmt.Printf("\r%s %d条", str, current)
	}
}
