// 按一定分表规则读取数据, 数字型分表

package mig

import (
	"fmt"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
	"log"
	"sync"
	"time"
)

type TaskFulls struct {
	*ndParam
	*Param
	SubNumber       int    // 分表数量
	TableNameFormat string // format
	Claim           func([]map[string]interface{})
}

func (t *TaskFulls) Do() {
	t.before()
	t.mig()
	t.after()
}

func (t *TaskFulls) before() {
	var err error
	t.Param.check()
	if t.SubNumber == 0 {
		panic("请设置分表数量")
	}
	if t.TableNameFormat == "" {
		panic("请设置分表规则")
	}

	t.ndParam = &ndParam{
		wait:      &sync.WaitGroup{},
		current:   atomic.NewInt64(0),
		counter:   make(chan *atomic.Int64),
		dataChan:  make(chan []map[string]interface{}, 100),
		startTime: time.Now(),
	}

	t.p, err = ants.NewPool(t.PoolNumber)
	if err != nil {
		panic(fmt.Sprintf("开启协程池 err: %s", err.Error()))
	}

	// 计数器
	t.wait.Add(1)
	go Counter(t.wait, t.counter, t.total)

	t.wait.Add(1)
	go func() {
		defer t.wait.Done()
		for data := range t.ndParam.dataChan {
			t.wait.Add(1)
			_ = t.p.Submit(func() {
				defer t.wait.Done()
				t.Claim(data)
			})
		}
	}()
}

func (t *TaskFulls) after() {
	t.wait.Wait()
	t.p.Release()
	stop := time.Since(t.startTime)
	log.Printf("结束迁移: success, 迁移名称: %s, 总迁移行数：%d, 耗时：%v", t.Name, t.current.Load(), stop)
}

func (t *TaskFulls) mig() {
	defer close(t.dataChan)
	defer close(t.counter)

	for tableFix := 0; tableFix < t.SubNumber; tableFix++ {
		var i int64
		var number = t.SelectNumber
		var table = fmt.Sprintf(t.TableNameFormat, tableFix)

		// 优先使用PrimaryKeyValue, 然后使用CreatedAtStart
		if t.PrimaryKeyValue != 0 {
			i = t.PrimaryKeyValue
		} else if t.CreatedAtStart != "" {
			if firstId := GetFirstIdByCreatedAt(t.SourceConn, table, t.PrimaryKeyName, t.CreatedAtStart); firstId != 0 {
				i = firstId
			}
		} else {
			if firstId := GetFirstId(t.SourceConn, table, t.PrimaryKeyName); firstId != 0 {
				i = firstId
			}
		}

		for {
			var data = make([]map[string]interface{}, 0, number)
			err := t.SourceConn.Table(table).Where("id >= ?", i).Where("id < ?", i+number).Find(&data).Error
			if nil != err {
				log.Printf("迁移出错, 迁移名称: %s, err：%s", t.Name, err.Error())
				return
			}

			if len(data) == 0 {
				if secondId := GetSecondId(t.SourceConn, table, t.PrimaryKeyName, i); secondId != 0 {
					i = secondId
					continue
				}
				break
			}
			i += number

			t.current.Add(int64(len(data)))
			t.counter <- t.current

			t.ndParam.dataChan <- data
		}
	}
	return
}
