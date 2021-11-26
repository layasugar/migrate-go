// 单表读取数据

package mig

import (
	"fmt"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
	"log"
	"sync"
	"time"
)

type Task struct {
	*ndParam
	*Param
	Claim func([]map[string]interface{})
}

func (t *Task) Do() {
	t.before()
	t.mig()
	t.after()
}

func (t *Task) before() {
	var err error
	t.Param.check()

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

func (t *Task) after() {
	t.wait.Wait()
	t.p.Release()
	stop := time.Since(t.startTime)
	log.Printf("结束迁移: success, 迁移名称: %s, 总迁移行数：%d, 耗时：%v", t.Name, t.current.Load(), stop)
}

func (t *Task) mig() {
	var i int64
	var number = t.SelectNumber
	var table = t.Param.SourceTable
	defer close(t.dataChan)
	defer close(t.counter)

	if firstId := GetFirstId(t.SourceConn, table, t.PrimaryKeyName); firstId != 0 {
		i = firstId
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
	return
}
