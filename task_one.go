package mig

import (
	"database/sql"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"log"
	"sync"
	"time"
)

type TaskOne struct {
	*NdParam
	*Param
	T TaskInterface // 数据处理接口
}

func (t *TaskOne) before() {
	var err error
	t.Param.check()
	t.NdParam = new(NdParam)

	t.startTime = time.Now()
	t.wait = &sync.WaitGroup{}
	t.p, err = ants.NewPool(t.PoolNumber)
	if err != nil {
		panic(fmt.Sprintf("开启协程池 err: %s", err.Error()))
	}

	// 初始化数据通道,(假设迁移总量是500万*1kb=5Gb)
	t.NdParam.dataChan = make(chan *sql.Rows, 5000000)

	go func() {
		for v := range t.NdParam.dataChan {
			t.wait.Add(1)
			t.err <- t.p.Submit(func() {
				defer t.wait.Done()
				t.T.TaskClaim(v)
			})
		}
	}()

	go func() {
		for v := range t.err {
			if v != nil {
				log.Printf("迁移出错, 迁移名称: %s, err：%s", t.Name, v.Error())
			}
		}
	}()
}

func (t *TaskOne) Do() {
	t.before()
	t.getSourceData(t.SourceTable)
	t.after()
}

func (t *TaskOne) after() {
	t.wait.Wait()
	t.p.Release()
	stop := time.Since(t.startTime)
	log.Printf("结束迁移, 迁移名称: %s, 总迁移行数：%d, 耗时：%v", t.Name, t.total, stop)
}

func (t *TaskOne) getSourceData(table string) {
	var i int64
	var number = t.SelectNumber
	var wait = t.wait

	if firstId := getFirstId(t.SourceConn, table); firstId != 0 {
		i = firstId
	}

	for {
		rows, err := t.SourceConn.Table(table).Where("id >= ?", i).Where("id < ?", i+number).Rows()
		if nil != err {
			t.err <- err
			return
		}

		var firstData []dbId
		for rows.Next() {
			err = t.SourceConn.Table(table).ScanRows(rows, &firstData)
			if err != nil {
				log.Printf(err.Error())
			}
		}

		if len(firstData) == 0 {
			if secondId := getSecondId(t.SourceConn, table, i); secondId != 0 {
				i = secondId
				continue
			}
			break
		}
		i += number
		t.NdParam.total += len(firstData)
		wait.Add(1)
		t.NdParam.dataChan <- rows
	}
	return
}
