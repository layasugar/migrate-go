package mig

import (
	"runtime"
	"testing"
)

func TestTaskRelations_Do(t *testing.T) {
	var task = TaskFulls{
		Param: &Param{
			SelectNumber: 20,
			PoolNumber:   runtime.NumCPU(),
			SourceConn:   getDbClient(),
			SourceTable:  "pay_order",
			Name:         "迁移用户",
		},
		SubNumber:       32,
		TableNameFormat: "pay_order_%d",
		Claim:           Claim,
	}
	task.Do()
}
