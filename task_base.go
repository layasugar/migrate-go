package mig

// 任务数据处理接口
type TaskInterface interface {
	Claim()
}

// 基本任务接口
type TaskBaseInterface interface {
	before()
	Do()
	after()
	getSourceData(table string)
	Scan()
}
