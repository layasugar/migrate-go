package mig

// TaskInterface 任务数据处理接口
type TaskInterface interface {
	Claim()
}

// TaskBaseInterface 基本任务接口
type TaskBaseInterface interface {
	// Do 开始执行
	Do()

	before()
	mig()
	after()
}
