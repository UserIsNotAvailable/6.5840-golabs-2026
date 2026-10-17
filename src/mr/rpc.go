package mr

import "fmt"

//
// RPC definitions.
//

type TaskType byte

const (
	None TaskType = iota
	Map
	Reduce
)

func (t TaskType) String() string {
	switch t {
	case None:
		return "None"
	case Map:
		return "Map"
	case Reduce:
		return "Reduce"
	default:
		return fmt.Sprintf("TaskType(%d)", byte(t))
	}
}

type SubmitAndAcquireArgs struct {
	WorkerId int
	TaskId   int
	TaskType TaskType
}

type SubmitAndAcquireReply struct {
	TaskId   int
	TaskType TaskType
	Done     bool

	InFilePath string // for map
	NReduce    int    // for map

	NMap int // for reduce
}
