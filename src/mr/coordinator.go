package mr

import (
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type taskStatus int

const (
	Pending taskStatus = iota
	Processing
	Completed
)

func (s taskStatus) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Processing:
		return "Processing"
	case Completed:
		return "Completed"
	default:
		return fmt.Sprintf("taskStatus(%d)", int(s))
	}
}

type task struct {
	id             int
	status         taskStatus
	expirationTime time.Time
	workerId       int
}

type mapTask struct {
	task
	inFilePath string
}

type reduceTask struct {
	task
}

type Coordinator struct {
	mapTasks    []*mapTask
	reduceTasks []*reduceTask

	expirationInterval time.Duration
	timer              *time.Timer

	mutex           sync.Mutex
	mapTaskReady    *sync.Cond // has Pending task or all tasks are Completed
	reduceTaskReady *sync.Cond // has Pending task or all tasks are Completed
}

func (c *Coordinator) SubmitAndAcquire(args *SubmitAndAcquireArgs, reply *SubmitAndAcquireReply) error {
	slog.Debug("rpc received", "worker", args.WorkerId, "submitType", args.TaskType, "submitId", args.TaskId)

	if -1 < args.TaskId {
		if Map == args.TaskType && len(c.mapTasks) <= args.TaskId {
			slog.Warn("submit ignored: map task id out of range",
				"worker", args.WorkerId, "id", args.TaskId, "nMap", len(c.mapTasks))
		} else if Reduce == args.TaskType && len(c.reduceTasks) <= args.TaskId {
			slog.Warn("submit ignored: reduce task id out of range",
				"worker", args.WorkerId, "id", args.TaskId, "nReduce", len(c.reduceTasks))
		} else {
			c.submitTask(args.WorkerId, args.TaskType, args.TaskId)
		}
	}

	m, r := c.acquireTask(args.WorkerId)

	switch {
	case nil != m:
		reply.TaskType = Map
		reply.TaskId = m.id
		reply.InFilePath = m.inFilePath
		reply.NReduce = len(c.reduceTasks)
	case nil != r:
		reply.TaskType = Reduce
		reply.TaskId = r.id
		reply.NMap = len(c.mapTasks)
	default:
		reply.Done = true
		slog.Info("no more tasks, worker may exit", "worker", args.WorkerId)
	}

	return nil
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, t := range c.mapTasks {
		if Completed != t.status {
			return false
		}
	}

	for _, t := range c.reduceTasks {
		if Completed != t.status {
			return false
		}
	}

	return true
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapTasks = make([]*mapTask, 0, len(files))
	c.reduceTasks = make([]*reduceTask, 0, nReduce)

	c.expirationInterval = 10 * time.Second
	c.mapTaskReady = sync.NewCond(&c.mutex)
	c.reduceTaskReady = sync.NewCond(&c.mutex)

	for i, file := range files {
		c.mapTasks = append(c.mapTasks, &mapTask{
			task: task{
				id:     i,
				status: Pending,
			},
			inFilePath: file,
		})
	}

	for i := range nReduce {
		c.reduceTasks = append(c.reduceTasks, &reduceTask{
			task: task{
				id:     i,
				status: Pending,
			},
		})
	}

	c.server(sockname)
	slog.Info("coordinator started",
		"sock", sockname, "nMap", len(c.mapTasks), "nReduce", len(c.reduceTasks), "timeout", c.expirationInterval)
	return &c
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	e := rpc.Register(c)
	if nil != e {
		log.Fatalf("register error: %v", e)
	}

	rpc.HandleHTTP()

	_ = os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if nil != e {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go func() {
		e := http.Serve(l, nil)
		if nil != e {
			log.Fatalf("serve error %s: %v", sockname, e)
		}
	}()
}

func (c *Coordinator) acquireTask(workerId int) (*mapTask, *reduceTask) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for {
		allCompleted := true

		for _, t := range c.mapTasks {
			if Pending == t.status {
				t.status = Processing
				t.expirationTime = time.Now().Add(c.expirationInterval)
				t.workerId = workerId
				c.setTimer()
				slog.Info("task assigned", "worker", workerId, "type", Map, "id", t.id, "file", t.inFilePath)
				return t, nil
			}

			if Completed != t.status {
				allCompleted = false
			}
		}
		if allCompleted {
			break
		} else {
			slog.Debug("no pending map task, worker waits", "worker", workerId)
			c.mapTaskReady.Wait()
		}
	}

	for {
		allCompleted := true

		for _, t := range c.reduceTasks {
			if Pending == t.status {
				t.status = Processing
				t.expirationTime = time.Now().Add(c.expirationInterval)
				t.workerId = workerId
				c.setTimer()
				slog.Info("task assigned",
					"worker", workerId, "type", Reduce, "id", t.id)
				return nil, t
			}

			if Completed != t.status {
				allCompleted = false
			}
		}
		if allCompleted {
			break
		} else {
			slog.Debug("no pending reduce task, worker waits", "worker", workerId)
			c.reduceTaskReady.Wait()
		}
	}

	return nil, nil
}

func (c *Coordinator) submitTask(workerId int, taskType TaskType, taskId int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var t *task
	var ready *sync.Cond
	switch taskType {
	case Map:
		t, ready = &c.mapTasks[taskId].task, c.mapTaskReady
	case Reduce:
		t, ready = &c.reduceTasks[taskId].task, c.reduceTaskReady
	default:
		return
	}

	if Completed == t.status {
		slog.Warn("duplicate submit ignored",
			"worker", workerId, "type", taskType, "id", t.id, "assignee", t.workerId)
		return
	}

	if t.workerId != workerId {
		slog.Warn("late submit accepted: task was reassigned",
			"worker", workerId, "type", taskType, "id", t.id, "assignee", t.workerId, "status", t.status)
	}

	t.status = Completed
	ready.Broadcast()

	slog.Info("task completed", "worker", workerId, "type", taskType, "id", t.id)
}

func (c *Coordinator) setTimer() {
	if nil != c.timer {
		return
	}

	c.timer = time.AfterFunc(c.expirationInterval, func() {
		now := time.Now()
		var next time.Time

		c.mutex.Lock()
		defer c.mutex.Unlock()

		for _, t := range c.mapTasks {
			if Processing == t.status {
				if !t.expirationTime.After(now) {
					slog.Warn("task timed out, back to pending",
						"type", Map, "id", t.id, "assignee", t.workerId, "timeout", c.expirationInterval)
					t.status = Pending
					t.expirationTime = time.Time{}
					c.mapTaskReady.Broadcast()
				} else if next.IsZero() || t.expirationTime.Before(next) {
					next = t.expirationTime
				}
			}
		}

		for _, t := range c.reduceTasks {
			if Processing == t.status {
				if !t.expirationTime.After(now) {
					slog.Warn("task timed out, back to pending",
						"type", Reduce, "id", t.id, "assignee", t.workerId, "timeout", c.expirationInterval)
					t.status = Pending
					t.expirationTime = time.Time{}
					c.reduceTaskReady.Broadcast()
				} else if next.IsZero() || t.expirationTime.Before(next) {
					next = t.expirationTime
				}
			}
		}

		if next.IsZero() {
			c.timer.Stop()
			c.timer = nil
		} else {
			c.timer.Reset(next.Sub(now))
		}
	})
}
