package mr

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sort"
	"time"
)
import "net/rpc"
import "hash/fnv"
import "os"

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

const mapOutFileNamePattern = "mr-%d-%d"
const reduceOutFileNamePattern = "mr-out-%d"
const tempFileNamePattern = "mr-tmp-*"

var coordSockName string // socket for coordinator
var errCoordinatorGone = errors.New("coordinator unreachable")

// Worker
// main/mrworker.go calls this function.
func Worker(sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	pid := os.Getpid()
	wlog := slog.With("worker", pid)
	wlog.Info("worker started", "sock", sockname)

	args := SubmitAndAcquireArgs{WorkerId: pid, TaskId: -1, TaskType: None}
	reply := SubmitAndAcquireReply{}
	for {
		wlog.Debug("requesting task", "submitType", args.TaskType, "submitId", args.TaskId)
		ok, e := call("Coordinator.SubmitAndAcquire", &args, &reply)
		if errors.Is(e, errCoordinatorGone) {
			wlog.Warn("coordinator gone, exiting", "err", e)
			return
		}
		if !ok {
			log.Fatalf("%d: call failed error %v", pid, e)
		}

		if reply.Done {
			wlog.Info("no more tasks, exiting")
			break
		}

		startTime := time.Now()
		if Map == reply.TaskType {
			wlog.Info("task received", "type", Map, "id", reply.TaskId, "file", reply.InFilePath)
			content, e := os.ReadFile(reply.InFilePath)
			if nil != e {
				log.Fatalf("map: cannot read file %v, error: %v", reply.InFilePath, e)
			}

			kva := mapf(reply.InFilePath, string(content))
			wlog.Debug("map function done", "id", reply.TaskId, "kvs", len(kva))
			partitions := make([][]KeyValue, reply.NReduce)
			for _, kv := range kva {
				y := ihash(kv.Key) % reply.NReduce
				partitions[y] = append(partitions[y], kv)
			}

			for reduceNo := range reply.NReduce {
				mapOutFileName := fmt.Sprintf(mapOutFileNamePattern, reply.TaskId, reduceNo)

				e := writeFileAtomic(mapOutFileName, func(w io.Writer) error {
					enc := json.NewEncoder(w)
					for _, kv := range partitions[reduceNo] {
						e := enc.Encode(&kv)
						if nil != e {
							return e
						}
					}
					return nil
				})
				if nil != e {
					log.Fatalf("map: write file error: %v", e)
				}
			}
		} else if Reduce == reply.TaskType {
			wlog.Info("task received", "type", Reduce, "id", reply.TaskId, "nMap", reply.NMap)
			grouped := make(map[string][]string)
			for mapNo := range reply.NMap {
				mapOutFileName := fmt.Sprintf(mapOutFileNamePattern, mapNo, reply.TaskId)
				mapOutFile, e := os.Open(mapOutFileName)
				if nil != e {
					log.Fatalf("reduce: cannot open %v, error: %v", mapOutFileName, e)
				}

				dec := json.NewDecoder(mapOutFile)
				for {
					var kv KeyValue
					e := dec.Decode(&kv)
					if io.EOF == e {
						break
					}
					if nil != e {
						log.Fatalf("reduce: decode error: %v", e)
					}
					grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
				}

				e = mapOutFile.Close()
				if nil != e {
					log.Fatalf("reduce: cannot close %v, error: %v", mapOutFileName, e)
				}
			}

			keys := make([]string, 0, len(grouped))
			for k := range grouped {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			wlog.Debug("reduce input loaded", "id", reply.TaskId, "keys", len(keys))

			reduceOutFileName := fmt.Sprintf(reduceOutFileNamePattern, reply.TaskId)
			e := writeFileAtomic(reduceOutFileName, func(w io.Writer) error {
				for _, k := range keys {
					output := reducef(k, grouped[k])
					_, e := fmt.Fprintf(w, "%v %v\n", k, output)
					if nil != e {
						return e
					}
				}
				return nil
			})
			if nil != e {
				log.Fatalf("reduce: write file error: %v", e)
			}
		} else {
			log.Fatalf("expect %v or %v but got %v", Map, Reduce, reply.TaskType)
		}

		wlog.Info("task finished",
			"type", reply.TaskType, "id", reply.TaskId, "elapsed", time.Since(startTime).Round(time.Millisecond))
		args = SubmitAndAcquireArgs{
			WorkerId: pid,
			TaskId:   reply.TaskId,
			TaskType: reply.TaskType,
		}

		reply = SubmitAndAcquireReply{}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) (ok bool, e error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, e := rpc.DialHTTP("unix", coordSockName)
	if nil != e {
		return false, fmt.Errorf("%w: dialing: %v", errCoordinatorGone, e)
	}

	defer func(c *rpc.Client) {
		clsErr := c.Close()
		if nil != clsErr {
			ok = false
			e = errors.Join(e, fmt.Errorf("close: %w", clsErr))
		}
	}(c)

	e = c.Call(rpcname, args, reply)

	if nil != e {
		if errors.Is(e, rpc.ErrShutdown) || errors.Is(e, io.ErrUnexpectedEOF) {
			return false, fmt.Errorf("%w: %w", errCoordinatorGone, e)
		}
		return false, e
	}

	return true, nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, e := h.Write([]byte(key))
	if nil != e {
		log.Fatal(e)
	}
	return int(h.Sum32() & 0x7fffffff)
}

func writeFileAtomic(fileName string, write func(w io.Writer) error) error {
	tempFile, e := os.CreateTemp(".", tempFileNamePattern)
	if nil != e {
		return fmt.Errorf("create temp file failed: %v %w", fileName, e)
	}

	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}()

	bw := bufio.NewWriter(tempFile)

	e = write(bw)
	if nil != e {
		return fmt.Errorf("write file failed: %v %w", tempFile.Name(), e)
	}

	e = bw.Flush()
	if nil != e {
		return fmt.Errorf("flush failed %v, error: %w", tempFile.Name(), e)
	}

	e = tempFile.Close()
	if nil != e {
		return fmt.Errorf("close failed %v: %w", tempFile.Name(), e)
	}

	e = os.Rename(tempFile.Name(), fileName)
	if nil != e {
		return fmt.Errorf("rename [%v] to [%v] failed, error: %w", tempFile.Name(), fileName, e)
	}

	return nil
}
