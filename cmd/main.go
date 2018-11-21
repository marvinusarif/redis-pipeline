package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	"github.com/google/gops/agent"
	redispipeline "github.com/redis-pipeline"
)

func main() {
	/*
		Warning :
			Redis Pipeline is useful for retrieving data or updating data in batch. This library is intended to execute as much commands as possible in single round trip time.
			There is no guarantee regarding the data sent to the pipeline get executed succesfully.

			Although you can execute MULTI/EXEC command using this Library it's not suggested to use this library if you need high level of data integrity.
			If you want please use Redis Worker Pool Library!
				Or
			You might use this library so it would wait until the response get returned from redis.

			*Note : Implementing context WithTimeout, there might be a race condition between returned response and timeouts.
			your context reaches its timeout and giving you false response but the commands get already succesfully sent and executed!
			**longer timeout might solve your problem
	*/

	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal(err)
	}
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)

	maxInterval := 200
	maxConn := 50
	maxCommandsBatch := uint64(100000)
	pool := &redigo.Pool{
		MaxActive:   maxConn,
		MaxIdle:     maxConn,
		IdleTimeout: 5 * time.Second,
		Wait:        true,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", ":32392", redigo.DialConnectTimeout(5*time.Second))
			if err != nil {
				fmt.Println(err)
				return nil, err
			}
			return c, err
		},
	}
	rb := redispipeline.NewRedisPipeline(pool, maxConn, maxInterval, maxCommandsBatch)

	fmt.Println("starting multi/exec session")
	now := time.Now()
	wg := &sync.WaitGroup{}
	for i := 1; i <= 300000; i++ {
		go func(rb redispipeline.RedisPipeline, i int) {
			wg.Add(1)
			defer wg.Done()
			multiExecRedis(rb, i)
		}(rb, i)
	}
	wg.Wait()
	fmt.Println(time.Since(now))
	fmt.Println("ending multi/exec session")

	fmt.Println("starting set session")
	now = time.Now()
	for i := 1; i <= 300000; i++ {
		go func(rb redispipeline.RedisPipeline, i int) {
			wg.Add(1)
			defer wg.Done()
			setKeyToRedis(rb, i)
		}(rb, i)
	}
	wg.Wait()
	fmt.Println(time.Since(now))
	fmt.Println("ending set session")

	fmt.Println("starting get session")
	now = time.Now()
	for i := 1; i <= 300000; i++ {
		go func(rb redispipeline.RedisPipeline, i int) {
			wg.Add(1)
			defer wg.Done()
			getKeyFromRedis(rb, i)
		}(rb, i)
	}
	wg.Wait()
	fmt.Println(time.Since(now))
	fmt.Println("ending get session")

	// create term so the app didn't exit
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-term:
		fmt.Println("terminate app")
	}
}

func multiExecRedis(rb redispipeline.RedisPipeline, i int) {
	_, err := rb.NewSession(context.Background()).
		PushCommand("MULTI").             //response : {OK, <nil>}
		PushCommand("SET", "testA%d", i). //response : {QUEUED, <nil>}
		PushCommand("SET", "testB%d", i). // response : {QUEUED, <nil>}
		PushCommand("SET", "testC%d", i). // response : {QUEUED, <nil>}
		PushCommand("SET", "testD%d", i). // response : {QUEUED, <nil>}
		PushCommand("SET", "testE%d", i). // response : {QUEUED, <nil>}
		PushCommand("INCR", "test").
		PushCommand("EXEC"). //response : {[1,1,1,1,1], <nil>}
		Execute()
	if err != nil {
		fmt.Println(err)
	}
	// `for _, resp := range resps {
	// 	if resp.Err != nil {
	// 		fmt.Println(resp.Err)
	// 		continue
	// 	}
	// 	switch reply := resp.Value.(type) {
	// 	case string:
	// 		fmt.Println("multi/exec", reply)
	// 	case int64:
	// 		fmt.Println("multi/exec", reply)
	// 	case float64:
	// 		fmt.Println("multi/exec", reply)
	// 	case []byte:
	// 		fmt.Println("multi/exec", string(reply))
	// 	case []interface{}:
	// 		s := reflect.ValueOf(resp.Value)
	// 		if s.Kind() != reflect.Slice {
	// 			panic("InterfaceSlice() given a non-slice type")
	// 		}
	// 		ret := make([]interface{}, s.Len())
	// 		for i := 0; i < s.Len(); i++ {
	// 			ret[i] = s.Index(i).Interface()
	// 			//you can cast from ret[i] to specific golang type
	// 		}
	// 		fmt.Println("multi/exec", ret)
	// 	}
	// }`
}

func getKeyFromRedis(rb redispipeline.RedisPipeline, i int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i*250)*time.Millisecond)
	defer cancel()
	_, err := rb.NewSession(ctx).
		PushCommand("GET", fmt.Sprintf("testA%d", i)).
		PushCommand("GET", fmt.Sprintf("testB%d", i)).
		PushCommand("GET", fmt.Sprintf("testC%d", i)).
		PushCommand("GET", fmt.Sprintf("testD%d", i)).
		PushCommand("GET", fmt.Sprintf("testE%d", i)).
		PushCommand("GET", "test").
		Execute()
	if err != nil {
		fmt.Println("get key", i, ":", err)
	}
	/*
	 Need to cast the response and error accordingly in sequential order
	*/
	// for _, resp := range resps {
	// 	if resp.Err != nil {
	// 		fmt.Println(resp.Err)
	// 		continue
	// 	}
	// 	switch reply := resp.Value.(type) {
	// 	case string:
	// 		fmt.Println("get", reply)
	// 	case int64:
	// 		fmt.Println("get", reply)
	// 	case float64:
	// 		fmt.Println("get", reply)
	// 	case []byte:
	// 		fmt.Println("get", string(reply))
	// 	}
	// }
}

func setKeyToRedis(rb redispipeline.RedisPipeline, i int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i*500)*time.Millisecond)
	defer cancel()
	_, err := rb.NewSession(ctx).
		PushCommand("SET", fmt.Sprintf("testA%d", i), i).
		PushCommand("SET", fmt.Sprintf("testB%d", i), i).
		PushCommand("SET", fmt.Sprintf("testC%d", i), i).
		PushCommand("SET", fmt.Sprintf("testD%d", i), i).
		PushCommand("SET", fmt.Sprintf("testE%d", i), i).
		PushCommand("INCR", "test").
		Execute()
	if err != nil {
		fmt.Println("set key", i, ":", err)
	}
	/*
	 Need to cast the response and error accordingly in sequential order
	*/
	// for _, resp := range resps {
	// 	if resp.Err != nil {
	// 		fmt.Println(resp.Err)
	// 		continue
	// 	}
	// 	switch reply := resp.Value.(type) {
	// 	case string:
	// 		fmt.Println("set", reply)
	// 	case int64:
	// 		fmt.Println("set", reply)
	// 	case float64:
	// 		fmt.Println("set", reply)
	// 	case []byte:
	// 		fmt.Println("set", string(reply))
	// 	}
	// }
}
