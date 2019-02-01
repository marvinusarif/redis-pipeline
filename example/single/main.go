package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/gops/agent"
	redis "github.com/redis-pipeline/adapter"
	redispipeline "github.com/redis-pipeline/src"
)

func main() {
	/*
		Warning :
			Redis Pipeline is useful for retrieving data or updating data in batch. This library is intended to execute as much commands as possible in single round trip time.
			if the pipeline session is timedout then all commands in the session won't get executed. If the timeout is not reached when all commands sent to redis server,
			it would wait until the response get returned from redis.
	*/
	//gops!
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal(err)
	}
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)

	redisHost := "localhost:30008"
	maxConn := 200
	maxCommandsBatch := uint64(100)

	client, err := redis.New(redis.SINGLE_MODE, redisHost, maxConn)
	if err != nil {
		log.Println(err)
	}
	rb := redispipeline.NewRedisPipeline(client, maxCommandsBatch)
	/*
		simulates concurrent rps
	*/
	var requestTimeout uint64
	requests := 20000
	redisJobPerRequest := 4
	fmt.Println("starting MULTI/EXEC session")
	now := time.Now()
	wg := &sync.WaitGroup{}
	requestTimeout = uint64(0)
	for x := 1; x <= requests; x++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1000)*time.Millisecond)
			defer cancel()
			var requestTimeoutError error
			for y := 1; y <= redisJobPerRequest; y++ {
				err := setKeyToRedis(rb, fmt.Sprintf("%d%d", x, y), ctx)
				if err != nil {
					log.Println(err)
					requestTimeoutError = err
				}
			}
			if requestTimeoutError != nil {
				atomic.AddUint64(&requestTimeout, 1)
			}
		}(x)
	}
	wg.Wait()
	fmt.Println(time.Since(now))
	fmt.Println("ending MULTI/EXEC session")

	fmt.Println("starting SET session")
	now = time.Now()
	wg = &sync.WaitGroup{}
	requestTimeout = uint64(0)
	for x := 1; x <= requests; x++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1000)*time.Millisecond)
			defer cancel()
			var requestTimeoutError error
			for y := 1; y <= redisJobPerRequest; y++ {
				err := setKeyToRedis(rb, fmt.Sprintf("%d%d", x, y), ctx)
				if err != nil {
					log.Println(err)
					requestTimeoutError = err
				}
			}
			if requestTimeoutError != nil {
				atomic.AddUint64(&requestTimeout, 1)
			}
		}(x)
	}
	wg.Wait()
	fmt.Println("timeout requests :", requestTimeout)
	fmt.Println(time.Since(now))
	fmt.Println("ending SET session")

	fmt.Println("starting GET session")
	now = time.Now()
	wg = &sync.WaitGroup{}
	requestTimeout = uint64(0)
	for x := 1; x <= requests; x++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1000)*time.Millisecond)
			defer cancel()
			var requestTimeoutError error
			for y := 1; y <= redisJobPerRequest; y++ {
				err := getKeyFromRedis(rb, fmt.Sprintf("%d%d", x, y), ctx)
				if err != nil {
					log.Println(err)
					requestTimeoutError = err
				}
			}
			if requestTimeoutError != nil {
				atomic.AddUint64(&requestTimeout, 1)
			}
		}(x)
	}
	wg.Wait()
	fmt.Println("timeout requests :", requestTimeout)
	fmt.Println(time.Since(now))
	fmt.Println("ending GET session")

	// create term so the app didn't exit
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-term:
		fmt.Println("terminate app")
	}
}

func multiExecRedis(rb redispipeline.RedisPipeline, i string, ctx context.Context) error {
	_, err := rb.NewSession(ctx).
		PushCommand("MULTI").             //response : {OK, <nil>}
		PushCommand("SET", "testA%s", i). //response : {QUEUED, <nil>}
		PushCommand("SET", "testB%s", i). // response : {QUEUED, <nil>}
		PushCommand("SET", "testC%s", i). // response : {QUEUED, <nil>}
		PushCommand("SET", "testD%s", i). // response : {QUEUED, <nil>}
		PushCommand("SET", "testE%s", i). // response : {QUEUED, <nil>}
		PushCommand("INCR", "test").
		PushCommand("EXEC"). //response : {[1,1,1,1,1], <nil>}
		Execute()
	if err != nil {
		return err
	}
	return nil
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

func getKeyFromRedis(rb redispipeline.RedisPipeline, i string, ctx context.Context) error {
	resps, err := rb.NewSession(ctx).
		PushCommand("GET", fmt.Sprintf("testA%s", i)).
		PushCommand("GET", fmt.Sprintf("testB%s", i)).
		PushCommand("GET", fmt.Sprintf("testC%s", i)).
		PushCommand("GET", fmt.Sprintf("testD%s", i)).
		PushCommand("GET", fmt.Sprintf("testE%s", i)).
		Execute()
	if err != nil {
		return err
	}
	/*
	 Need to cast the response and error accordingly in sequential order
	*/
	for _, resp := range resps {
		if resp.Err != nil {
			fmt.Println(resp.Err)
			continue
		}
		switch reply := resp.Value.(type) {
		case string:
			fmt.Println("cast to string")
			fmt.Println("expect", i, "get", reply)
		case int64:
			fmt.Println("cast to int")
			fmt.Println("expect", i, "get", reply)
		case float64:
			fmt.Println("cast to float")
			fmt.Println("expect", i, "get", reply)
		case []byte:
			fmt.Println("cast to byte")
			fmt.Println("expect", i, "get", string(reply))
		}
	}
	return nil
}

func setKeyToRedis(rb redispipeline.RedisPipeline, i string, ctx context.Context) error {
	resps, err := rb.NewSession(ctx).
		PushCommand("SET", fmt.Sprintf("testA%s", i), i).
		PushCommand("SET", fmt.Sprintf("testB%s", i), i).
		PushCommand("SET", fmt.Sprintf("testC%s", i), i).
		PushCommand("SET", fmt.Sprintf("testD%s", i), i).
		PushCommand("SET", fmt.Sprintf("testE%s", i), i).
		Execute()
	if err != nil {
		return err
	}
	/*
	 Need to cast the response and error accordingly in sequential order
	*/
	for _, resp := range resps {
		if resp.Err != nil {
			fmt.Println(resp.Err)
			continue
		}
		switch reply := resp.Value.(type) {
		case string:
			fmt.Println("cast to string")
			fmt.Println("expect", i, "get", reply)
		case int64:
			fmt.Println("cast to int")
			fmt.Println("expect", i, "get", reply)
		case float64:
			fmt.Println("cast to float")
			fmt.Println("expect", i, "get", reply)
		case []byte:
			fmt.Println("cast to byte")
			fmt.Println("expect", i, "get", string(reply))
		}
	}
	return nil
}
