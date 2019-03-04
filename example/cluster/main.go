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

	"github.com/gomodule/redigo/redis"
	"github.com/google/gops/agent"
	redispipeline "github.com/redis-pipeline"
	rediscli "github.com/redis-pipeline/client"
)

func main() {
	/*
		Warning :
			Redis Pipeline is useful for retrieving data or updating data in batch. This library is intended to execute as much commands as possible in single round trip time.
			if the pipeline session is timeout then all commands in the session won't get executed. If the timeout is not reached when all commands sent to redis server,
			it would wait until the response get returned from redis.
	*/
	//gops!
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal(err)
	}
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)

	redisHost := "127.0.0.1:30001;127.0.0.1:30002;127.0.0.1:30003;127.0.0.1:30004;127.0.0.1:30005;127.0.0.1:30006"
	maxConn := 100
	contextTimeout := 300
	maxIntervalInMs := uint64(10)
	maxCommandsBatch := uint64(100)

	client := rediscli.New(rediscli.CLUSTER_MODE, redisHost, maxConn,
		redis.DialReadTimeout(1*time.Second),
		redis.DialWriteTimeout(1*time.Second),
		redis.DialConnectTimeout(5*time.Second))
	rbc := redispipeline.NewRedisPipeline(client, maxIntervalInMs, maxCommandsBatch)

	var requestTimeout uint64
	requests := 3000
	redisJobPerRequest := 4
	fmt.Println("starting SET session")
	now := time.Now()
	wg := &sync.WaitGroup{}
	requestTimeout = uint64(0)
	for x := 1; x <= requests; x++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(contextTimeout)*time.Millisecond)
			defer cancel()
			var requestTimeoutError error
			for y := 1; y <= redisJobPerRequest; y++ {
				err := setKeyToRedis(ctx, rbc, fmt.Sprintf("%d%d", x, y))
				if err != nil {
					requestTimeoutError = err
				}
			}
			if requestTimeoutError != nil {
				atomic.AddUint64(&requestTimeout, 1)
			}
		}(x)
	}
	wg.Wait()
	fmt.Println("timeout requests on SET :", requestTimeout)
	fmt.Println(time.Since(now))
	fmt.Println("ending SET session")

	fmt.Println("starting GET session")
	requestTimeout = uint64(0)
	for x := 1; x <= requests; x++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(contextTimeout)*time.Millisecond)
			defer cancel()
			var requestTimeoutError error
			for y := 1; y <= redisJobPerRequest; y++ {
				err := getKeyFromRedis(ctx, rbc, fmt.Sprintf("%d%d", x, y))
				if err != nil {
					requestTimeoutError = err
				}
			}
			if requestTimeoutError != nil {
				atomic.AddUint64(&requestTimeout, 1)
			}
		}(x)
	}
	wg.Wait()
	fmt.Println("timeout requests on GET :", requestTimeout)
	fmt.Println(time.Since(now))
	fmt.Println("ending GET session")

	fmt.Println("starting GET FROM SLAVE session")
	requestTimeout = uint64(0)
	for x := 1; x <= requests; x++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(contextTimeout)*time.Millisecond)
			defer cancel()
			var requestTimeoutError error
			for y := 1; y <= redisJobPerRequest; y++ {
				err := getKeyFromRedisSlave(ctx, rbc, fmt.Sprintf("%d%d", x, y))
				if err != nil {
					requestTimeoutError = err
				}
			}
			if requestTimeoutError != nil {
				atomic.AddUint64(&requestTimeout, 1)
			}
		}(x)
	}
	wg.Wait()
	fmt.Println("timeout requests on GET FROM SLAVE :", requestTimeout)
	fmt.Println(time.Since(now))
	fmt.Println("ending GET from slave session")
	// create term so the app didn't exit
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-term:
		fmt.Println("terminate app")
	}
}

func getKeyFromRedis(ctx context.Context, rb redispipeline.RedisPipeline, i string) error {
	_, err := rb.NewSession(ctx).
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
	// for _, resp := range resps {
	// 	if resp.Err != nil {
	// 		fmt.Println(resp.Err)
	// 		continue
	// 	}
	// 	switch reply := resp.Value.(type) {
	// 	case string:
	// 		fmt.Println("cast to string")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case int64:
	// 		fmt.Println("cast to int")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case float64:
	// 		fmt.Println("cast to float")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case []byte:
	// 		fmt.Println("cast to byte")
	// 		fmt.Println("expect", i, "get", string(reply))
	// 	}
	// }
	return nil
}

func setKeyToRedis(ctx context.Context, rb redispipeline.RedisPipeline, i string) error {
	_, err := rb.NewSession(ctx).
		PushCommand("SET", fmt.Sprintf("testA%s", i), i).
		PushCommand("SET", fmt.Sprintf("testB%s", i), i).
		PushCommand("SET", fmt.Sprintf("testC%s", i), i).
		PushCommand("SET", fmt.Sprintf("testD%s", i), i).
		PushCommand("SET", fmt.Sprintf("testE%s", i), i).
		Execute()
	if err != nil {
		return err
	}
	// return nil
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
	// 		fmt.Println("cast to string")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case int64:
	// 		fmt.Println("cast to int")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case float64:
	// 		fmt.Println("cast to float")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case []byte:
	// 		fmt.Println("cast to byte")
	// 		fmt.Println("expect", i, "get", string(reply))
	// 	}
	// }
	return nil
}

func getKeyFromRedisSlave(ctx context.Context, rb redispipeline.RedisPipeline, i string) error {
	_, err := rb.NewSession(ctx).
		PushCommandReadOnly("GET", fmt.Sprintf("testA%s", i)).
		PushCommandReadOnly("GET", fmt.Sprintf("testB%s", i)).
		PushCommandReadOnly("GET", fmt.Sprintf("testC%s", i)).
		PushCommandReadOnly("GET", fmt.Sprintf("testD%s", i)).
		PushCommandReadOnly("GET", fmt.Sprintf("testE%s", i)).
		Execute()
	if err != nil {
		return err
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
	// 		fmt.Println("cast to string")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case int64:
	// 		fmt.Println("cast to int")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case float64:
	// 		fmt.Println("cast to float")
	// 		fmt.Println("expect", i, "get", reply)
	// 	case []byte:
	// 		fmt.Println("cast to byte")
	// 		fmt.Println("expect", i, "get", string(reply))
	// 	}
	// }
	return nil
}
