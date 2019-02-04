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
	redispipeline "github.com/redis-pipeline"
	redis "github.com/redis-pipeline/client"
)

func main() {
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal(err)
	}
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)

	// redisHost := "127.0.0.1:30001;127.0.0.1:30002;127.0.0.1:30003;127.0.0.1:30004;127.0.0.1:30005;127.0.0.1:30006"
	redisHost := "127.0.0.1:30001;127.0.0.1:30002;127.0.0.1:30003"
	maxConn := 5
	maxCommandsBatch := uint64(100)

	client := redis.New(redis.CLUSTER_MODE, redisHost, maxConn)
	rbc := redispipeline.NewRedisPipeline(client, maxCommandsBatch)

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
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1000)*time.Millisecond)
			defer cancel()
			var requestTimeoutError error
			for y := 1; y <= redisJobPerRequest; y++ {
				err := setKeyToRedis(rbc, fmt.Sprintf("%d%d", x, y), ctx)
				if err != nil {
					fmt.Println(err)
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

	fmt.Println("starting get session")
	requestTimeout = uint64(0)
	for x := 1; x <= requests; x++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1000)*time.Millisecond)
			defer cancel()
			var requestTimeoutError error
			for y := 1; y <= redisJobPerRequest; y++ {
				err := getKeyFromRedis(rbc, fmt.Sprintf("%d%d", x, y), ctx)
				if err != nil {
					fmt.Println(err)
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
	fmt.Println("ending get session")

	// create term so the app didn't exit
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-term:
		fmt.Println("terminate app")
	}
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
	// return nil
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
