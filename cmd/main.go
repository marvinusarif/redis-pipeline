package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	redispipeline "github.com/redis-pipeline"
)

func main() {
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime)

	pool := &redigo.Pool{
		MaxActive:   1,
		MaxIdle:     1,
		IdleTimeout: 5 * time.Second,
		Wait:        true,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", ":32392", redigo.DialConnectTimeout(5*time.Second))
			if err != nil {
				log.Println(err)
				return nil, err
			}
			return c, err
		},
	}

	interval := 1000
	maxConn := 3
	maxCommandsBatch := uint64(1000)
	rb := redispipeline.NewRedisPipeline(interval, pool, maxConn, maxCommandsBatch)

	for i := 0; i < 300000; i++ {
		go func(i int) {
			// fmt.Println(i)
			rp := rb.NewSession()
			rp.PushCommand("INCR", fmt.Sprintf("testA")).
				PushCommand("INCR", fmt.Sprintf("testB")).
				PushCommand("INCR", fmt.Sprintf("testC")).
				Execute()

			/*
			   Need to cast the response and error accordingly in sequential order
			*/
			// for _, resp := range resps {
			// 	if resp.Err != nil {
			// 		log.Println(resp.Err)
			// 		continue
			// 	}
			// 	switch resp.Value.(type) {
			// 	case string:
			// 		log.Println(resp.Value)
			// 	case int64:
			// 		log.Println(resp.Value)
			// 	case float64:
			// 		log.Println(resp.Value)
			// 	}
			// 	//log.Println(resp.Value.(string))
			// }
		}(i)
	}

	// create term so the app didn't exit
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	select {
	case <-term:
		log.Println("terminate app")
	}
}
