package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/workers"
	"github.com/go-redis/redis/v8"
	"github.com/pborman/getopt/v2"
)

var ctx = context.Background()

func main() {
	var c types.Config
	cfg := getopt.StringLong("config-file", 'c', "", "The path to a config file")
	helpFlag := getopt.BoolLong("help", 'h', "display help")
	getopt.Parse()
	if *helpFlag || *cfg == "" {
		getopt.PrintUsage(os.Stderr)
		os.Exit(1)
	}

	c.GetConf(*cfg)

	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", c.Host, c.Port),
		Password:     c.Password,
		MinIdleConns: len(c.Microservices),
		MaxConnAge:   0,
		MaxRetries:   10,
	})

	// confirm we can connect to redis before starting
	err := client.Ping(ctx).Err()
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}

	for i, ms := range c.Microservices {
		wg.Add(1)
		if i == 0 {
			go workers.InitialWorker(ms, client, ctx)
		} else if i == len(c.Microservices)-1 {
			go workers.FinalWorker(ms, client, ctx)
		} else {
			go workers.StandardWorker(ms, client, ctx)
			go workers.StandardSaver(ms, client, ctx)
		}
	}

	wg.Wait()
}
