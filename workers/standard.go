package workers

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
	"github.com/go-redis/redis/v8"
)

func StandardWorker(ms types.Microservice, redisClient *redis.Client, ctx context.Context) {
	if ms.BlockMS == 0 {
		ms.BlockMS = 10
	}
	if ms.BatchSize == 0 {
		ms.BatchSize = 1
	}
	log.Printf("Starting worker: %+v", ms)
	redisClient.XGroupCreateMkStream(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), "0").Err()
	for {
		res, _ := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    fmt.Sprintf("Group-%s", ms.Input),
			Consumer: fmt.Sprintf("Consumer-%s", ms.Input),
			Streams:  []string{ms.Input, ">"},
			Count:    int64(ms.BatchSize),
			Block:    time.Duration(ms.BlockMS) * time.Millisecond,
		}).Result()

		for _, x := range res {
			for _, y := range x.Messages {
				kvs := map[string]interface{}{
					fmt.Sprintf("%s-ts", ms.Input): time.Now().UnixNano() / int64(time.Millisecond),
				}
				for k, v := range y.Values {
					kvs[k] = v
				}
				if ms.ProcMax-ms.ProcMin > 0 {
					d := rand.Intn(ms.ProcMax-ms.ProcMin) + ms.ProcMin
					time.Sleep(time.Duration(d) * time.Millisecond)
				}
				redisClient.XAdd(ctx, &redis.XAddArgs{
					Stream: ms.Output,
					ID:     "*",
					Values: kvs,
				}).Result()

				errack := redisClient.XAck(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), y.ID).Err()
				if errack != nil {
					log.Printf("%s: Unable to ack message: %s %s ", ms.Input, y.ID, errack)
				}

			}
		}
	}

}
