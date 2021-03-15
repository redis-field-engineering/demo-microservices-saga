package workers

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/stats"
	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/go-redis/redis/v8"
)

func StandardWorker(ms types.Microservice, redisClient *redis.Client, rtsClient *redistimeseries.Client, ctx context.Context) {
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
				// Inject some errors
				if ms.ErrorRate > 0 {
					if rand.Intn(10000) <= int(100*ms.ErrorRate) {
						stats.LogworkerError(
							ctx, redisClient, ms.Name,
							fmt.Sprintf("Consumer-%s", ms.Input),
							y.Values["Name"].(string),
							"Random error as configured")
						continue
					}
				}
				// Check to see if we've already seen this
				nxres, _ := redisClient.HSetNX(ctx, fmt.Sprintf("STATE:%s", y.Values["Name"]), ms.Name, y.ID).Result()

				// have not seen before
				if nxres == true {
					xadderr := redisClient.XAdd(ctx, &redis.XAddArgs{
						Stream: ms.Output,
						ID:     "*",
						Values: kvs,
					}).Err()

					if xadderr != nil {
						stats.LogworkerError(
							ctx, redisClient, ms.Name,
							fmt.Sprintf("Consumer-%s", ms.Input),
							y.Values["Name"].(string),
							fmt.Sprintf("Could not add message to next stream: %s : %s ", ms.Output, xadderr),
						)

					}

					stats.DropStat(rtsClient, ms.Name)
					// have seen before
				} else {
					stats.LogworkerError(
						ctx, redisClient, ms.Name,
						fmt.Sprintf("Consumer-%s", ms.Input),
						y.Values["Name"].(string),
						fmt.Sprintf("The message %s has already been seen", y.Values["Name"]),
					)
				}

				// Either way we have to ack this messages so it does not get reprocesses
				errack := redisClient.XAck(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), y.ID).Err()
				if errack != nil {
					log.Printf("%s: Unable to ack message: %s %s ", ms.Input, y.ID, errack)
				}
			}
		}
	}

}
