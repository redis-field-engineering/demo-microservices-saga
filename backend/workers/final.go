package workers

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/stats"
	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/go-redis/redis/v8"
)

func FinalWorker(ms types.Microservice, redisClient *redis.Client, rtsClient *redistimeseries.Client, ctx context.Context) {
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
			Block:    10 * time.Millisecond,
		}).Result()

		for _, x := range res {
			for _, y := range x.Messages {
				kvs := map[string]interface{}{
					fmt.Sprintf("%s-ts", ms.Input): time.Now().UnixNano() / int64(time.Millisecond),
				}
				for k, v := range y.Values {
					kvs[k] = v
				}

				errack := redisClient.XAck(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), y.ID).Err()
				if errack != nil {
					log.Printf("%s: Unable to ack message: %s %s ", ms.Input, y.ID, errack)
				}
				redisClient.HSetNX(ctx, fmt.Sprintf("STATE:%s", y.Values["Name"]), ms.Name, y.ID)
				stats.DropStat(rtsClient, ms.Name)

			}
		}
	}

}
