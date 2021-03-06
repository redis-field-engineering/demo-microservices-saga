package workers

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
	"github.com/go-redis/redis/v8"
)

func FinalWorker(ms types.Microservice, redisClient *redis.Client, ctx context.Context) {
	log.Println("Starting worker: ", ms)
	redisClient.XGroupCreateMkStream(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), "0").Err()
	for {
		res, _ := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    fmt.Sprintf("Group-%s", ms.Input),
			Consumer: fmt.Sprintf("Consumer-%s", ms.Input),
			Streams:  []string{ms.Input, ">"},
			Count:    1,
			Block:    1 * time.Second,
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

				log.Printf("%s - completed %s", ms.Input, y.ID)
			}
		}
	}

}
