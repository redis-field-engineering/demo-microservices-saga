package workers

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
)

func InitialWorker(ms types.Microservice, redisClient *redis.Client, ctx context.Context) {
	log.Printf("Starting worker: %+v", ms)
	runVars := map[string]string{
		"messages": "10000",
		"prefix":   "message",
	}
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
				for key, element := range y.Values {
					runVars[key] = element.(string)
				}
				_, errack := redisClient.XAck(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), y.ID).Result()
				if errack != nil {
					log.Printf("%s: Unable to ack message: %s %s ", ms.Input, y.ID, errack)
				}
			}
			p, err := strconv.Atoi(runVars["messages"])
			msgCount := 10000
			if err == nil {
				msgCount = p
			}

			for z := 0; z < msgCount; z++ {
				myname := fmt.Sprintf("%s-%d", runVars["prefix"], z)
				redisClient.XAdd(ctx, &redis.XAddArgs{
					Stream: ms.Output,
					ID:     "*",
					Values: map[string]interface{}{"Name": myname},
				}).Result()
				redisClient.HSetNX(ctx, fmt.Sprintf("STATE:%s", myname), "id", myname)

				// sleep by default for 2 ms
				d := 2
				if ms.ProcMax-ms.ProcMin > 0 {
					d = rand.Intn(ms.ProcMax-ms.ProcMin) + ms.ProcMin
				}
				time.Sleep(time.Duration(d) * time.Millisecond)
			}

		}
	}

}
