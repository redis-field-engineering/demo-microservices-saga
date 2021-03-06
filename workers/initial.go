package workers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
)

func InitialWorker(ms types.Microservice, redisClient *redis.Client, ctx context.Context) {
	log.Println("Starting worker: ", ms)
	runVars := map[string]int{
		"messages":  10000,
		"errorrate": 0,
		"sleepms":   5,
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
					p, err := strconv.Atoi(element.(string))
					if err == nil {
						runVars[key] = p
					}
				}
				_, errack := redisClient.XAck(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), y.ID).Result()
				if errack != nil {
					log.Printf("%s: Unable to ack message: %s %s ", ms.Input, y.ID, errack)
				}
			}
			for z := 0; z <= runVars["messages"]; z++ {
				myname := fmt.Sprintf("message-%d", z)
				redisClient.XAdd(ctx, &redis.XAddArgs{
					Stream: ms.Output,
					ID:     "*",
					Values: map[string]interface{}{"Name": myname},
				}).Result()
				time.Sleep(time.Duration(runVars["sleepms"]) * time.Millisecond)
			}

		}
	}

}
