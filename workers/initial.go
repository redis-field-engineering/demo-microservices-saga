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
		res, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{}).Result()
		if err != nil {
			log.Println("Recreating stream: ", ms.Input, err)
			redisClient.XGroupCreateMkStream(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), "0").Err()
		}
		for _, x := range res {
			for _, y := range x.Messages {
				for key, element := range y.Values {
					p, err := strconv.Atoi(element.(string))
					if err == nil {
						runVars[key] = p
					}
				}
				_, errdel := redisClient.XDel(ctx, ms.Input, y.ID).Result()
				if errdel != nil {
					log.Println("Unable to delete message: ", y.ID)
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
