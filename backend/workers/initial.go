package workers

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/go-redis/redis/v8"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/stats"
	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
)

func InitialWorker(ms types.Microservice, redisClient *redis.Client, rtsClient *redistimeseries.Client, ctx context.Context) {
	log.Printf("Starting worker: %+v", ms)
	runVars := map[string]string{
		"messages": "10000",
		"prefix":   "message",
	}
	redisClient.XGroupCreateMkStream(ctx, ms.Name, fmt.Sprintf("Group-%s", ms.Name), "0").Err()
	for {
		res, _ := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    fmt.Sprintf("Group-%s", ms.Name),
			Consumer: fmt.Sprintf("Consumer-%s", ms.Name),
			Streams:  []string{ms.Name, ">"},
			Count:    1,
			Block:    1 * time.Second,
		}).Result()

		for _, x := range res {
			for _, y := range x.Messages {
				for key, element := range y.Values {
					runVars[key] = element.(string)
				}
				_, errack := redisClient.XAck(ctx, ms.Name, fmt.Sprintf("Group-%s", ms.Name), y.ID).Result()
				if errack != nil {
					log.Printf("%s: Unable to ack message: %s %s ", ms.Name, y.ID, errack)
				}
			}
			p, err := strconv.Atoi(runVars["messages"])
			msgCount := 10000
			if err == nil {
				msgCount = p
			}

			for z := 0; z < msgCount; z++ {
				myname := fmt.Sprintf("%s%09d", runVars["prefix"], z)
				redisClient.XAdd(ctx, &redis.XAddArgs{
					Stream: ms.Output,
					ID:     "*",
					Values: map[string]interface{}{"Name": myname},
				}).Result()
				redisClient.HSetNX(ctx, fmt.Sprintf("STATE:%s", myname), "id", myname)
				stats.DropStat(rtsClient, ms.Name)

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
