package stats

import (
	"context"
	"fmt"
	"log"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
	"github.com/go-redis/redis/v8"
)

func DropStat(client *redistimeseries.Client, statname string) {
	labels := map[string]string{"stage": statname}
	keyname := fmt.Sprintf("TS:%s:Ops", statname)
	_, haveit := client.Info(keyname)
	if haveit != nil {
		client.CreateKeyWithOptions(keyname, redistimeseries.DefaultCreateOptions)
	}
	_, err := client.IncrByAutoTs(
		keyname,
		1,
		redistimeseries.CreateOptions{
			Uncompressed: false,
			Labels:       labels,
		},
	)

	if err != nil {
		log.Printf("Stats Error: %s : %s : %s", statname, keyname, err)
	}

}

func LogworkerError(ctx context.Context, redisClient *redis.Client, ms string, consumer string, msg string, errmsg string) {
	kvs := map[string]interface{}{
		"timestamp":    time.Now().UnixNano() / int64(time.Millisecond),
		"microservice": ms,
		"consumer":     consumer,
		"message":      msg,
		"error":        errmsg,
	}

	err := redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: "Errors",
		ID:     "*",
		Values: kvs,
	}).Err()

	if err != nil {
		log.Printf("Unable to write to stream error message:%s content: %s:%s\n", err, ms, errmsg)
	}

}
