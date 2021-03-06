package workers

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/RedisLabs-Field-Engineering/demo-microservices-saga/types"
	"github.com/go-redis/redis/v8"
)

func StandardSaver(ms types.Microservice, redisClient *redis.Client, ctx context.Context) {
	if ms.SaveBatchSize == 0 {
		ms.SaveBatchSize = 10
	}
	for {
		pend, err := redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   ms.Input,
			Group:    fmt.Sprintf("Group-%s", ms.Input),
			Start:    "-",
			End:      "+",
			Count:    int64(ms.SaveBatchSize),
			Consumer: fmt.Sprintf("Consumer-%s", ms.Input),
		}).Result()

		if err != nil {
			log.Printf("%s: Error getting pending: %s", ms.Name, err)
			time.Sleep(1 * time.Second)
		}
		time.Sleep(1000 * time.Millisecond)
		if len(pend) > 0 {
			var msgids []string
			for _, z := range pend {
				msgids = append(msgids, z.ID)
			}
			claims, cerr := redisClient.XClaim(ctx, &redis.XClaimArgs{
				Stream:   ms.Input,
				Group:    fmt.Sprintf("Group-%s", ms.Input),
				Consumer: fmt.Sprintf("Consumer-%s", ms.Input),
				MinIdle:  8 * time.Second,
				Messages: msgids,
			}).Result()
			if cerr != nil {
				log.Printf("%s: Error claiming: %s", ms.Name, err)
			}
			for _, k := range claims {
				kvs := map[string]interface{}{
					fmt.Sprintf("%s-ts", ms.Input):    time.Now().UnixNano() / int64(time.Millisecond),
					fmt.Sprintf("%s-retry", ms.Input): 1,
				}
				for r, s := range k.Values {
					kvs[r] = s
				}
				xadderr := redisClient.XAdd(ctx, &redis.XAddArgs{
					Stream: ms.Output,
					ID:     "*",
					Values: kvs,
				}).Err()
				if xadderr == nil {

					// TODO: handle this
					redisClient.HSetNX(ctx, fmt.Sprintf("STATE:%s", k.Values["Name"]), ms.Name, k.ID)

					errack := redisClient.XAck(ctx, ms.Input, fmt.Sprintf("Group-%s", ms.Input), k.ID).Err()
					if errack != nil {
						log.Printf("%s: Unable to ack message: %s %s ", ms.Input, k.ID, errack)
					}
				}

			}
		}

	}

}
