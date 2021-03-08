package stats

import (
	"fmt"
	"log"
	"time"

	redistimeseries "github.com/RedisTimeSeries/redistimeseries-go"
)

func DropStat(client *redistimeseries.Client, statname string) {
	currentTimestamp := time.Now().UnixNano() / 1e6
	labels := map[string]string{"stage": statname}
	keyname := fmt.Sprintf("TS:%s:Ops", statname)
	_, haveit := client.Info(keyname)
	if haveit != nil {
		client.CreateKeyWithOptions(keyname, redistimeseries.DefaultCreateOptions)
	}
	_, err := client.IncrBy(
		keyname,
		currentTimestamp,
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
