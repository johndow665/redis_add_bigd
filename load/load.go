package load

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
)

func loadLines(ctx context.Context, rdb *redis.Client, setName string, lines <-chan string, batchSize int64, wg *sync.WaitGroup) {
	defer wg.Done()
	var count int64 = 0
	pipeline := rdb.Pipeline()
	const updateInterval = 10000
	for line := range lines {
		pipeline.SAdd(ctx, setName, line)
		count++
		if count%batchSize == 0 {
			_, err := pipeline.Exec(ctx)
			if err != nil {
				fmt.Printf("\nОшибка при добавлении пакета строк в множество Redis: %v\n", err)
			}
			if count%updateInterval == 0 {
				fmt.Printf("\rДобавлено строк в множество '%s': %d", setName, count)
			}
		}
	}
	if count%batchSize != 0 {
		_, err := pipeline.Exec(ctx)
		if err != nil {
			fmt.Printf("\nОшибка при добавлении оставшихся строк в множество Redis: %v\n", err)
		}
	}
	fmt.Printf("\rДобавлено строк в множество '%s': %d\n", setName, count)
}
