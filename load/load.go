package load

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
)

func LoadLines(ctx context.Context, rdb *redis.Client, setName string, filePath string, batchSize int64, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Ошибка при открытии файла: %v\n", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := make(chan string)

	go func() {
		defer close(lines) // Закрытие канала после отправки всех строк
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Printf("Ошибка при чтении файла: %v\n", err)
		}
	}()

	var count int64 = 0
	pipeline := rdb.Pipeline()
	const updateInterval = 10000
	const saveInterval = 10000000 // Интервал для BGSAVE
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
			if count%saveInterval == 0 {
				err := rdb.BgSave(ctx).Err()
				if err != nil {
					fmt.Printf("\nОшибка при инициации BGSAVE: %v\n", err)
				} else {
					fmt.Println("\nBGSAVE инициирован.")
				}
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
