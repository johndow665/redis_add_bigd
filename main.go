package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

func countLines(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var count int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func processLines(ctx context.Context, rdb *redis.Client, lines <-chan string, totalLines int64, wg *sync.WaitGroup, startTime time.Time) {
	defer wg.Done()
	var count int64 = 0
	pipeline := rdb.Pipeline()
	const batchSize = 100

	for line := range lines {
		pipeline.SAdd(ctx, "pass", line)
		count++
		if count%batchSize == 0 {
			_, err := pipeline.Exec(ctx)
			if err != nil {
				fmt.Printf("Ошибка при добавлении пакета строк в множество Redis: %v\n", err)
			}
			elapsed := time.Since(startTime)
			processed := count
			remaining := totalLines - processed
			estimatedRemaining := time.Duration(float64(elapsed) / float64(processed) * float64(remaining))
			fmt.Printf("Добавлено строк в множество 'pass': %d, примерно осталось времени: %v\n", count, estimatedRemaining)
		}
	}
	if count%batchSize != 0 {
		_, err := pipeline.Exec(ctx)
		if err != nil {
			fmt.Printf("Ошибка при добавлении оставшихся строк в множество Redis: %v\n", err)
		}
	}
	fmt.Printf("Добавлено строк в множество 'pass': %d\n", count)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Необходимо указать путь к файлу.")
		os.Exit(1)
	}
	filePath := os.Args[1]

	totalLines, err := countLines(filePath)
	if err != nil {
		fmt.Printf("Ошибка при подсчете строк в файле: %v\n", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("Ошибка подключения к Redis: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Ответ Redis на PING: %s\n", pong)

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Ошибка при открытии файла: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	lines := make(chan string)
	var wg sync.WaitGroup

	numWorkers := 10 // Устанавливаем количество горутин равным 10 или любому другому значению, которое вы считаете оптимальным
	startTime := time.Now()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processLines(ctx, rdb, lines, totalLines, &wg, startTime)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
	close(lines)

	wg.Wait()

	if err := scanner.Err(); err != nil {
		fmt.Printf("Ошибка при чтении файла: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Загрузка данных в Redis завершена.")
}
