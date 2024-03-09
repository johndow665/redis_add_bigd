package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-redis/redis/v8"
)

func processLines(ctx context.Context, rdb *redis.Client, setName string, lines <-chan string, batchSize int64, wg *sync.WaitGroup) {
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

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: main.exe <set name> <file path> [batch size]")
		os.Exit(1)
	}
	setName := os.Args[1]
	filePath := os.Args[2]

	// Установка значения batchSize по умолчанию
	var batchSize int64 = 1000000
	if len(os.Args) > 3 {
		var err error
		batchSize, err = strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			fmt.Printf("Ошибка при чтении размера пакета: %v\n", err)
			os.Exit(1)
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
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

	numWorkers := 1
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processLines(ctx, rdb, setName, lines, batchSize, &wg)
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
