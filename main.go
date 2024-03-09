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

func parseLines(ctx context.Context, rdb *redis.Client, setName string, filePath string, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("Ошибка при создании файла: %v\n", err)
		return
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	var cursor uint64
	const pageSize = 1000
	for {
		keys, nextCursor, err := rdb.SScan(ctx, setName, cursor, "", pageSize).Result()
		if err != nil {
			fmt.Printf("Ошибка при сканировании множества Redis: %v\n", err)
			return
		}

		pipeline := rdb.Pipeline()
		for _, key := range keys {
			if len(key) >= 6 && len(key) <= 24 {
				_, err := writer.WriteString(key + "\n")
				if err != nil {
					fmt.Printf("Ошибка при записи в файл: %v\n", err)
					return
				}
				pipeline.SRem(ctx, setName, key)
			}
		}

		_, err = pipeline.Exec(ctx)
		if err != nil {
			fmt.Printf("Ошибка при удалении элементов из множества Redis: %v\n", err)
			return
		}

		err = writer.Flush()
		if err != nil {
			fmt.Printf("Ошибка при сбросе буфера записи: %v\n", err)
			return
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	fmt.Println("Парсинг и удаление данных из Redis завершены.")
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: main.exe <load|parse> <set name> <file path> [batch size]")
		os.Exit(1)
	}

	mode := os.Args[1]
	setName := os.Args[2]
	filePath := os.Args[3]

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

	var wg sync.WaitGroup

	switch mode {
	case "load":
		var batchSize int64 = 1000000
		if len(os.Args) > 4 {
			batchSize, err = strconv.ParseInt(os.Args[4], 10, 64)
			if err != nil {
				fmt.Printf("Ошибка при чтении размера пакета: %v\n", err)
				os.Exit(1)
			}
		}

		file, err := os.Open(filePath)
		if err != nil {
			fmt.Printf("Ошибка при открытии файла: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()

		lines := make(chan string)
		numWorkers := 1 // Можно увеличить количество воркеров для ускорения загрузки
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go loadLines(ctx, rdb, setName, lines, batchSize, &wg)
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

	case "parse":
		wg.Add(1)
		go parseLines(ctx, rdb, setName, filePath, &wg)
		wg.Wait()

	default:
		fmt.Println("Неизвестный режим работы. Используйте 'load' или 'parse'.")
		os.Exit(1)
	}
}
