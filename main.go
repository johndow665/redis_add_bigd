package main

import (
	"context"
	"fmt"
	"os"
	"redis_add_data/load"
	"redis_add_data/parse"
	"strconv"
	"sync"

	"github.com/go-redis/redis/v8"
)

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

		wg.Add(1)
		go load.LoadLines(ctx, rdb, setName, filePath, batchSize, &wg)
		wg.Wait()

		fmt.Println("Загрузка данных в Redis завершена.")

	case "parse":
		wg.Add(1)
		go parse.ParseLines(ctx, rdb, setName, filePath, &wg)
		wg.Wait()

	default:
		fmt.Println("Неизвестный режим работы. Используйте 'load' или 'parse'.")
		os.Exit(1)
	}
}
