package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
)

func processLines(ctx context.Context, rdb *redis.Client, lines <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var count int64 = 0
	pipeline := rdb.Pipeline() // это пайплайн
	const batchSize = 100      // размер пакет
	for line := range lines {
		pipeline.SAdd(ctx, "pass", line)
		count++
		if count%batchSize == 0 {
			_, err := pipeline.Exec(ctx)
			if err != nil {
				fmt.Printf("Ошибка при добавлении пакета строк в множество Redis: %v\n", err)
			}
			fmt.Printf("Добавлено строк в множество 'pass': %d\n", count)
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
		fmt.Println("File path.")
		os.Exit(1)
	}
	filePath := os.Args[1]

	// Создаем клиент Redis.
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379", // Адрес сервера Redis.
		Password: "",               // Пароль, если есть.
		DB:       0,                // Используемая база данных.
	})

	ctx := context.Background()

	// Проверяем подключение к Redis.
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("Ошибка подключения к Redis: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Ответ Redis на PING: %s\n", pong)

	// Открываем файл.
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Ошибка при открытии файла: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Создаем канал для строк файла.
	lines := make(chan string)

	// Используем WaitGroup для ожидания завершения всех горутин.
	var wg sync.WaitGroup

	// Запускаем горутины.
	numWorkers := 1 // Укажите нужное количество горутин.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processLines(ctx, rdb, lines, &wg)
	}

	// Читаем строки из файла и отправляем их в канал.
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
	close(lines) // Закрываем канал после чтения всех строк.

	// Ожидаем завершения всех горутин.
	wg.Wait()

	if err := scanner.Err(); err != nil {
		fmt.Printf("Ошибка при чтении файла: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Загрузка данных в Redis завершена.")
}
