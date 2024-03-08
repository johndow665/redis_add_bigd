package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
)

func processLines(ctx context.Context, rdb *redis.Client, lines <-chan string, wg *sync.WaitGroup, workerID int) {
	defer wg.Done()
	for line := range lines {
		key := fmt.Sprintf("key:%d", workerID) // Генерируем уникальный ключ для каждой строки.
		err := rdb.Set(ctx, key, line, 0).Err()
		if err != nil {
			fmt.Printf("Ошибка при записи в Redis: %v\n", err)
			continue
		}
	}
}

func main() {
	// Создаем клиент Redis.
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Адрес сервера Redis.
		Password: "",               // Пароль, если есть.
		DB:       0,                // Используемая база данных.
	})

	ctx := context.Background()

	// Открываем файл.
	file, err := os.Open("your_large_file.txt") // Замените на имя вашего файла.
	if err != nil {
		panic(err)
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
		go processLines(ctx, rdb, lines, &wg, i)
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
		panic(err)
	}

	fmt.Println("Загрузка данных в Redis завершена.")
}
