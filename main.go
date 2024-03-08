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
	for line := range lines {
		// Добавляем строку в список 'pass' в Redis.
		err := rdb.RPush(ctx, "pass", line).Err()
		if err != nil {
			fmt.Printf("Ошибка при добавлении строки в список Redis: %v\n", err)
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
		panic(err)
	}

	fmt.Println("Загрузка данных в Redis завершена.")
}
