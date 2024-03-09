package parse

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
)

func ParseLines(ctx context.Context, rdb *redis.Client, setName string, filePath string, minLen int, maxLen int, wg *sync.WaitGroup) {
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
	var matchCount int        // Счетчик для подсчета количества совпадений
	const logInterval = 50000 // Частота логирования

	for {
		keys, nextCursor, err := rdb.SScan(ctx, setName, cursor, "", pageSize).Result()
		if err != nil {
			fmt.Printf("Ошибка при сканировании множества Redis: %v\n", err)
			return
		}

		pipeline := rdb.Pipeline()
		for _, key := range keys {
			if len(key) >= minLen && len(key) <= maxLen {
				matchCount++ // Увеличиваем счетчик совпадений
				_, err := writer.WriteString(key + "\n")
				if err != nil {
					fmt.Printf("Ошибка при записи в файл: %v\n", err)
					return
				}
				pipeline.SRem(ctx, setName, key)

				// Логируем каждые logInterval совпадений
				if matchCount%logInterval == 0 {
					fmt.Printf("Найдено совпадений: %d\n", matchCount)
				}
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
	fmt.Printf("Парсинг и удаление данных из Redis завершены. Всего найдено совпадений: %d\n", matchCount)
}
