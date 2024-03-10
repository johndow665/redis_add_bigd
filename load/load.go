package load

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
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

	// Определение начальной позиции для чтения файла
	startPosition := getLastProcessedLine("tl.txt")

	scanner := bufio.NewScanner(file)

	// Пропуск строк до достижения начальной позиции
	for i := int64(0); i < startPosition && scanner.Scan(); i++ {
		// Просто продолжаем сканирование без действий
	}

	pipeline := rdb.Pipeline()
	var count int64 = 0
	for scanner.Scan() {
		pipeline.SAdd(ctx, setName, scanner.Text())
		count++
		if count == batchSize {
			break // Завершаем цикл после обработки batchSize строк
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Ошибка при чтении файла: %v\n", err)
		return
	}

	_, err = pipeline.Exec(ctx)
	if err != nil {
		fmt.Printf("Ошибка при добавлении строк в множество Redis: %v\n", err)
		return
	}

	// Сохранение прогресса после обработки пакета
	saveLastProcessedLine("tl.txt", startPosition+count)
	fmt.Printf("Обработано строк с %d по %d\n", startPosition, startPosition+count)
}

func getLastProcessedLine(filename string) int64 {
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0 // Если файла нет, начинаем с начала
	}
	lineNumber, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0 // Если ошибка при чтении, начинаем с начала
	}
	return lineNumber
}

func saveLastProcessedLine(filename string, lineNumber int64) {
	err := os.WriteFile(filename, []byte(strconv.FormatInt(lineNumber, 10)), 0644)
	if err != nil {
		fmt.Printf("Ошибка при записи в файл прогресса: %v\n", err)
	}
}
