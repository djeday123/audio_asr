package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"audio-labeler/internal/asr"
	"audio-labeler/internal/audio"
	"audio-labeler/internal/db"
	"audio-labeler/internal/metrics"
	"audio-labeler/internal/scanner"
)

type Config struct {
	DataDir    string
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	ASRHost    string
	ASRKey     string
	Workers    int
	BatchSize  int
	ScanOnly   bool
}

var (
	processed  int64
	errors     int64
	skipped    int64
	totalFiles int64
	stopFlag   int32
)

func main() {
	cfg := parseFlags()

	// Подключение к БД
	database, err := db.New(cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)
	if err != nil {
		log.Fatalf("DB connection failed: %v", err)
	}
	defer database.Close()
	log.Println("✓ Connected to MariaDB")

	// ASR клиент
	asrClient := asr.NewClient("http://"+cfg.ASRHost, cfg.ASRKey, 60*time.Second)
	log.Println("✓ ASR client ready")

	// Подсчёт файлов
	log.Println("Counting files...")
	count, err := scanner.CountFiles(cfg.DataDir)
	if err != nil {
		log.Fatalf("Count files failed: %v", err)
	}
	atomic.StoreInt64(&totalFiles, int64(count))
	log.Printf("✓ Found %d audio files\n", count)

	if cfg.ScanOnly {
		return
	}

	// Graceful shutdown
	setupSignalHandler()

	// Запуск сканера
	tasks, scanErrs := scanner.ScanLibriSpeech(cfg.DataDir)

	// Worker pool
	var wg sync.WaitGroup
	taskChan := make(chan scanner.AudioTask, cfg.BatchSize)

	// Запуск воркеров
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go worker(i, &wg, taskChan, database, asrClient)
	}

	// Прогресс
	go progressReporter()

	// Раздача задач
	go func() {
		for task := range tasks {
			if atomic.LoadInt32(&stopFlag) == 1 {
				break
			}
			taskChan <- task
		}
		close(taskChan)
	}()

	// Ожидание ошибок сканера
	go func() {
		for err := range scanErrs {
			log.Printf("Scan error: %v", err)
		}
	}()

	// Ждём завершения
	wg.Wait()

	// Финальная статистика
	printStats(database)
}

func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.DataDir, "dir", "", "путь к LibriSpeech данным")
	flag.StringVar(&cfg.DBHost, "db-host", "127.0.0.1", "MariaDB host")
	flag.IntVar(&cfg.DBPort, "db-port", 53306, "MariaDB port")
	flag.StringVar(&cfg.DBUser, "db-user", "root", "MariaDB user")
	flag.StringVar(&cfg.DBPassword, "db-pass", "", "MariaDB password")
	flag.StringVar(&cfg.DBName, "db-name", "audio_labeling", "MariaDB database")
	flag.StringVar(&cfg.ASRHost, "asr-host", "127.0.0.1:28000", "ASR API host")
	flag.StringVar(&cfg.ASRKey, "asr-key", "", "ASR API key")
	flag.IntVar(&cfg.Workers, "workers", 10, "количество воркеров")
	flag.IntVar(&cfg.BatchSize, "batch", 100, "размер буфера")
	flag.BoolVar(&cfg.ScanOnly, "scan-only", false, "только подсчитать файлы")

	flag.Parse()

	if cfg.DataDir == "" {
		log.Fatal("укажи -dir")
	}
	if cfg.DBPassword == "" {
		log.Fatal("укажи -db-pass")
	}
	if cfg.ASRKey == "" {
		log.Fatal("укажи -asr-key")
	}

	return cfg
}

func worker(id int, wg *sync.WaitGroup, tasks <-chan scanner.AudioTask, database *db.DB, asrClient *asr.Client) {
	defer wg.Done()
	log.Printf("Worker %d started", id)

	for task := range tasks {
		if atomic.LoadInt32(&stopFlag) == 1 {
			log.Printf("Worker %d stopping", id)
			return
		}
		processTask(task, database, asrClient)
	}
	log.Printf("Worker %d finished", id)
}

func processTask(task scanner.AudioTask, database *db.DB, asrClient *asr.Client) {
	// 1. Хеш файла
	hash, err := audio.MD5File(task.WavPath)
	if err != nil {
		log.Printf("Hash error %s: %v", task.WavPath, err)
		atomic.AddInt64(&errors, 1)
		return
	}

	// 2. Проверка дубликата
	exists, err := database.ExistsByHash(hash)
	if err != nil {
		log.Printf("DB check error: %v", err)
		atomic.AddInt64(&errors, 1)
		return
	}
	if exists {
		atomic.AddInt64(&skipped, 1)
		return
	}

	// 3. Метаданные аудио
	meta, err := audio.GetMetadata(task.WavPath)
	if err != nil {
		log.Printf("Metadata error %s: %v", task.WavPath, err)
		atomic.AddInt64(&errors, 1)
		return
	}

	// 4. Сохраняем в БД (pending)
	af := &db.AudioFile{
		FilePath:              task.WavPath,
		FileHash:              hash,
		DurationSec:           meta.DurationSec,
		SampleRate:            meta.SampleRate,
		Channels:              meta.Channels,
		BitDepth:              meta.BitDepth,
		FileSize:              meta.FileSize,
		AudioMetadata:         meta.ToJSON(),
		TranscriptionOriginal: task.Transcription,
	}

	id, err := database.Insert(af)
	if err != nil {
		log.Printf("DB insert error: %v", err)
		atomic.AddInt64(&errors, 1)
		return
	}

	// 5. ASR распознавание
	result, err := asrClient.Recognize(task.WavPath)
	if err != nil {
		log.Printf("ASR error %s: %v", task.WavPath, err)
		database.UpdateError(id, err.Error())
		atomic.AddInt64(&errors, 1)
		return
	}

	// 6. Расчёт WER/CER
	wer := metrics.WER(task.Transcription, result.Text)
	cer := metrics.CER(task.Transcription, result.Text)

	// 7. Обновляем БД
	if err := database.UpdateASR(id, result.Text, wer, cer); err != nil {
		log.Printf("DB update error: %v", err)
		atomic.AddInt64(&errors, 1)
		return
	}

	atomic.AddInt64(&processed, 1)
}

func setupSignalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Println("\n⚠ Stopping gracefully...")
		atomic.StoreInt32(&stopFlag, 1)
	}()
}

func progressReporter() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	start := time.Now()

	for range ticker.C {
		if atomic.LoadInt32(&stopFlag) == 1 {
			return
		}
		p := atomic.LoadInt64(&processed)
		e := atomic.LoadInt64(&errors)
		s := atomic.LoadInt64(&skipped)
		t := atomic.LoadInt64(&totalFiles)

		elapsed := time.Since(start)
		rate := float64(p) / elapsed.Seconds()

		pct := float64(p+e+s) / float64(t) * 100

		fmt.Printf("\r⏱ %s | ✓ %d | ✗ %d | ⏭ %d | %.1f%% | %.1f/s    ",
			elapsed.Round(time.Second), p, e, s, pct, rate)
	}
}

func printStats(database *db.DB) {
	fmt.Println("\n\n=== Final Stats ===")
	total, pending, processed, errors, _ := database.Stats()
	fmt.Printf("Total:     %d\n", total)
	fmt.Printf("Pending:   %d\n", pending)
	fmt.Printf("Processed: %d\n", processed)
	fmt.Printf("Errors:    %d\n", errors)
}
