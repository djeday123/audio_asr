package service

import (
	"errors"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"audio-labeler/internal/audio"
	"audio-labeler/internal/db"
	"audio-labeler/internal/scanner"
)

type ScanStatus struct {
	Running   bool    `json:"running"`
	Total     int64   `json:"total"`
	Processed int64   `json:"processed"`
	Skipped   int64   `json:"skipped"`
	Errors    int64   `json:"errors"`
	Percent   float64 `json:"percent"`
	Rate      float64 `json:"rate"`
	Elapsed   string  `json:"elapsed"`
	LastError string  `json:"last_error,omitempty"`
}

type Scanner struct {
	db             *db.DB
	dataDir        string
	defaultWorkers int
	running        int32
	stopFlag       int32
	processed      int64
	skipped        int64
	errors         int64
	total          int64
	startTime      time.Time
	lastError      string
	existingPaths  map[string]bool
	mu             sync.Mutex
}

func NewScanner(db *db.DB, dataDir string, defaultWorkers int) *Scanner {
	return &Scanner{
		db:             db,
		dataDir:        dataDir,
		defaultWorkers: defaultWorkers,
	}
}

func (s *Scanner) Start(limit, workers int) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("scan already running")
	}

	if workers <= 0 {
		workers = s.defaultWorkers
	}

	atomic.StoreInt64(&s.processed, 0)
	atomic.StoreInt64(&s.skipped, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt32(&s.stopFlag, 0)
	s.lastError = ""
	s.startTime = time.Now()

	go s.run(limit, workers)
	return nil
}

func (s *Scanner) Stop() {
	atomic.StoreInt32(&s.stopFlag, 1)
}

func (s *Scanner) setLastError(err string) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

func (s *Scanner) Status() ScanStatus {
	p := atomic.LoadInt64(&s.processed)
	sk := atomic.LoadInt64(&s.skipped)
	e := atomic.LoadInt64(&s.errors)
	t := atomic.LoadInt64(&s.total)

	var pct, rate float64
	elapsed := time.Since(s.startTime)

	if t > 0 {
		pct = float64(p+sk+e) / float64(t) * 100
	}
	if elapsed.Seconds() > 0 {
		rate = float64(p) / elapsed.Seconds()
	}

	s.mu.Lock()
	lastErr := s.lastError
	s.mu.Unlock()

	return ScanStatus{
		Running:   atomic.LoadInt32(&s.running) == 1,
		Total:     t,
		Processed: p,
		Skipped:   sk,
		Errors:    e,
		Percent:   pct,
		Rate:      rate,
		Elapsed:   elapsed.Round(time.Second).String(),
		LastError: lastErr,
	}
}

func (s *Scanner) run(limit, workers int) {
	defer atomic.StoreInt32(&s.running, 0)

	log.Printf("Scanning %s with limit=%d workers=%d", s.dataDir, limit, workers)

	// Загружаем все существующие пути из базы
	log.Println("Loading existing file paths from database...")
	existingPaths, err := s.db.GetAllFilePaths()
	if err != nil {
		s.setLastError("load paths: " + err.Error())
		log.Printf("Load paths error: %v", err)
		return
	}
	s.existingPaths = existingPaths
	log.Printf("Found %d existing files in database", len(existingPaths))

	tasks, err := scanner.ScanLibriSpeech(s.dataDir, limit)
	if err != nil {
		s.setLastError("scan dir: " + err.Error())
		log.Printf("Scan error: %v", err)
		return
	}
	atomic.StoreInt64(&s.total, int64(len(tasks)))
	log.Printf("Found %d tasks in directory", len(tasks))

	taskChan := make(chan scanner.AudioTask, 100)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go s.worker(&wg, taskChan)
	}

	for _, task := range tasks {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			break
		}
		taskChan <- task
	}
	close(taskChan)

	wg.Wait()
	log.Printf("Scan complete: processed=%d skipped=%d errors=%d",
		atomic.LoadInt64(&s.processed),
		atomic.LoadInt64(&s.skipped),
		atomic.LoadInt64(&s.errors))
}

func (s *Scanner) worker(wg *sync.WaitGroup, tasks <-chan scanner.AudioTask) {
	defer wg.Done()

	for task := range tasks {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			return
		}

		// Быстрая проверка по пути — без чтения файла
		if s.existingPaths[task.WavPath] {
			atomic.AddInt64(&s.skipped, 1)
			continue
		}

		// Hash
		hash, err := audio.MD5File(task.WavPath)
		if err != nil {
			s.setLastError("hash: " + err.Error())
			log.Printf("Hash error %s: %v", task.WavPath, err)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		// Check duplicate
		exists, _ := s.db.ExistsByHash(hash)
		if exists {
			atomic.AddInt64(&s.skipped, 1)
			continue
		}

		// Metadata via ffprobe
		meta, err := audio.GetMetadata(task.WavPath)
		if err != nil {
			s.setLastError("metadata: " + err.Error())
			log.Printf("Metadata error %s: %v", task.WavPath, err)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		// Stats via sox (SNR, RMS)
		stats, err := audio.GetStats(task.WavPath)
		if err != nil {
			s.setLastError("stats: " + err.Error())
			log.Printf("Stats error %s: %v", task.WavPath, err)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		// Защита от NaN/Inf
		snrDB := stats.SNREstimate
		snrSox := stats.SNRSox
		snrWada := stats.SNRWada
		rmsDB := stats.RMSLevDB

		if math.IsNaN(snrDB) || math.IsInf(snrDB, 0) || snrDB > 999 || snrDB < -999 {
			snrDB = 0
		}
		if math.IsNaN(snrSox) || math.IsInf(snrSox, 0) || snrSox > 999 || snrSox < -999 {
			snrSox = 0
		}
		if math.IsNaN(snrWada) || math.IsInf(snrWada, 0) || snrWada > 999 || snrWada < -999 {
			snrWada = 0
		}
		if math.IsNaN(rmsDB) || math.IsInf(rmsDB, 0) || rmsDB > 999 || rmsDB < -999 {
			rmsDB = 0
		}

		af := &db.AudioFile{
			UserID:                task.UserID,
			ChapterID:             task.ChapterID,
			FilePath:              task.WavPath,
			FileHash:              hash,
			DurationSec:           meta.DurationSec,
			SNRDB:                 stats.SNREstimate,
			SNRSox:                stats.SNRSox,
			SNRWada:               stats.SNRWada,
			NoiseLevel:            stats.NoiseLevel,
			RMSDB:                 stats.RMSLevDB,
			SampleRate:            meta.SampleRate,
			Channels:              meta.Channels,
			BitDepth:              meta.BitDepth,
			FileSize:              meta.FileSize,
			AudioMetadata:         meta.ToJSON(),
			TranscriptionOriginal: task.Transcription,
		}

		_, err = s.db.Insert(af)
		if err != nil {
			log.Printf("=============== \n Insert error: %v | SNR: sox=%.2f spectral=%.2f vad=%.2f wada=%.2f estimate=%.2f rms=%.2f | file=%s",
				err,
				stats.SNRSox,
				stats.SNRSpectral,
				stats.SNRVad,
				stats.SNRWada,
				stats.SNREstimate,
				stats.RMSLevDB,
				task.WavPath,
			)
			s.setLastError("insert: " + err.Error())
			log.Printf("Insert error: %v", err)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		atomic.AddInt64(&s.processed, 1)
	}
}
