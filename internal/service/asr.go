package service

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"audio-labeler/internal/asr"
	"audio-labeler/internal/db"
	"audio-labeler/internal/metrics"
)

type ASRStatus struct {
	Running   bool    `json:"running"`
	Total     int64   `json:"total"`
	Processed int64   `json:"processed"`
	Errors    int64   `json:"errors"`
	Percent   float64 `json:"percent"`
	Rate      float64 `json:"rate"`
	AvgWER    float64 `json:"avg_wer"`
	Elapsed   string  `json:"elapsed"`
	LastError string  `json:"last_error,omitempty"`
}

type ASRService struct {
	db             *db.DB
	decoder        *asr.KaldiDecoder
	defaultWorkers int
	running        int32
	stopFlag       int32
	processed      int64
	errors         int64
	total          int64
	totalWER       float64
	startTime      time.Time
	lastError      string
	mu             sync.Mutex
}

func NewASRService(database *db.DB, modelDir string, defaultWorkers int) (*ASRService, error) {
	decoder, err := asr.NewKaldiDecoder(modelDir)
	if err != nil {
		return nil, err
	}

	return &ASRService{
		db:             database,
		decoder:        decoder,
		defaultWorkers: defaultWorkers,
	}, nil
}

func (s *ASRService) Start(limit, workers int) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("ASR already running")
	}

	if workers <= 0 {
		workers = s.defaultWorkers
	}

	if err := s.decoder.Health(); err != nil {
		atomic.StoreInt32(&s.running, 0)
		return errors.New("Kaldi not available: " + err.Error())
	}

	// Reset counters
	atomic.StoreInt64(&s.processed, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt32(&s.stopFlag, 0)
	s.totalWER = 0
	s.lastError = ""
	s.startTime = time.Now()

	go s.run(limit, workers)
	return nil
}

func (s *ASRService) Stop() {
	atomic.StoreInt32(&s.stopFlag, 1)
}

func (s *ASRService) setLastError(err string) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

func (s *ASRService) addWER(wer float64) {
	s.mu.Lock()
	s.totalWER += wer
	s.mu.Unlock()
}

func (s *ASRService) Status() ASRStatus {
	p := atomic.LoadInt64(&s.processed)
	e := atomic.LoadInt64(&s.errors)
	t := atomic.LoadInt64(&s.total)

	var pct, rate, avgWER float64
	elapsed := time.Since(s.startTime)

	if t > 0 {
		pct = float64(p+e) / float64(t) * 100
	}
	if elapsed.Seconds() > 0 {
		rate = float64(p) / elapsed.Seconds()
	}

	s.mu.Lock()
	if p > 0 {
		avgWER = s.totalWER / float64(p)
	}
	lastErr := s.lastError
	s.mu.Unlock()

	return ASRStatus{
		Running:   atomic.LoadInt32(&s.running) == 1,
		Total:     t,
		Processed: p,
		Errors:    e,
		Percent:   pct,
		Rate:      rate,
		AvgWER:    avgWER,
		Elapsed:   elapsed.Round(time.Second).String(),
		LastError: lastErr,
	}
}

func (s *ASRService) run(limit, workers int) {
	defer atomic.StoreInt32(&s.running, 0)

	files, err := s.db.GetPending(limit)
	if err != nil {
		s.setLastError("get pending: " + err.Error())
		log.Printf("ASR get pending error: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("ASR: no pending files")
		return
	}

	atomic.StoreInt64(&s.total, int64(len(files)))
	log.Printf("ASR: processing %d files with %d workers", len(files), workers)

	taskChan := make(chan db.AudioFile, 100)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go s.worker(&wg, taskChan)
	}

	for _, file := range files {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			break
		}
		taskChan <- file
	}
	close(taskChan)

	wg.Wait()

	processed := atomic.LoadInt64(&s.processed)
	avgWER := 0.0
	if processed > 0 {
		s.mu.Lock()
		avgWER = s.totalWER / float64(processed) * 100
		s.mu.Unlock()
	}

	log.Printf("ASR complete: processed=%d errors=%d avgWER=%.2f%%",
		processed,
		atomic.LoadInt64(&s.errors),
		avgWER)
}

func (s *ASRService) worker(wg *sync.WaitGroup, tasks <-chan db.AudioFile) {
	defer wg.Done()

	for file := range tasks {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			return
		}

		// Decode
		result, err := s.decoder.Decode(file.FilePath)
		if err != nil {
			s.setLastError("decode: " + err.Error())
			log.Printf("ASR error %s: %v", file.FilePath, err)
			s.db.UpdateError(file.ID, err.Error())
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		if !result.Success {
			s.setLastError(result.Error)
			s.db.UpdateError(file.ID, result.Error)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		// Calculate WER/CER
		wer := metrics.WER(file.TranscriptionOriginal, result.Text)
		cer := metrics.CER(file.TranscriptionOriginal, result.Text)

		// Update database
		err = s.db.UpdateASR(file.ID, result.Text, wer, cer)
		if err != nil {
			s.setLastError("update db: " + err.Error())
			log.Printf("DB update error: %v", err)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		s.addWER(wer)
		atomic.AddInt64(&s.processed, 1)
	}
}

func (s *ASRService) ProcessSingle(filePath string) (string, error) {
	result, err := s.decoder.Decode(filePath)
	if err != nil {
		return "", err
	}
	if !result.Success {
		return "", errors.New(result.Error)
	}
	return result.Text, nil
}
