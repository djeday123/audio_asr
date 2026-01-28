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

// ASRGPUNoLMService обрабатывает файлы через Kaldi GPU batch без LM
type ASRGPUNoLMService struct {
	db        *db.DB
	decoder   *asr.KaldiDecoder
	batchSize int
	running   int32
	stopFlag  int32
	processed int64
	errors    int64
	total     int64
	totalWER  float64
	startTime time.Time
	lastError string
	mu        sync.Mutex
}

func NewASRGPUNoLMService(database *db.DB, modelDir string, batchSize int) (*ASRGPUNoLMService, error) {
	decoder, err := asr.NewKaldiDecoderNoLM(modelDir)
	if err != nil {
		return nil, err
	}

	if batchSize <= 0 {
		batchSize = 32
	}

	return &ASRGPUNoLMService{
		db:        database,
		decoder:   decoder,
		batchSize: batchSize,
	}, nil
}

func (s *ASRGPUNoLMService) Start(limit int) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("ASR GPU NoLM already running")
	}

	if err := s.decoder.Health(); err != nil {
		atomic.StoreInt32(&s.running, 0)
		return errors.New("Kaldi not available: " + err.Error())
	}

	atomic.StoreInt64(&s.processed, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt32(&s.stopFlag, 0)
	s.totalWER = 0
	s.lastError = ""
	s.startTime = time.Now()

	go s.run(limit)
	return nil
}

func (s *ASRGPUNoLMService) Stop() {
	atomic.StoreInt32(&s.stopFlag, 1)
}

func (s *ASRGPUNoLMService) setLastError(err string) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

func (s *ASRGPUNoLMService) addWER(wer float64) {
	s.mu.Lock()
	s.totalWER += wer
	s.mu.Unlock()
}

func (s *ASRGPUNoLMService) Status() ASRStatus {
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

func (s *ASRGPUNoLMService) run(limit int) {
	defer atomic.StoreInt32(&s.running, 0)

	files, err := s.db.GetPendingNoLM(limit)
	if err != nil {
		s.setLastError("get pending: " + err.Error())
		log.Printf("ASR GPU NoLM get pending error: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("ASR GPU NoLM: no pending files")
		return
	}

	atomic.StoreInt64(&s.total, int64(len(files)))
	log.Printf("ASR GPU NoLM: processing %d files in batches of %d (lm-scale=0)", len(files), s.batchSize)

	for i := 0; i < len(files); i += s.batchSize {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			break
		}

		end := i + s.batchSize
		if end > len(files) {
			end = len(files)
		}

		batch := files[i:end]
		s.processBatch(batch)
	}

	processed := atomic.LoadInt64(&s.processed)
	avgWER := 0.0
	if processed > 0 {
		s.mu.Lock()
		avgWER = s.totalWER / float64(processed) * 100
		s.mu.Unlock()
	}

	log.Printf("ASR GPU NoLM complete: processed=%d errors=%d avgWER=%.2f%%",
		processed,
		atomic.LoadInt64(&s.errors),
		avgWER)
}

func (s *ASRGPUNoLMService) processBatch(files []db.AudioFile) {
	paths := make([]string, len(files))
	fileMap := make(map[string]*db.AudioFile)

	for i, f := range files {
		paths[i] = f.FilePath
		fileMap[f.FilePath] = &files[i]
	}

	// GPU batch декодинг с NoLM (lattice rescoring)
	results, err := s.decoder.DecodeBatchGPU(paths)
	if err != nil {
		s.setLastError("GPU batch decode: " + err.Error())
		log.Printf("ASR GPU NoLM batch error: %v", err)
		for _, f := range files {
			s.db.UpdateASRNoLMError(f.ID, err.Error())
			atomic.AddInt64(&s.errors, 1)
		}
		return
	}

	for path, result := range results {
		file := fileMap[path]
		if file == nil {
			continue
		}

		if !result.Success {
			s.setLastError(result.Error)
			s.db.UpdateASRNoLMError(file.ID, result.Error)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		wer := metrics.WER(file.TranscriptionOriginal, result.Text)
		cer := metrics.CER(file.TranscriptionOriginal, result.Text)

		err := s.db.UpdateASRNoLM(file.ID, result.Text, wer, cer)
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
