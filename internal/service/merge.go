package service

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"audio-labeler/internal/audio"
	"audio-labeler/internal/db"
)

type MergeService struct {
	db        *db.DB
	outputDir string
	running   int32
	stopFlag  int32
	processed int64
	errors    int64
	total     int64
	startTime time.Time
	mu        sync.Mutex
}

func NewMergeService(database *db.DB, outputDir string) *MergeService {
	return &MergeService{
		db:        database,
		outputDir: outputDir,
	}
}

type MergeRequest struct {
	IDs       []int64 `json:"ids"`
	OutputDir string  `json:"output_dir,omitempty"`
}

type MergeResult struct {
	NewID         int64   `json:"new_id"`
	OutputPath    string  `json:"output_path"`
	TransPath     string  `json:"trans_path"`
	Duration      float64 `json:"duration"`
	Transcription string  `json:"transcription"`
	ParentIDs     string  `json:"parent_ids"`
	ChapterID     string  `json:"chapter_id"`
}

type MergeQueueStatus struct {
	Running   bool   `json:"running"`
	Total     int64  `json:"total"`
	Processed int64  `json:"processed"`
	Errors    int64  `json:"errors"`
	Elapsed   string `json:"elapsed"`
}

// MergeFiles - существующий метод (оставляем для совместимости)
func (s *MergeService) MergeFiles(ids []int64, outputDir string) (*MergeResult, error) {
	if len(ids) < 2 {
		return nil, fmt.Errorf("need at least 2 files to merge")
	}

	if outputDir == "" {
		outputDir = s.outputDir
	}

	// Получаем все файлы
	files := make([]*db.AudioFile, 0, len(ids))
	var speakerID string

	for _, id := range ids {
		file, err := s.db.GetFile(id)
		if err != nil {
			return nil, fmt.Errorf("file %d not found: %w", id, err)
		}

		if speakerID == "" {
			speakerID = file.UserID
		} else if file.UserID != speakerID {
			return nil, fmt.Errorf("all files must be from same speaker")
		}

		if file.ASRStatus == "processed" && file.WER > 0.001 {
			return nil, fmt.Errorf("file %d has WER %.2f%% (must be 0%%)", id, file.WER*100)
		}

		files = append(files, file)
	}

	return s.mergeFilesInternal(files, speakerID, outputDir)
}

// mergeFilesInternal - внутренняя логика merge
func (s *MergeService) mergeFilesInternal(files []*db.AudioFile, speakerID, outputDir string) (*MergeResult, error) {
	// Получаем следующий chapter_id
	nextChapterID, err := s.db.GetNextChapterID(speakerID)
	if err != nil {
		return nil, fmt.Errorf("get next chapter_id failed: %w", err)
	}

	// Собираем пути и проверяем/добавляем тишину
	paths := make([]string, len(files))
	transcriptions := make([]string, len(files))
	idStrings := make([]string, len(files))

	for i, f := range files {
		// Проверяем тишину в конце
		silenceInfo, err := audio.DetectTrailingSilence(f.FilePath, 100)
		if err != nil {
			log.Printf("Warning: silence check failed for %d: %v", f.ID, err)
		}

		filePath := f.FilePath

		// Если нет тишины — добавляем
		if silenceInfo != nil && !silenceInfo.HasTrailingSilence {
			log.Printf("Adding silence to file %d (no trailing silence)", f.ID)

			// Создаём временный файл с тишиной
			tmpPath := f.FilePath + ".with_silence.wav"
			if err := audio.AddTrailingSilence(f.FilePath, tmpPath, 150); err != nil {
				log.Printf("Warning: failed to add silence to %d: %v", f.ID, err)
			} else {
				filePath = tmpPath
				// Помечаем что временный файл нужно удалить после merge
				defer os.Remove(tmpPath)
			}
		}

		paths[i] = filePath
		transcriptions[i] = f.TranscriptionOriginal
		idStrings[i] = fmt.Sprintf("%d", f.ID)
	}

	// Структура LibriSpeech
	chapterDir := filepath.Join(outputDir, speakerID, nextChapterID)
	if err := os.MkdirAll(chapterDir, 0755); err != nil {
		return nil, fmt.Errorf("create dir failed: %w", err)
	}

	baseName := fmt.Sprintf("%s-%s-0000", speakerID, nextChapterID)
	outputPath := filepath.Join(chapterDir, baseName+".wav")
	transPath := filepath.Join(chapterDir, fmt.Sprintf("%s-%s.trans.txt", speakerID, nextChapterID))

	// Склеиваем аудио (150ms пауза между файлами)
	if err := audio.MergeAudioFiles(paths, outputPath, 150); err != nil {
		return nil, fmt.Errorf("merge failed: %w", err)
	}

	// Объединяем транскрипции
	mergedTranscription := strings.TrimSpace(strings.Join(transcriptions, " "))
	parentIDs := strings.Join(idStrings, "|")

	// trans.txt
	transContent := fmt.Sprintf("%s %s\n", baseName, mergedTranscription)
	if err := os.WriteFile(transPath, []byte(transContent), 0644); err != nil {
		return nil, fmt.Errorf("write trans.txt failed: %w", err)
	}

	// Метаданные
	meta, err := audio.GetMetadata(outputPath)
	if err != nil {
		return nil, fmt.Errorf("get metadata failed: %w", err)
	}

	stats, _ := audio.GetStats(outputPath)
	hash, _ := audio.MD5File(outputPath)

	newFile := &db.AudioFile{
		UserID:                speakerID,
		ChapterID:             nextChapterID,
		FilePath:              outputPath,
		FileHash:              hash,
		DurationSec:           meta.DurationSec,
		SampleRate:            meta.SampleRate,
		Channels:              meta.Channels,
		BitDepth:              meta.BitDepth,
		FileSize:              meta.FileSize,
		AudioMetadata:         meta.ToJSON(),
		TranscriptionOriginal: mergedTranscription,
	}

	if stats != nil {
		newFile.SNRDB = stats.SNREstimate
		newFile.SNRSox = stats.SNRSox
		newFile.SNRWada = stats.SNRWada
		newFile.NoiseLevel = stats.NoiseLevel
		newFile.RMSDB = stats.RMSLevDB
	}

	// Вставляем
	newID, err := s.db.InsertMerged(newFile, parentIDs)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	// Собираем IDs из files
	ids := make([]int64, len(files))
	for i, f := range files {
		ids[i] = f.ID
	}

	// Помечаем исходные
	s.db.UpdateMergedID(ids, newID)
	s.db.DeactivateFiles(ids)

	return &MergeResult{
		NewID:         newID,
		OutputPath:    outputPath,
		TransPath:     transPath,
		Duration:      meta.DurationSec,
		Transcription: mergedTranscription,
		ParentIDs:     parentIDs,
		ChapterID:     nextChapterID,
	}, nil
}

// ========================================
// Queue Processing
// ========================================

// ProcessMergeQueue обрабатывает очередь merge
func (s *MergeService) ProcessMergeQueue(limit int) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return fmt.Errorf("merge queue already processing")
	}

	atomic.StoreInt64(&s.processed, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt32(&s.stopFlag, 0)
	s.startTime = time.Now()

	go s.runQueue(limit)
	return nil
}

func (s *MergeService) StopQueue() {
	atomic.StoreInt32(&s.stopFlag, 1)
}

func (s *MergeService) QueueStatus() MergeQueueStatus {
	return MergeQueueStatus{
		Running:   atomic.LoadInt32(&s.running) == 1,
		Total:     atomic.LoadInt64(&s.total),
		Processed: atomic.LoadInt64(&s.processed),
		Errors:    atomic.LoadInt64(&s.errors),
		Elapsed:   time.Since(s.startTime).Round(time.Second).String(),
	}
}

func (s *MergeService) runQueue(limit int) {
	defer atomic.StoreInt32(&s.running, 0)

	items, err := s.db.GetPendingMergeQueue(limit)
	if err != nil {
		log.Printf("Merge queue error: %v", err)
		return
	}

	if len(items) == 0 {
		log.Println("Merge queue: no pending items")
		return
	}

	atomic.StoreInt64(&s.total, int64(len(items)))
	log.Printf("Merge queue: processing %d items", len(items))

	for _, item := range items {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			break
		}

		s.processQueueItem(item)
	}

	log.Printf("Merge queue complete: processed=%d errors=%d",
		atomic.LoadInt64(&s.processed),
		atomic.LoadInt64(&s.errors))
}

func (s *MergeService) processQueueItem(item db.MergeQueueItem) {
	// Помечаем как processing
	s.db.UpdateMergeQueueStatus(item.ID, "processing")

	// Парсим IDs
	ids, err := db.ParseMergeIDs(item.IDsString)
	if err != nil {
		s.db.UpdateMergeQueueError(item.ID, err.Error())
		atomic.AddInt64(&s.errors, 1)
		return
	}

	// Проверяем файлы
	_, err = s.db.CheckFilesForMerge(ids)
	if err != nil {
		s.db.UpdateMergeQueueError(item.ID, err.Error())
		atomic.AddInt64(&s.errors, 1)
		return
	}

	// Выполняем merge
	result, err := s.MergeFiles(ids, s.outputDir)
	if err != nil {
		s.db.UpdateMergeQueueError(item.ID, err.Error())
		atomic.AddInt64(&s.errors, 1)
		return
	}

	// Успех
	s.db.UpdateMergeQueueCompleted(item.ID, result.NewID, result.OutputPath, result.Duration, result.Transcription)
	atomic.AddInt64(&s.processed, 1)

	log.Printf("Merged queue item %d -> file %d (%.2fs)", item.ID, result.NewID, result.Duration)
}

// AddToQueue добавляет строку в очередь и возвращает ID
func (s *MergeService) AddToQueue(idsString string) (int64, error) {
	// Валидация
	ids, err := db.ParseMergeIDs(idsString)
	if err != nil {
		return 0, err
	}

	_, err = s.db.CheckFilesForMerge(ids)
	if err != nil {
		return 0, err
	}

	return s.db.AddToMergeQueue(idsString)
}

// ProcessSingleFromString обрабатывает одну строку сразу (без очереди)
func (s *MergeService) ProcessSingleFromString(idsString string) (*MergeResult, error) {
	ids, err := db.ParseMergeIDs(idsString)
	if err != nil {
		return nil, err
	}

	return s.MergeFiles(ids, s.outputDir)
}
