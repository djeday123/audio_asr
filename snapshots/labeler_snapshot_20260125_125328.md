# Perl Compiler Snapshot - Sun Jan 25 12:53:28 PM UTC 2026

## Project Structure
```
./cmd/main.go
./internal/api/handlers.go
./internal/api/kaldi_asr.go
./internal/api/router.go
./internal/api/whisper_asr.go
./internal/asr/kaldi.go
./internal/asr/whisper.go
./internal/audio/hash.go
./internal/audio/metadata.go
./internal/audio/silence.go
./internal/audio/stats.go
./internal/audio/wada.go
./internal/config/config.go
./internal/db/asr.go
./internal/db/mariadb.go
./internal/db/merge.go
./internal/db/whisper.go
./internal/metrics/wer.go
./internal/scanner/librispeech.go
./internal/service/asr.go
./internal/service/asr_gpu.go
./internal/service/asr_gpu_nolm.go
./internal/service/asr_nolm.go
./internal/service/merge.go
./internal/service/scanner.go
./internal/service/whisper_local.go
./internal/service/whisper_openai.go
./scripts/create_snapshots.sh
```

## File: ./cmd/main.go
```go
package main

import (
	"flag"
	"log"
	"net/http"

	"audio-labeler/internal/api"
	"audio-labeler/internal/config"
	"audio-labeler/internal/db"
)

func main() {
	envFile := flag.String("env", ".env", "path to .env file")
	flag.Parse()

	// Load config
	cfg, err := config.Load(*envFile)
	if err != nil {
		log.Fatalf("Config error: %v", err)
	}

	// Database
	database, err := db.New(
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Name,
	)
	if err != nil {
		log.Fatalf("DB error: %v", err)
	}
	defer database.Close()
	log.Println("✓ Connected to MariaDB")

	// Router (создаёт все сервисы внутри)
	router := api.NewRouter(cfg, database)

	// Start
	log.Printf("✓ Server starting on %s", cfg.Server.Addr)
	log.Printf("  Data dir: %s", cfg.Data.Dir)
	printEndpoints()

	if err := http.ListenAndServe(cfg.Server.Addr, router); err != nil {
		log.Fatal(err)
	}
}

func printEndpoints() {
	log.Println("\nEndpoints:")
	log.Println("  POST /api/scan/start")
	log.Println("  POST /api/asr/start")
	log.Println("  POST /api/whisper-local/start")
	log.Println("  POST /api/whisper-openai/start")
	log.Println("  POST /api/whisper-openai/start-forced")
	log.Println("  GET  /api/stats")
	log.Println("  GET  /api/files")
	log.Println("  GET  /")
}
```

## File: ./internal/api/handlers.go
```go
package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"audio-labeler/internal/audio"
	"audio-labeler/internal/db"
	"audio-labeler/internal/metrics"
	"audio-labeler/internal/service"
)

type Handlers struct {
	db            *db.DB
	scanner       *service.Scanner
	asr           *service.ASRService
	asrNoLM       *service.ASRNoLMService
	asrGPU        *service.ASRGPUService
	asrGPUNoLM    *service.ASRGPUNoLMService
	whisperLocal  *service.WhisperLocalService
	whisperOpenAI *service.WhisperOpenAIService
	mergeService  *service.MergeService
}

func NewHandlers(db *db.DB, scanner *service.Scanner, asr *service.ASRService, asrNoLM *service.ASRNoLMService,
	asrGPU *service.ASRGPUService, asrGPUNoLM *service.ASRGPUNoLMService,
	whisperLocal *service.WhisperLocalService, whisperOpenAI *service.WhisperOpenAIService, mergeService *service.MergeService) *Handlers {
	return &Handlers{
		db:            db,
		scanner:       scanner,
		asr:           asr,
		asrNoLM:       asrNoLM,
		asrGPU:        asrGPU,
		asrGPUNoLM:    asrGPUNoLM,
		whisperLocal:  whisperLocal,
		whisperOpenAI: whisperOpenAI,
		mergeService:  mergeService,
	}
}

type Response struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func (h *Handlers) json(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *Handlers) success(w http.ResponseWriter, data interface{}) {
	h.json(w, http.StatusOK, Response{Success: true, Data: data})
}

func (h *Handlers) error(w http.ResponseWriter, status int, msg string) {
	h.json(w, status, Response{Success: false, Error: msg})
}

// === Scan handlers ===

func (h *Handlers) ScanStart(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	workers, _ := strconv.Atoi(r.URL.Query().Get("workers"))

	err := h.scanner.Start(limit, workers)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message": "Scan started",
		"limit":   limit,
		"workers": workers,
	})
}

func (h *Handlers) ScanStatus(w http.ResponseWriter, r *http.Request) {
	h.success(w, h.scanner.Status())
}

func (h *Handlers) ScanStop(w http.ResponseWriter, r *http.Request) {
	h.scanner.Stop()
	h.success(w, "Scan stopped")
}

// === Stats handler ===
func (h *Handlers) Stats(w http.ResponseWriter, r *http.Request) {
	extended, err := h.db.StatsExtendedCached()
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	avgMetrics, _ := h.db.AvgMetricsAll()

	result := map[string]interface{}{
		"total":                    extended["total"],
		"pending":                  extended["pending"],
		"processed":                extended["processed"],
		"errors":                   extended["errors"],
		"verified":                 extended["verified"],
		"needs_review":             extended["needs_review"],
		"pending_nolm":             extended["pending_nolm"],
		"processed_nolm":           extended["processed_nolm"],
		"pending_whisper_local":    extended["pending_whisper_local"],
		"processed_whisper_local":  extended["processed_whisper_local"],
		"pending_whisper_openai":   extended["pending_whisper_openai"],
		"processed_whisper_openai": extended["processed_whisper_openai"],
		"kaldi_wer":                avgMetrics["kaldi_wer"],
		"kaldi_cer":                avgMetrics["kaldi_cer"],
		"kaldi_nolm_wer":           avgMetrics["kaldi_nolm_wer"],
		"kaldi_nolm_cer":           avgMetrics["kaldi_nolm_cer"],
		"whisper_local_wer":        avgMetrics["whisper_local_wer"],
		"whisper_local_cer":        avgMetrics["whisper_local_cer"],
		"whisper_openai_wer":       avgMetrics["whisper_openai_wer"],
		"whisper_openai_cer":       avgMetrics["whisper_openai_cer"],
	}

	h.success(w, result)
}

// === Files handlers ===
func (h *Handlers) FilesList(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 5000 {
		limit = 50
	}

	speaker := r.URL.Query().Get("speaker")
	werOp := r.URL.Query().Get("wer_op")
	werValue, _ := strconv.ParseFloat(r.URL.Query().Get("wer_value"), 64)
	durOp := r.URL.Query().Get("dur_op")
	durValue, _ := strconv.ParseFloat(r.URL.Query().Get("dur_value"), 64)

	asrStatus := r.URL.Query().Get("asr_status")
	asrNoLMStatus := r.URL.Query().Get("asr_nolm_status") // <-- НОВЫЙ
	whisperLocalStatus := r.URL.Query().Get("whisper_local_status")
	whisperOpenaiStatus := r.URL.Query().Get("whisper_openai_status")
	verified := r.URL.Query().Get("verified") // <-- НОВЫЙ
	merged := r.URL.Query().Get("merged")
	active := r.URL.Query().Get("active")

	result, err := h.db.GetFilesFiltered(page, limit, speaker, werOp, werValue, durOp, durValue,
		asrStatus, asrNoLMStatus, whisperLocalStatus, whisperOpenaiStatus, verified, merged, active)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, result)
}

func (h *Handlers) FilesGet(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	file, err := h.db.GetFile(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	h.success(w, file)
}

// === Health handler ===

func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	asrOK := h.asr != nil
	h.success(w, map[string]interface{}{
		"status": "ok",
		"asr":    asrOK,
	})
}

// === Test/Debug handlers ===

func (h *Handlers) TestAudioStats(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		h.error(w, http.StatusBadRequest, "path parameter required")
		return
	}

	stats, err := audio.GetStats(path)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	quality := stats.Quality()

	h.success(w, map[string]interface{}{
		"path": path,
		"snr": map[string]float64{
			"sox":      stats.SNRSox,
			"spectral": stats.SNRSpectral,
			"vad":      stats.SNRVad,
			"wada":     stats.SNRWada,
			"estimate": stats.SNREstimate,
		},
		"noise_level": stats.NoiseLevel,
		"rms_db":      stats.RMSLevDB,
		"duration":    stats.LengthSec,
		"quality":     quality,
	})
}

// ServeAudio отдает аудио файл
func (h *Handlers) ServeAudio(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	file, err := h.db.GetFile(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	w.Header().Set("Content-Type", "audio/wav")
	w.Header().Set("Accept-Ranges", "bytes")
	http.ServeFile(w, r, file.FilePath)
}

// ServeWeb отдает веб интерфейс
func (h *Handlers) ServeWeb(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/index.html")
}

func (h *Handlers) SpeakersList(w http.ResponseWriter, r *http.Request) {
	speakers, err := h.db.GetSpeakers()
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.success(w, speakers)
}

func (h *Handlers) ProcessFile(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	target := r.URL.Query().Get("target")

	file, err := h.db.GetFile(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	var result interface{}

	switch target {
	case "kaldi":
		if h.asr == nil {
			h.error(w, http.StatusServiceUnavailable, "Kaldi not configured")
			return
		}
		go h.processFileKaldi(file)
		result = "Kaldi processing started"

	case "kaldi-nolm":
		if h.asrNoLM == nil {
			h.error(w, http.StatusServiceUnavailable, "Kaldi NoLM not configured")
			return
		}
		go h.processFileKaldiNoLM(file)
		result = "Kaldi NoLM processing started"

	case "whisper-local":
		if h.whisperLocal == nil {
			h.error(w, http.StatusServiceUnavailable, "Whisper Local not configured")
			return
		}
		go h.processFileWhisperLocal(file)
		result = "Whisper Local processing started"

	case "whisper-openai":
		if h.whisperOpenAI == nil {
			h.error(w, http.StatusServiceUnavailable, "Whisper OpenAI not configured")
			return
		}
		go h.processFileWhisperOpenAI(file)
		result = "Whisper OpenAI processing started"

	default:
		h.error(w, http.StatusBadRequest, "invalid target: kaldi, whisper-local, whisper-openai")
		return
	}

	h.success(w, result)
}

func (h *Handlers) RecalcWER(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	file, err := h.db.GetFileForRecalc(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	updated := h.recalcFile(file)

	h.success(w, map[string]interface{}{
		"id":      id,
		"updated": updated,
	})
}

func (h *Handlers) RecalcAll(w http.ResponseWriter, r *http.Request) {
	files, err := h.db.GetAllForRecalc()
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	count := 0
	for _, file := range files {
		if h.recalcFile(&file) {
			count++
		}
	}

	h.success(w, map[string]interface{}{
		"count": count,
	})
}

func (h *Handlers) recalcFile(file *db.AudioFileRecalc) bool {
	updated := false

	// Kaldi
	if file.TranscriptionASR != "" {
		wer := metrics.WER(file.TranscriptionOriginal, file.TranscriptionASR)
		cer := metrics.CER(file.TranscriptionOriginal, file.TranscriptionASR)
		h.db.UpdateASRMetrics(file.ID, wer, cer)
		updated = true
	}

	// Kaldi NoLM  <-- ДОБАВИТЬ ЭТО!
	if file.TranscriptionASRNoLM != "" {
		wer := metrics.WER(file.TranscriptionOriginal, file.TranscriptionASRNoLM)
		cer := metrics.CER(file.TranscriptionOriginal, file.TranscriptionASRNoLM)
		h.db.UpdateASRNoLMMetrics(file.ID, wer, cer)
		updated = true
	}

	// Whisper Local
	if file.TranscriptionWhisperLocal != "" {
		wer := metrics.WER(file.TranscriptionOriginal, file.TranscriptionWhisperLocal)
		cer := metrics.CER(file.TranscriptionOriginal, file.TranscriptionWhisperLocal)
		h.db.UpdateWhisperLocalMetrics(file.ID, wer, cer)
		updated = true
	}

	// Whisper OpenAI
	if file.TranscriptionWhisperOpenAI != "" {
		wer := metrics.WER(file.TranscriptionOriginal, file.TranscriptionWhisperOpenAI)
		cer := metrics.CER(file.TranscriptionOriginal, file.TranscriptionWhisperOpenAI)
		h.db.UpdateWhisperOpenAIMetrics(file.ID, wer, cer)
		updated = true
	}

	return updated
}

// ============================================================
// Редактирование транскрипции
// ============================================================

func (h *Handlers) UpdateTranscription(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	var req struct {
		Transcription string `json:"transcription"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid json")
		return
	}

	// Получаем файл для пересчёта WER
	file, err := h.db.GetFile(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	// Обновляем транскрипцию
	err = h.db.UpdateOriginalTranscription(id, req.Transcription)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Пересчитываем WER для всех ASR систем
	if file.TranscriptionASR != "" {
		wer := metrics.WER(req.Transcription, file.TranscriptionASR)
		cer := metrics.CER(req.Transcription, file.TranscriptionASR)
		h.db.UpdateASRMetrics(id, wer, cer)
	}

	if file.TranscriptionASRNoLM != "" {
		wer := metrics.WER(req.Transcription, file.TranscriptionASRNoLM)
		cer := metrics.CER(req.Transcription, file.TranscriptionASRNoLM)
		h.db.UpdateASRNoLMMetrics(id, wer, cer)
	}

	if file.TranscriptionWhisperLocal != "" {
		wer := metrics.WER(req.Transcription, file.TranscriptionWhisperLocal)
		cer := metrics.CER(req.Transcription, file.TranscriptionWhisperLocal)
		h.db.UpdateWhisperLocalMetrics(id, wer, cer)
	}

	if file.TranscriptionWhisperOpenAI != "" {
		wer := metrics.WER(req.Transcription, file.TranscriptionWhisperOpenAI)
		cer := metrics.CER(req.Transcription, file.TranscriptionWhisperOpenAI)
		h.db.UpdateWhisperOpenAIMetrics(id, wer, cer)
	}

	h.success(w, map[string]interface{}{
		"id":          id,
		"wer_updated": true,
	})
}

// ============================================================
// Верификация
// ============================================================

func (h *Handlers) VerifyFile(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	err = h.db.SetVerificationStatus(id, true)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"id":       id,
		"verified": true,
	})
}

func (h *Handlers) UnverifyFile(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	err = h.db.SetVerificationStatus(id, false)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"id":       id,
		"verified": false,
	})
}

// ============================================================
// MERGE
// ============================================================

func (h *Handlers) MergeFiles(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs       []int64 `json:"ids"`
		OutputDir string  `json:"output_dir"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid json")
		return
	}

	if len(req.IDs) < 2 {
		h.error(w, http.StatusBadRequest, "need at least 2 files")
		return
	}

	result, err := h.mergeService.MergeFiles(req.IDs, req.OutputDir)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, result)
}

// ============================================================
// SILENCE DETECTION
// ============================================================

func (h *Handlers) CheckSilence(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	file, err := h.db.GetFile(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	info, err := audio.DetectTrailingSilence(file.FilePath, 100) // 100ms minimum
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Обновляем статус в БД
	h.db.UpdateSilenceStatus(id, info.HasTrailingSilence, false)

	h.success(w, info)
}

func (h *Handlers) AddSilence(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	file, err := h.db.GetFile(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	// Создаём новый файл с тишиной
	ext := filepath.Ext(file.FilePath)
	base := strings.TrimSuffix(file.FilePath, ext)
	newPath := base + "_sil" + ext

	if err := audio.AddTrailingSilence(file.FilePath, newPath, 100); err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Получаем новые метаданные
	meta, _ := audio.GetMetadata(newPath)
	hash, _ := audio.MD5File(newPath)

	// Обновляем БД
	h.db.UpdateFilePath(id, newPath, meta.DurationSec, hash)
	h.db.UpdateSilenceStatus(id, true, true)

	h.success(w, map[string]interface{}{
		"new_path":     newPath,
		"new_duration": meta.DurationSec,
	})
}

func (h *Handlers) RemoveSilence(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	file, err := h.db.GetFile(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	// Создаём файл без тишины
	ext := filepath.Ext(file.FilePath)
	base := strings.TrimSuffix(file.FilePath, ext)
	// Убираем _sil если есть
	base = strings.TrimSuffix(base, "_sil")
	newPath := base + "_trimmed" + ext

	if err := audio.RemoveTrailingSilence(file.FilePath, newPath); err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	meta, _ := audio.GetMetadata(newPath)
	hash, _ := audio.MD5File(newPath)

	h.db.UpdateFilePath(id, newPath, meta.DurationSec, hash)
	h.db.UpdateSilenceStatus(id, false, false)

	h.success(w, map[string]interface{}{
		"new_path":     newPath,
		"new_duration": meta.DurationSec,
	})
}

// GetShortFiles возвращает короткие файлы сгруппированные по спикеру
func (h *Handlers) GetShortFiles(w http.ResponseWriter, r *http.Request) {
	maxDur, _ := strconv.ParseFloat(r.URL.Query().Get("max_duration"), 64)
	if maxDur <= 0 {
		maxDur = 1.0 // по умолчанию < 1 сек
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	files, err := h.db.GetShortFilesBySpeaker(maxDur, limit)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, files)
}
```

## File: ./internal/api/kaldi_asr.go
```go
package api

import (
	"audio-labeler/internal/db"
	"audio-labeler/internal/metrics"
	"log"
	"net/http"
	"strconv"
)

// ============================================================
// === ASR handlers ===
// ============================================================

func (h *Handlers) ASRStart(w http.ResponseWriter, r *http.Request) {
	if h.asr == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR service not available")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	workers, _ := strconv.Atoi(r.URL.Query().Get("workers"))

	if workers <= 0 {
		workers = 5
	}

	err := h.asr.Start(limit, workers)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message": "ASR started",
		"limit":   limit,
		"workers": workers,
	})
}

func (h *Handlers) ASRStatus(w http.ResponseWriter, r *http.Request) {
	if h.asr == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR service not available")
		return
	}
	h.success(w, h.asr.Status())
}

func (h *Handlers) ASRStop(w http.ResponseWriter, r *http.Request) {
	if h.asr == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR service not available")
		return
	}
	h.asr.Stop()
	h.success(w, "ASR stopped")
}

// ============================================================
// ASR NoLM
// ============================================================

func (h *Handlers) ASRNoLMStart(w http.ResponseWriter, r *http.Request) {
	if h.asrNoLM == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR NoLM service not available")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	workers, _ := strconv.Atoi(r.URL.Query().Get("workers"))

	if workers <= 0 {
		workers = 5
	}

	err := h.asrNoLM.Start(limit, workers)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message": "ASR NoLM started (lm-scale=0)",
		"limit":   limit,
		"workers": workers,
	})
}

func (h *Handlers) ASRNoLMStatus(w http.ResponseWriter, r *http.Request) {
	if h.asrNoLM == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR NoLM service not available")
		return
	}
	h.success(w, h.asrNoLM.Status())
}

func (h *Handlers) ASRNoLMStop(w http.ResponseWriter, r *http.Request) {
	if h.asrNoLM == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR NoLM service not available")
		return
	}
	h.asrNoLM.Stop()
	h.success(w, "ASR NoLM stopped")
}

// ============================================================
// ASR GPU handlers
// ============================================================

func (h *Handlers) ASRGPUStart(w http.ResponseWriter, r *http.Request) {
	if h.asrGPU == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR GPU service not available")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	batchSize, _ := strconv.Atoi(r.URL.Query().Get("batch_size"))
	if batchSize <= 0 {
		batchSize = 32
	}

	err := h.asrGPU.Start(limit)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message":    "ASR GPU started",
		"limit":      limit,
		"batch_size": batchSize,
	})
}

func (h *Handlers) ASRGPUStatus(w http.ResponseWriter, r *http.Request) {
	if h.asrGPU == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR GPU service not available")
		return
	}
	h.success(w, h.asrGPU.Status())
}

func (h *Handlers) ASRGPUStop(w http.ResponseWriter, r *http.Request) {
	if h.asrGPU == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR GPU service not available")
		return
	}
	h.asrGPU.Stop()
	h.success(w, "ASR GPU stopped")
}

// ============================================================
// ASR GPU NoLM handlers
// ============================================================

func (h *Handlers) ASRGPUNoLMStart(w http.ResponseWriter, r *http.Request) {
	if h.asrGPUNoLM == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR GPU NoLM service not available")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	batchSize, _ := strconv.Atoi(r.URL.Query().Get("batch_size"))
	if batchSize <= 0 {
		batchSize = 32
	}

	err := h.asrGPUNoLM.Start(limit)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message":    "ASR GPU NoLM started (lm-scale=0)",
		"limit":      limit,
		"batch_size": batchSize,
	})
}

func (h *Handlers) ASRGPUNoLMStatus(w http.ResponseWriter, r *http.Request) {
	if h.asrGPUNoLM == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR GPU NoLM service not available")
		return
	}
	h.success(w, h.asrGPUNoLM.Status())
}

func (h *Handlers) ASRGPUNoLMStop(w http.ResponseWriter, r *http.Request) {
	if h.asrGPUNoLM == nil {
		h.error(w, http.StatusServiceUnavailable, "ASR GPU NoLM service not available")
		return
	}
	h.asrGPUNoLM.Stop()
	h.success(w, "ASR GPU NoLM stopped")
}

func (h *Handlers) processFileKaldi(file *db.AudioFile) {
	result, err := h.asr.ProcessSingle(file.FilePath)
	if err != nil {
		h.db.UpdateError(file.ID, err.Error())
		return
	}
	wer := metrics.WER(file.TranscriptionOriginal, result)
	cer := metrics.CER(file.TranscriptionOriginal, result)
	h.db.UpdateASR(file.ID, result, wer, cer)
}

func (h *Handlers) processFileKaldiNoLM(file *db.AudioFile) {
	log.Printf("NoLM processing file ID=%d path=%s", file.ID, file.FilePath)

	result, err := h.asrNoLM.ProcessSingle(file.FilePath)
	if err != nil {
		log.Printf("NoLM ERROR ID=%d: %v", file.ID, err)
		h.db.UpdateASRNoLMError(file.ID, err.Error())
		return
	}
	wer := metrics.WER(file.TranscriptionOriginal, result)
	cer := metrics.CER(file.TranscriptionOriginal, result)

	log.Printf("NoLM OK ID=%d WER=%.2f%% text=%s", file.ID, wer*100, result[:min(500, len(result))])

	h.db.UpdateASRNoLM(file.ID, result, wer, cer)
}
```

## File: ./internal/api/router.go
```go
package api

import (
	"log"
	"net/http"
	"time"

	"audio-labeler/internal/config"
	"audio-labeler/internal/db"
	"audio-labeler/internal/service"
)

type Router struct {
	mux      *http.ServeMux
	handlers *Handlers
}

func NewRouter(cfg *config.Config, database *db.DB) *Router {
	// Scanner
	scanner := service.NewScanner(database, cfg.Data.Dir, cfg.Workers.Scan)
	log.Printf("✓ Scanner: %s (workers=%d)", cfg.Data.Dir, cfg.Workers.Scan)

	// Kaldi ASR
	var asrService *service.ASRService
	if cfg.Kaldi.ModelDir != "" {
		var err error
		asrService, err = service.NewASRService(database, cfg.Kaldi.ModelDir, cfg.Workers.ASR)
		if err != nil {
			log.Printf("⚠ Kaldi ASR error: %v", err)
		} else {
			log.Printf("✓ Kaldi ASR: %s (workers=%d)", cfg.Kaldi.ModelDir, cfg.Workers.ASR)
		}
	}

	// Kaldi ASR NoLM (без LM, lm-scale=0)
	var asrNoLMService *service.ASRNoLMService
	if cfg.Kaldi.ModelDir != "" {
		var err error
		asrNoLMService, err = service.NewASRNoLMService(database, cfg.Kaldi.ModelDir, cfg.Workers.ASR)
		if err != nil {
			log.Printf("⚠ Kaldi ASR NoLM error: %v", err)
		} else {
			log.Printf("✓ Kaldi ASR NoLM: %s (lm-scale=0)", cfg.Kaldi.ModelDir)
		}
	}

	// Kaldi ASR GPU
	var asrGPUService *service.ASRGPUService
	if cfg.Kaldi.ModelDir != "" {
		var err error
		asrGPUService, err = service.NewASRGPUService(database, cfg.Kaldi.ModelDir, 32)
		if err != nil {
			log.Printf("⚠ Kaldi ASR GPU error: %v", err)
		} else {
			log.Printf("✓ Kaldi ASR GPU: %s (batch_size=32)", cfg.Kaldi.ModelDir)
		}
	}

	// Kaldi ASR GPU NoLM
	var asrGPUNoLMService *service.ASRGPUNoLMService
	if cfg.Kaldi.ModelDir != "" {
		var err error
		asrGPUNoLMService, err = service.NewASRGPUNoLMService(database, cfg.Kaldi.ModelDir, 32)
		if err != nil {
			log.Printf("⚠ Kaldi ASR GPU NoLM error: %v", err)
		} else {
			log.Printf("✓ Kaldi ASR GPU NoLM: %s (batch_size=32, lm-scale=0)", cfg.Kaldi.ModelDir)
		}
	}

	// Whisper Local
	var whisperLocal *service.WhisperLocalService
	if cfg.Whisper.LocalURL != "" {
		whisperLocal = service.NewWhisperLocalService(database, cfg.Whisper.LocalURL, cfg.Whisper.Lang, 3)
		log.Printf("✓ Whisper Local: %s", cfg.Whisper.LocalURL)
	}

	// Whisper OpenAI
	var whisperOpenAI *service.WhisperOpenAIService
	if cfg.Whisper.OpenAIKey != "" {
		whisperOpenAI = service.NewWhisperOpenAIService(database, cfg.Whisper.OpenAIKey, cfg.Whisper.OpenAIModel, cfg.Whisper.Lang, 3)
		log.Println("✓ Whisper OpenAI configured")
	}

	// Merge Service  <-- ДОБАВЬ ЭТО
	mergeOutputDir := "/data/merged" // или cfg.Data.Dir + "/merged"
	mergeService := service.NewMergeService(database, mergeOutputDir)
	log.Printf("✓ Merge Service: output to %s", mergeOutputDir)

	r := &Router{
		mux:      http.NewServeMux(),
		handlers: NewHandlers(database, scanner, asrService, asrNoLMService, asrGPUService, asrGPUNoLMService, whisperLocal, whisperOpenAI, mergeService),
	}

	r.setupRoutes()
	return r
}

func (r *Router) setupRoutes() {

	r.mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))
	// Health
	r.mux.HandleFunc("GET /api/health", r.handlers.Health)

	// Stats
	r.mux.HandleFunc("GET /api/stats", r.handlers.Stats)
	r.mux.HandleFunc("GET /api/test/audio-stats", r.handlers.TestAudioStats)
	r.mux.HandleFunc("GET /api/speakers", r.handlers.SpeakersList)

	// Scan
	r.mux.HandleFunc("POST /api/scan/start", r.handlers.ScanStart)
	r.mux.HandleFunc("GET /api/scan/status", r.handlers.ScanStatus)
	r.mux.HandleFunc("POST /api/scan/stop", r.handlers.ScanStop)

	// ASR
	r.mux.HandleFunc("POST /api/asr/start", r.handlers.ASRStart)
	r.mux.HandleFunc("GET /api/asr/status", r.handlers.ASRStatus)
	r.mux.HandleFunc("POST /api/asr/stop", r.handlers.ASRStop)

	// ASR NoLM (Kaldi без LM)
	r.mux.HandleFunc("POST /api/asr-nolm/start", r.handlers.ASRNoLMStart)
	r.mux.HandleFunc("GET /api/asr-nolm/status", r.handlers.ASRNoLMStatus)
	r.mux.HandleFunc("POST /api/asr-nolm/stop", r.handlers.ASRNoLMStop)

	// ASR GPU
	r.mux.HandleFunc("POST /api/asr-gpu/start", r.handlers.ASRGPUStart)
	r.mux.HandleFunc("GET /api/asr-gpu/status", r.handlers.ASRGPUStatus)
	r.mux.HandleFunc("POST /api/asr-gpu/stop", r.handlers.ASRGPUStop)

	// ASR GPU NoLM
	r.mux.HandleFunc("POST /api/asr-gpu-nolm/start", r.handlers.ASRGPUNoLMStart)
	r.mux.HandleFunc("GET /api/asr-gpu-nolm/status", r.handlers.ASRGPUNoLMStatus)
	r.mux.HandleFunc("POST /api/asr-gpu-nolm/stop", r.handlers.ASRGPUNoLMStop)

	// Whisper Local
	r.mux.HandleFunc("POST /api/whisper-local/start", r.handlers.WhisperLocalStart)
	r.mux.HandleFunc("GET /api/whisper-local/status", r.handlers.WhisperLocalStatus)
	r.mux.HandleFunc("POST /api/whisper-local/stop", r.handlers.WhisperLocalStop)

	// Whisper OpenAI
	r.mux.HandleFunc("POST /api/whisper-openai/start", r.handlers.WhisperOpenAIStart)
	r.mux.HandleFunc("POST /api/whisper-openai/start-forced", r.handlers.WhisperOpenAIStartForced)
	r.mux.HandleFunc("GET /api/whisper-openai/status", r.handlers.WhisperOpenAIStatus)
	r.mux.HandleFunc("POST /api/whisper-openai/stop", r.handlers.WhisperOpenAIStop)

	// Data files
	r.mux.HandleFunc("GET /api/files", r.handlers.FilesList)
	r.mux.HandleFunc("GET /api/files/{id}", r.handlers.FilesGet)
	r.mux.HandleFunc("GET /api/audio/{id}", r.handlers.ServeAudio)

	// Edit transcription (редактирование оригинала)
	r.mux.HandleFunc("PUT /api/files/{id}/transcription", r.handlers.UpdateTranscription)

	// Verification (верификация оператором)
	r.mux.HandleFunc("POST /api/files/{id}/verify", r.handlers.VerifyFile)
	r.mux.HandleFunc("POST /api/files/{id}/unverify", r.handlers.UnverifyFile)

	// Process single file
	r.mux.HandleFunc("POST /api/process/{id}", r.handlers.ProcessFile)

	// Recalc
	r.mux.HandleFunc("POST /api/recalc/{id}", r.handlers.RecalcWER)
	r.mux.HandleFunc("POST /api/recalc-all", r.handlers.RecalcAll)

	// Merge
	r.mux.HandleFunc("POST /api/merge", r.handlers.MergeFiles)
	r.mux.HandleFunc("GET /api/short-files", r.handlers.GetShortFiles)

	// Silence
	r.mux.HandleFunc("GET /api/files/{id}/silence", r.handlers.CheckSilence)
	r.mux.HandleFunc("POST /api/files/{id}/add-silence", r.handlers.AddSilence)
	r.mux.HandleFunc("POST /api/files/{id}/remove-silence", r.handlers.RemoveSilence)

	// Other
	r.mux.HandleFunc("GET /", r.handlers.ServeWeb)
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if req.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Логируем запрос
	log.Printf("→ %s %s %s", req.Method, req.URL.Path, req.URL.RawQuery)

	r.mux.ServeHTTP(w, req)

	// Логируем время выполнения
	log.Printf("← %s %s [%v]", req.Method, req.URL.Path, time.Since(start))
}
```

## File: ./internal/api/whisper_asr.go
```go
package api

import (
	"audio-labeler/internal/db"
	"audio-labeler/internal/metrics"
	"net/http"
	"strconv"
)

// === Whisper Local ===

func (h *Handlers) WhisperLocalStart(w http.ResponseWriter, r *http.Request) {
	if h.whisperLocal == nil {
		h.error(w, http.StatusServiceUnavailable, "Whisper Local not configured")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	workers, _ := strconv.Atoi(r.URL.Query().Get("workers"))
	if workers <= 0 {
		workers = 3
	}

	err := h.whisperLocal.Start(limit, workers)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message": "Whisper Local started",
		"limit":   limit,
		"workers": workers,
	})
}

func (h *Handlers) WhisperLocalStatus(w http.ResponseWriter, r *http.Request) {
	if h.whisperLocal == nil {
		h.error(w, http.StatusServiceUnavailable, "Whisper Local not configured")
		return
	}
	h.success(w, h.whisperLocal.Status())
}

func (h *Handlers) WhisperLocalStop(w http.ResponseWriter, r *http.Request) {
	if h.whisperLocal == nil {
		h.error(w, http.StatusServiceUnavailable, "Whisper Local not configured")
		return
	}
	h.whisperLocal.Stop()
	h.success(w, "Whisper Local stopped")
}

// === Whisper OpenAI ===

func (h *Handlers) WhisperOpenAIStart(w http.ResponseWriter, r *http.Request) {
	if h.whisperOpenAI == nil {
		h.error(w, http.StatusServiceUnavailable, "Whisper OpenAI not configured (no API key)")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	workers, _ := strconv.Atoi(r.URL.Query().Get("workers"))
	if workers <= 0 {
		workers = 3
	}

	// min_wer - минимальный WER от локального Whisper (по умолчанию 0 = все с ошибками)
	minWER := 0.0
	if w := r.URL.Query().Get("min_wer"); w != "" {
		minWER, _ = strconv.ParseFloat(w, 64)
	}

	err := h.whisperOpenAI.Start(limit, workers, minWER)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message": "Whisper OpenAI started",
		"limit":   limit,
		"workers": workers,
		"min_wer": minWER,
	})
}

func (h *Handlers) WhisperOpenAIStatus(w http.ResponseWriter, r *http.Request) {
	if h.whisperOpenAI == nil {
		h.error(w, http.StatusServiceUnavailable, "Whisper OpenAI not configured")
		return
	}
	h.success(w, h.whisperOpenAI.Status())
}

func (h *Handlers) WhisperOpenAIStop(w http.ResponseWriter, r *http.Request) {
	if h.whisperOpenAI == nil {
		h.error(w, http.StatusServiceUnavailable, "Whisper OpenAI not configured")
		return
	}
	h.whisperOpenAI.Stop()
	h.success(w, "Whisper OpenAI stopped")
}

func (h *Handlers) WhisperOpenAIStartForced(w http.ResponseWriter, r *http.Request) {
	if h.whisperOpenAI == nil {
		h.error(w, http.StatusServiceUnavailable, "Whisper OpenAI not configured (no API key)")
		return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	workers, _ := strconv.Atoi(r.URL.Query().Get("workers"))
	if workers <= 0 {
		workers = 3
	}

	err := h.whisperOpenAI.StartForced(limit, workers)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message": "Whisper OpenAI FORCED started (no filter)",
		"limit":   limit,
		"workers": workers,
	})
}

func (h *Handlers) processFileWhisperLocal(file *db.AudioFile) {
	result, err := h.whisperLocal.ProcessSingle(file.FilePath)
	if err != nil {
		h.db.UpdateWhisperLocalError(file.ID, err.Error())
		return
	}
	wer := metrics.WER(file.TranscriptionOriginal, result)
	cer := metrics.CER(file.TranscriptionOriginal, result)
	h.db.UpdateWhisperLocal(file.ID, result, wer, cer)
}

func (h *Handlers) processFileWhisperOpenAI(file *db.AudioFile) {
	result, err := h.whisperOpenAI.ProcessSingle(file.FilePath)
	if err != nil {
		h.db.UpdateWhisperOpenAIError(file.ID, err.Error())
		return
	}
	wer := metrics.WER(file.TranscriptionOriginal, result)
	cer := metrics.CER(file.TranscriptionOriginal, result)
	h.db.UpdateWhisperOpenAI(file.ID, result, wer, cer)
}
```

## File: ./internal/asr/kaldi.go
```go
package asr

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type KaldiDecoder struct {
	kaldiRoot  string
	modelPath  string
	graphDir   string
	wordsTxt   string
	hclgFst    string
	onlineConf string
	lmScale    float64
}

type DecodeResult struct {
	Text           string
	Duration       float64
	ProcessingTime float64
	RTF            float64
	Success        bool
	Error          string
}

func NewKaldiDecoder(modelDir string) (*KaldiDecoder, error) {
	kaldiRoot := "/opt/kaldi"

	d := &KaldiDecoder{
		kaldiRoot:  kaldiRoot,
		modelPath:  filepath.Join(modelDir, "model/final.mdl"),
		graphDir:   filepath.Join(modelDir, "graph"),
		wordsTxt:   filepath.Join(modelDir, "graph/words.txt"),
		hclgFst:    filepath.Join(modelDir, "graph/HCLG.fst"),
		onlineConf: filepath.Join(modelDir, "conf/online.conf"),
		lmScale:    1.0,
	}

	files := []string{d.modelPath, d.wordsTxt, d.hclgFst, d.onlineConf}
	for _, f := range files {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			return nil, fmt.Errorf("missing file: %s", f)
		}
	}

	return d, nil
}

func NewKaldiDecoderNoLM(modelDir string) (*KaldiDecoder, error) {
	d, err := NewKaldiDecoder(modelDir)
	if err != nil {
		return nil, err
	}
	d.lmScale = 0.0
	return d, nil
}

func (d *KaldiDecoder) SetLMScale(scale float64) {
	d.lmScale = scale
}

func (d *KaldiDecoder) GetLMScale() float64 {
	return d.lmScale
}

// ============================================================
// Single file decoding
// ============================================================

func (d *KaldiDecoder) Decode(wavPath string) (*DecodeResult, error) {
	if _, err := os.Stat(wavPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("audio file not found: %s", wavPath)
	}

	duration, err := getAudioDuration(wavPath)
	if err != nil {
		return nil, fmt.Errorf("get duration: %w", err)
	}

	uttID := fmt.Sprintf("utt_%d", time.Now().UnixNano())

	// NoLM режим — используем lattice rescoring
	if d.lmScale == 0 {
		return d.decodeWithRescoring(wavPath, uttID, duration)
	}

	// Обычный режим — прямое декодирование
	return d.decodeDirect(wavPath, uttID, duration)
}

// decodeDirect — обычное декодирование с LM
func (d *KaldiDecoder) decodeDirect(wavPath, uttID string, duration float64) (*DecodeResult, error) {
	start := time.Now()

	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		fmt.Sprintf("ark:echo %s %s |", uttID, uttID),
		fmt.Sprintf("scp:echo %s %s |", uttID, wavPath),
		"ark:/dev/null",
	)

	output, err := cmd.CombinedOutput()
	elapsed := time.Since(start).Seconds()

	if err != nil {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("decode error: %v, output: %s", err, string(output)),
		}, nil
	}

	text := d.parseOutput(string(output), uttID)

	rtf := 0.0
	if duration > 0 {
		rtf = elapsed / duration
	}

	return &DecodeResult{
		Text:           text,
		Duration:       duration,
		ProcessingTime: elapsed,
		RTF:            rtf,
		Success:        true,
	}, nil
}

// decodeWithRescoring — декодирование с lattice rescoring (lm-scale=0)
func (d *KaldiDecoder) decodeWithRescoring(wavPath, uttID string, duration float64) (*DecodeResult, error) {
	start := time.Now()

	tmpDir, err := os.MkdirTemp("", "kaldi_nolm_")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	latticePath := filepath.Join(tmpDir, "lat.ark")

	// Шаг 1: Декодируем в lattice
	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd1 := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		fmt.Sprintf("ark:echo %s %s |", uttID, uttID),
		fmt.Sprintf("scp:echo %s %s |", uttID, wavPath),
		"ark:"+latticePath,
	)

	if output, err := cmd1.CombinedOutput(); err != nil {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("decode step failed: %v, output: %s", err, string(output)),
		}, nil
	}

	// Проверяем что lattice создан
	if fi, err := os.Stat(latticePath); err != nil || fi.Size() == 0 {
		return &DecodeResult{
			Success: false,
			Error:   "lattice file empty or not created",
		}, nil
	}

	// Шаг 2: Rescoring + Best Path + Convert to words
	rescoreBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-scale")
	bestPathBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-best-path")
	int2symPl := filepath.Join(d.kaldiRoot, "egs/wsj/s5/utils/int2sym.pl")

	// Проверяем наличие int2sym.pl
	if _, err := os.Stat(int2symPl); os.IsNotExist(err) {
		// Пробуем альтернативный путь
		int2symPl = filepath.Join(d.kaldiRoot, "egs/work_3/s5/utils/int2sym.pl")
	}

	cmd2 := exec.Command("bash", "-c", fmt.Sprintf(
		"%s --lm-scale=0.0 --acoustic-scale=1.0 'ark:%s' ark:- | %s ark:- ark,t:- | %s -f 2- %s",
		rescoreBin, latticePath, bestPathBin, int2symPl, d.wordsTxt,
	))

	output, err := cmd2.CombinedOutput()
	elapsed := time.Since(start).Seconds()

	if err != nil {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("rescore step failed: %v, output: %s", err, string(output)),
		}, nil
	}

	text := d.parseOutput(string(output), uttID)

	rtf := 0.0
	if duration > 0 {
		rtf = elapsed / duration
	}

	return &DecodeResult{
		Text:           text,
		Duration:       duration,
		ProcessingTime: elapsed,
		RTF:            rtf,
		Success:        true,
	}, nil
}

// ============================================================
// Batch decoding
// ============================================================

func (d *KaldiDecoder) DecodeBatch(wavPaths []string) (map[string]*DecodeResult, error) {
	if len(wavPaths) == 0 {
		return nil, nil
	}

	// NoLM режим — используем batch lattice rescoring
	if d.lmScale == 0 {
		return d.decodeBatchWithRescoring(wavPaths)
	}

	// Обычный режим — прямое batch декодирование
	return d.decodeBatchDirect(wavPaths)
}

// decodeBatchDirect — batch декодирование с LM
func (d *KaldiDecoder) decodeBatchDirect(wavPaths []string) (map[string]*DecodeResult, error) {
	results := make(map[string]*DecodeResult)

	tmpDir, err := os.MkdirTemp("", "kaldi_batch_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	wavScp := filepath.Join(tmpDir, "wav.scp")
	spk2utt := filepath.Join(tmpDir, "spk2utt")

	// Создаём wav.scp
	wavScpFile, err := os.Create(wavScp)
	if err != nil {
		return nil, err
	}

	uttIDs := make(map[string]string) // uttID -> wavPath
	var uttList []string

	for i, path := range wavPaths {
		uttID := fmt.Sprintf("utt%06d", i)
		uttIDs[uttID] = path
		uttList = append(uttList, uttID)
		fmt.Fprintf(wavScpFile, "%s %s\n", uttID, path)
	}
	wavScpFile.Close()

	// Создаём spk2utt
	spk2uttFile, err := os.Create(spk2utt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(spk2uttFile, "global %s\n", strings.Join(uttList, " "))
	spk2uttFile.Close()

	start := time.Now()

	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		"ark:"+spk2utt,
		"scp:"+wavScp,
		"ark:/dev/null",
	)

	output, err := cmd.CombinedOutput()
	totalElapsed := time.Since(start).Seconds()

	if err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch decode error: %v", err),
			}
		}
		return results, nil
	}

	// Парсим результаты
	transcriptions := d.parseOutputBatch(string(output), uttIDs)

	// Формируем результаты
	for uttID, path := range uttIDs {
		duration, _ := getAudioDuration(path)
		text := transcriptions[uttID]

		rtf := 0.0
		avgTime := totalElapsed / float64(len(wavPaths))
		if duration > 0 {
			rtf = avgTime / duration
		}

		results[path] = &DecodeResult{
			Text:           text,
			Duration:       duration,
			ProcessingTime: avgTime,
			RTF:            rtf,
			Success:        true,
		}
	}

	return results, nil
}

// decodeBatchWithRescoring — batch декодирование с lattice rescoring (NoLM)
func (d *KaldiDecoder) decodeBatchWithRescoring(wavPaths []string) (map[string]*DecodeResult, error) {
	results := make(map[string]*DecodeResult)

	tmpDir, err := os.MkdirTemp("", "kaldi_batch_nolm_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	wavScp := filepath.Join(tmpDir, "wav.scp")
	spk2utt := filepath.Join(tmpDir, "spk2utt")
	latticePath := filepath.Join(tmpDir, "lat.ark")

	// Создаём wav.scp
	wavScpFile, err := os.Create(wavScp)
	if err != nil {
		return nil, err
	}

	uttIDs := make(map[string]string) // uttID -> wavPath
	var uttList []string

	for i, path := range wavPaths {
		uttID := fmt.Sprintf("utt%06d", i)
		uttIDs[uttID] = path
		uttList = append(uttList, uttID)
		fmt.Fprintf(wavScpFile, "%s %s\n", uttID, path)
	}
	wavScpFile.Close()

	// Создаём spk2utt
	spk2uttFile, err := os.Create(spk2utt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(spk2uttFile, "global %s\n", strings.Join(uttList, " "))
	spk2uttFile.Close()

	start := time.Now()

	// Шаг 1: Batch декодирование в lattice
	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd1 := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		"ark:"+spk2utt,
		"scp:"+wavScp,
		"ark:"+latticePath,
	)

	if output, err := cmd1.CombinedOutput(); err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch decode step failed: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Проверяем lattice
	if fi, err := os.Stat(latticePath); err != nil || fi.Size() == 0 {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   "batch lattice file empty or not created",
			}
		}
		return results, nil
	}

	// Шаг 2: Batch rescoring + best path
	rescoreBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-scale")
	bestPathBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-best-path")

	int2symPl := filepath.Join(d.kaldiRoot, "egs/wsj/s5/utils/int2sym.pl")
	if _, err := os.Stat(int2symPl); os.IsNotExist(err) {
		int2symPl = filepath.Join(d.kaldiRoot, "egs/work_3/s5/utils/int2sym.pl")
	}

	cmd2 := exec.Command("bash", "-c", fmt.Sprintf(
		"%s --lm-scale=0.0 --acoustic-scale=1.0 'ark:%s' ark:- | %s ark:- ark,t:- | %s -f 2- %s",
		rescoreBin, latticePath, bestPathBin, int2symPl, d.wordsTxt,
	))

	output, err := cmd2.CombinedOutput()
	totalElapsed := time.Since(start).Seconds()

	if err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch rescore step failed: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Парсим результаты
	transcriptions := d.parseOutputBatch(string(output), uttIDs)

	// Формируем результаты
	for uttID, path := range uttIDs {
		duration, _ := getAudioDuration(path)
		text := transcriptions[uttID]

		rtf := 0.0
		avgTime := totalElapsed / float64(len(wavPaths))
		if duration > 0 {
			rtf = avgTime / duration
		}

		results[path] = &DecodeResult{
			Text:           text,
			Duration:       duration,
			ProcessingTime: avgTime,
			RTF:            rtf,
			Success:        true,
		}
	}

	return results, nil
}

// ============================================================
// Helper functions
// ============================================================

// parseOutput извлекает текст для одного utterance
func (d *KaldiDecoder) parseOutput(output, uttID string) string {
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, uttID+" ") {
			parts := strings.SplitN(line, " ", 2)
			if len(parts) > 1 {
				return parts[1]
			}
			return ""
		}
	}
	return ""
}

// parseOutputBatch извлекает тексты для множества utterances
func (d *KaldiDecoder) parseOutputBatch(output string, uttIDs map[string]string) map[string]string {
	transcriptions := make(map[string]string)

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		for uttID := range uttIDs {
			if strings.HasPrefix(line, uttID+" ") {
				parts := strings.SplitN(line, " ", 2)
				if len(parts) > 1 {
					transcriptions[uttID] = parts[1]
				} else {
					transcriptions[uttID] = ""
				}
				break
			}
		}
	}

	return transcriptions
}

func getAudioDuration(path string) (float64, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path)

	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	var duration float64
	fmt.Sscanf(strings.TrimSpace(string(out)), "%f", &duration)
	return duration, nil
}

func (d *KaldiDecoder) Health() error {
	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")
	if _, err := os.Stat(decoderBin); os.IsNotExist(err) {
		return fmt.Errorf("decoder not found: %s", decoderBin)
	}

	// Проверяем lattice tools для NoLM режима
	rescoreBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-scale")
	if _, err := os.Stat(rescoreBin); os.IsNotExist(err) {
		return fmt.Errorf("lattice-scale not found: %s", rescoreBin)
	}

	bestPathBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-best-path")
	if _, err := os.Stat(bestPathBin); os.IsNotExist(err) {
		return fmt.Errorf("lattice-best-path not found: %s", bestPathBin)
	}

	return nil
}

// ============================================================
// GPU Batch decoding (через batched-wav-nnet3-cuda)
// ============================================================

// DecodeBatchGPU — batch декодирование на GPU
func (d *KaldiDecoder) DecodeBatchGPU(wavPaths []string) (map[string]*DecodeResult, error) {
	if len(wavPaths) == 0 {
		return nil, nil
	}

	// NoLM режим — GPU + lattice rescoring
	if d.lmScale == 0 {
		return d.decodeBatchGPUWithRescoring(wavPaths)
	}

	// Обычный режим — GPU batch
	return d.decodeBatchGPUDirect(wavPaths)
}

// decodeBatchGPUDirect — GPU batch декодирование с LM
func (d *KaldiDecoder) decodeBatchGPUDirect(wavPaths []string) (map[string]*DecodeResult, error) {
	results := make(map[string]*DecodeResult)

	tmpDir, err := os.MkdirTemp("", "kaldi_gpu_batch_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	wavScp := filepath.Join(tmpDir, "wav.scp")
	spk2utt := filepath.Join(tmpDir, "spk2utt")

	// Создаём wav.scp
	wavScpFile, err := os.Create(wavScp)
	if err != nil {
		return nil, err
	}

	uttIDs := make(map[string]string)
	var uttList []string

	for i, path := range wavPaths {
		uttID := fmt.Sprintf("utt%06d", i)
		uttIDs[uttID] = path
		uttList = append(uttList, uttID)
		fmt.Fprintf(wavScpFile, "%s %s\n", uttID, path)
	}
	wavScpFile.Close()

	// Создаём spk2utt
	spk2uttFile, err := os.Create(spk2utt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(spk2uttFile, "global %s\n", strings.Join(uttList, " "))
	spk2uttFile.Close()

	start := time.Now()

	// GPU batch decoder
	gpuDecoder := filepath.Join(d.kaldiRoot, "src/cudadecoderbin/batched-wav-nnet3-cuda")

	cmd := exec.Command(gpuDecoder,
		"--config="+d.onlineConf,
		"--cuda-decoder-copy-threads=2",
		"--cuda-worker-threads=4",
		"--max-batch-size="+fmt.Sprintf("%d", len(wavPaths)),
		"--num-channels="+fmt.Sprintf("%d", len(wavPaths)),
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		"ark:"+spk2utt,
		"scp:"+wavScp,
		"ark,t:-",
	)

	output, err := cmd.CombinedOutput()
	totalElapsed := time.Since(start).Seconds()

	if err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("GPU batch decode error: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Парсим результаты
	transcriptions := d.parseOutputBatch(string(output), uttIDs)

	for uttID, path := range uttIDs {
		duration, _ := getAudioDuration(path)
		text := transcriptions[uttID]

		rtf := 0.0
		avgTime := totalElapsed / float64(len(wavPaths))
		if duration > 0 {
			rtf = avgTime / duration
		}

		results[path] = &DecodeResult{
			Text:           text,
			Duration:       duration,
			ProcessingTime: avgTime,
			RTF:            rtf,
			Success:        true,
		}
	}

	return results, nil
}

// decodeBatchGPUWithRescoring — GPU batch + lattice rescoring (NoLM)
func (d *KaldiDecoder) decodeBatchGPUWithRescoring(wavPaths []string) (map[string]*DecodeResult, error) {
	results := make(map[string]*DecodeResult)

	tmpDir, err := os.MkdirTemp("", "kaldi_gpu_batch_nolm_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	wavScp := filepath.Join(tmpDir, "wav.scp")
	spk2utt := filepath.Join(tmpDir, "spk2utt")
	latticePath := filepath.Join(tmpDir, "lat.ark")

	// Создаём wav.scp
	wavScpFile, err := os.Create(wavScp)
	if err != nil {
		return nil, err
	}

	uttIDs := make(map[string]string)
	var uttList []string

	for i, path := range wavPaths {
		uttID := fmt.Sprintf("utt%06d", i)
		uttIDs[uttID] = path
		uttList = append(uttList, uttID)
		fmt.Fprintf(wavScpFile, "%s %s\n", uttID, path)
	}
	wavScpFile.Close()

	// Создаём spk2utt
	spk2uttFile, err := os.Create(spk2utt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(spk2uttFile, "global %s\n", strings.Join(uttList, " "))
	spk2uttFile.Close()

	start := time.Now()

	// Шаг 1: GPU batch декодирование в lattice
	// Используем CPU декодер для lattice output (GPU decoder не поддерживает lattice output напрямую)
	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd1 := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		"ark:"+spk2utt,
		"scp:"+wavScp,
		"ark:"+latticePath,
	)

	if output, err := cmd1.CombinedOutput(); err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch decode step failed: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Шаг 2: Batch rescoring + best path
	rescoreBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-scale")
	bestPathBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-best-path")
	int2symPl := filepath.Join(d.kaldiRoot, "egs/work_3/s5/utils/int2sym.pl")

	cmd2 := exec.Command("bash", "-c", fmt.Sprintf(
		"%s --lm-scale=0.0 --acoustic-scale=1.0 'ark:%s' ark:- | %s ark:- ark,t:- | %s -f 2- %s",
		rescoreBin, latticePath, bestPathBin, int2symPl, d.wordsTxt,
	))

	output, err := cmd2.CombinedOutput()
	totalElapsed := time.Since(start).Seconds()

	if err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch rescore step failed: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Парсим результаты
	transcriptions := d.parseOutputBatch(string(output), uttIDs)

	for uttID, path := range uttIDs {
		duration, _ := getAudioDuration(path)
		text := transcriptions[uttID]

		rtf := 0.0
		avgTime := totalElapsed / float64(len(wavPaths))
		if duration > 0 {
			rtf = avgTime / duration
		}

		results[path] = &DecodeResult{
			Text:           text,
			Duration:       duration,
			ProcessingTime: avgTime,
			RTF:            rtf,
			Success:        true,
		}
	}

	return results, nil
}
```

## File: ./internal/asr/whisper.go
```go
package asr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// === Whisper Local Client (faster-whisper) ===

type WhisperLocalClient struct {
	baseURL    string
	language   string
	httpClient *http.Client
}

func NewWhisperLocalClient(baseURL, language string) *WhisperLocalClient {
	if language == "" {
		language = "az"
	}
	return &WhisperLocalClient{
		baseURL:  baseURL,
		language: language,
		httpClient: &http.Client{
			Timeout: 300 * time.Second,
		},
	}
}

func (c *WhisperLocalClient) Transcribe(audioPath string) (*DecodeResult, error) {
	start := time.Now()

	file, err := os.Open(audioPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("audio", filepath.Base(audioPath))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(part, file); err != nil {
		return nil, err
	}

	writer.WriteField("language", c.language)
	writer.Close()

	req, err := http.NewRequest("POST", c.baseURL+"/transcribe", &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("status %d: %s", resp.StatusCode, string(body)),
		}, nil
	}

	var result struct {
		Text     string  `json:"text"`
		Duration float64 `json:"duration"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Seconds()
	duration := result.Duration
	if duration == 0 {
		duration, _ = getAudioDuration(audioPath)
	}

	rtf := 0.0
	if duration > 0 {
		rtf = elapsed / duration
	}

	return &DecodeResult{
		Text:           result.Text,
		Duration:       duration,
		ProcessingTime: elapsed,
		RTF:            rtf,
		Success:        true,
	}, nil
}

func (c *WhisperLocalClient) Health() error {
	resp, err := c.httpClient.Get(c.baseURL + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// === Whisper OpenAI Client ===

type WhisperOpenAIClient struct {
	baseURL    string
	apiKey     string
	model      string
	language   string
	httpClient *http.Client
}

func NewWhisperOpenAIClient(apiKey, model, language string) *WhisperOpenAIClient {
	if model == "" {
		model = "whisper-1"
	}
	if language == "" {
		language = "az"
	}
	return &WhisperOpenAIClient{
		baseURL:  "https://api.openai.com/v1",
		apiKey:   apiKey,
		model:    model,
		language: language,
		httpClient: &http.Client{
			Timeout: 300 * time.Second,
		},
	}
}

func (c *WhisperOpenAIClient) Transcribe(audioPath string) (*DecodeResult, error) {
	start := time.Now()

	file, err := os.Open(audioPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("file", filepath.Base(audioPath))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(part, file); err != nil {
		return nil, err
	}

	writer.WriteField("model", c.model)
	writer.WriteField("language", c.language)
	writer.Close()

	req, err := http.NewRequest("POST", c.baseURL+"/audio/transcriptions", &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("status %d: %s", resp.StatusCode, string(body)),
		}, nil
	}

	var result struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Seconds()
	duration, _ := getAudioDuration(audioPath)

	rtf := 0.0
	if duration > 0 {
		rtf = elapsed / duration
	}

	return &DecodeResult{
		Text:           result.Text,
		Duration:       duration,
		ProcessingTime: elapsed,
		RTF:            rtf,
		Success:        true,
	}, nil
}

func (c *WhisperOpenAIClient) Health() error {
	req, _ := http.NewRequest("GET", c.baseURL+"/models", nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
```

## File: ./internal/audio/hash.go
```go
package audio

import (
    "crypto/md5"
    "encoding/hex"
    "io"
    "os"
)

func MD5File(path string) (string, error) {
    f, err := os.Open(path)
    if err != nil {
        return "", err
    }
    defer f.Close()
    
    h := md5.New()
    if _, err := io.Copy(h, f); err != nil {
        return "", err
    }
    
    return hex.EncodeToString(h.Sum(nil)), nil
}
```

## File: ./internal/audio/metadata.go
```go
package audio

import (
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
)

type Metadata struct {
	DurationSec float64 `json:"duration_sec"`
	SampleRate  int     `json:"sample_rate"`
	Channels    int     `json:"channels"`
	BitDepth    int     `json:"bit_depth"`
	FileSize    int64   `json:"file_size"`
	Codec       string  `json:"codec"`
	Format      string  `json:"format"`
}

func GetMetadata(path string) (*Metadata, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("ffprobe",
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		path)

	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var probe struct {
		Streams []struct {
			SampleRate   string `json:"sample_rate"`
			Channels     int    `json:"channels"`
			BitsPerSample int   `json:"bits_per_sample"`
			CodecName    string `json:"codec_name"`
		} `json:"streams"`
		Format struct {
			Duration   string `json:"duration"`
			FormatName string `json:"format_name"`
		} `json:"format"`
	}

	if err := json.Unmarshal(out, &probe); err != nil {
		return nil, err
	}

	m := &Metadata{FileSize: fi.Size()}

	if probe.Format.Duration != "" {
		m.DurationSec, _ = strconv.ParseFloat(probe.Format.Duration, 64)
	}
	m.Format = probe.Format.FormatName

	if len(probe.Streams) > 0 {
		s := probe.Streams[0]
		m.SampleRate, _ = strconv.Atoi(s.SampleRate)
		m.Channels = s.Channels
		m.BitDepth = s.BitsPerSample
		m.Codec = s.CodecName
	}

	return m, nil
}

func (m *Metadata) ToJSON() string {
	b, _ := json.Marshal(m)
	return string(b)
}
```

## File: ./internal/audio/silence.go
```go
package audio

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// SilenceInfo информация о тишине в конце файла
type SilenceInfo struct {
	HasTrailingSilence bool    `json:"has_trailing_silence"`
	SilenceDuration    float64 `json:"silence_duration_ms"` // в миллисекундах
	TotalDuration      float64 `json:"total_duration"`
}

// getAudioDuration получает длительность аудио через ffprobe
func getAudioDuration(path string) (float64, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path)

	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	var duration float64
	_, err = fmt.Sscanf(strings.TrimSpace(string(out)), "%f", &duration)
	return duration, err
}

// DetectTrailingSilence проверяет есть ли тишина в конце аудио
// Kaldi требует минимум 100ms тишины в конце
func DetectTrailingSilence(wavPath string, minSilenceMs float64) (*SilenceInfo, error) {
	if minSilenceMs <= 0 {
		minSilenceMs = 100 // 100ms по умолчанию для Kaldi
	}

	// Получаем длительность файла
	duration, err := getAudioDuration(wavPath)
	if err != nil {
		return nil, err
	}

	// Используем sox для определения тишины в конце
	// reverse -> silence detect -> получаем длину тишины с конца
	cmd := exec.Command("sox", wavPath, "-n", "reverse", "silence", "1", "0.01", "1%", "reverse", "stat")
	output, _ := cmd.CombinedOutput()

	// Альтернативный метод через ffmpeg silencedetect
	silenceDur := detectSilenceFFmpeg(wavPath, duration)

	info := &SilenceInfo{
		TotalDuration:      duration,
		SilenceDuration:    silenceDur * 1000, // в ms
		HasTrailingSilence: silenceDur*1000 >= minSilenceMs,
	}

	_ = output // sox output для дебага если нужно

	return info, nil
}

// detectSilenceFFmpeg определяет тишину в конце через ffmpeg
func detectSilenceFFmpeg(wavPath string, totalDuration float64) float64 {
	// Анализируем последние 500ms файла
	startTime := totalDuration - 0.5
	if startTime < 0 {
		startTime = 0
	}

	cmd := exec.Command("ffmpeg",
		"-ss", fmt.Sprintf("%.3f", startTime),
		"-i", wavPath,
		"-af", "silencedetect=noise=-40dB:d=0.05",
		"-f", "null", "-",
	)

	output, _ := cmd.CombinedOutput()
	outStr := string(output)

	// Ищем silence_end в конце файла
	// [silencedetect @ ...] silence_end: 4.532 | silence_duration: 0.156
	lines := strings.Split(outStr, "\n")
	var lastSilenceDur float64

	for _, line := range lines {
		if strings.Contains(line, "silence_duration:") {
			parts := strings.Split(line, "silence_duration:")
			if len(parts) > 1 {
				durStr := strings.TrimSpace(strings.Split(parts[1], "|")[0])
				if dur, err := strconv.ParseFloat(durStr, 64); err == nil {
					lastSilenceDur = dur
				}
			}
		}
	}

	// Проверяем RMS в последних 100ms
	if lastSilenceDur == 0 {
		lastSilenceDur = checkRMSAtEnd(wavPath, totalDuration)
	}

	return lastSilenceDur
}

// checkRMSAtEnd проверяет RMS уровень в последних N мс
func checkRMSAtEnd(wavPath string, totalDuration float64) float64 {
	// Проверяем последние 100ms
	startTime := totalDuration - 0.1
	if startTime < 0 {
		return 0
	}

	cmd := exec.Command("sox", wavPath, "-n",
		"trim", fmt.Sprintf("%.3f", startTime),
		"stat")

	output, _ := cmd.CombinedOutput()
	outStr := string(output)

	// Ищем RMS amplitude
	for _, line := range strings.Split(outStr, "\n") {
		if strings.Contains(line, "RMS") && strings.Contains(line, "amplitude") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				if rms, err := strconv.ParseFloat(parts[len(parts)-1], 64); err == nil {
					// RMS < 0.01 считаем тишиной
					if rms < 0.01 {
						return 0.1 // 100ms тишины
					}
				}
			}
		}
	}

	return 0
}

// AddTrailingSilence добавляет тишину в конец файла
func AddTrailingSilence(inputPath, outputPath string, silenceMs float64) error {
	if silenceMs <= 0 {
		silenceMs = 100
	}

	// Получаем sample rate исходного файла
	meta, err := GetMetadata(inputPath)
	if err != nil {
		return err
	}

	sampleRate := meta.SampleRate
	if sampleRate == 0 {
		sampleRate = 8000 // fallback для телефонных записей
	}

	// sox input.wav output.wav pad 0 0.1 (добавляет 100ms в конец)
	silenceSec := silenceMs / 1000.0

	cmd := exec.Command("sox", inputPath, outputPath,
		"pad", "0", fmt.Sprintf("%.3f", silenceSec))

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sox pad error: %v, output: %s", err, string(output))
	}

	return nil
}

// RemoveTrailingSilence удаляет тишину с конца файла
func RemoveTrailingSilence(inputPath, outputPath string) error {
	// sox input.wav output.wav reverse silence 1 0.01 1% reverse
	cmd := exec.Command("sox", inputPath, outputPath,
		"reverse", "silence", "1", "0.01", "1%", "reverse")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sox silence remove error: %v, output: %s", err, string(output))
	}

	return nil
}

// MergeAudioFiles склеивает несколько WAV файлов в один с паузами между ними
func MergeAudioFiles(inputPaths []string, outputPath string, pauseMs float64) error {
	if len(inputPaths) == 0 {
		return fmt.Errorf("no input files")
	}

	// Создаём директорию если не существует
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}

	if len(inputPaths) == 1 {
		// Просто копируем
		return copyFile(inputPaths[0], outputPath)
	}

	// По умолчанию 150ms пауза
	if pauseMs <= 0 {
		pauseMs = 150
	}

	// Получаем sample rate из первого файла
	meta, err := GetMetadata(inputPaths[0])
	if err != nil {
		return err
	}
	sampleRate := meta.SampleRate
	if sampleRate == 0 {
		sampleRate = 16000
	}

	// Создаём временный файл тишины
	tmpDir, err := os.MkdirTemp("", "merge_")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	silencePath := filepath.Join(tmpDir, "silence.wav")
	pauseSec := pauseMs / 1000.0

	// Генерируем тишину: sox -n -r 16000 -c 1 silence.wav trim 0.0 0.15
	cmdSilence := exec.Command("sox", "-n", "-r", fmt.Sprintf("%d", sampleRate),
		"-c", "1", "-b", "16", silencePath, "trim", "0.0", fmt.Sprintf("%.3f", pauseSec))
	if output, err := cmdSilence.CombinedOutput(); err != nil {
		return fmt.Errorf("create silence failed: %v, output: %s", err, string(output))
	}

	// Строим список файлов с тишиной между ними
	// file1 silence file2 silence file3 -> output
	var args []string
	for i, path := range inputPaths {
		args = append(args, path)
		// Добавляем тишину после каждого файла кроме последнего
		if i < len(inputPaths)-1 {
			args = append(args, silencePath)
		}
	}
	args = append(args, outputPath)

	// sox file1.wav silence.wav file2.wav silence.wav file3.wav output.wav
	cmd := exec.Command("sox", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sox concat error: %v, output: %s", err, string(output))
	}

	return nil
}

func copyFile(src, dst string) error {
	input, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, input, 0644)
}
```

## File: ./internal/audio/stats.go
```go
package audio

import (
	"bufio"
	"encoding/json"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

type AudioStats struct {
	// Sox metrics
	DCOffset    float64 `json:"dc_offset"`
	MinLevel    float64 `json:"min_level"`
	MaxLevel    float64 `json:"max_level"`
	PkLevDB     float64 `json:"pk_lev_db"`
	RMSLevDB    float64 `json:"rms_lev_db"`
	RMSPkDB     float64 `json:"rms_pk_db"`
	RMSTrDB     float64 `json:"rms_tr_db"`
	CrestFactor float64 `json:"crest_factor"`
	FlatFactor  float64 `json:"flat_factor"`
	PkCount     int     `json:"pk_count"`
	BitDepth    string  `json:"bit_depth"`
	NumSamples  string  `json:"num_samples"`
	LengthSec   float64 `json:"length_sec"`

	// SNR estimates (разные методы)
	SNRSox      float64 `json:"snr_sox"`      // RMS Pk - RMS Tr
	SNRSpectral float64 `json:"snr_spectral"` // ffmpeg astats
	SNRVad      float64 `json:"snr_vad"`      // silence detection
	SNRWada     float64 `json:"snr_wada"`     // WADA algorithm
	SNREstimate float64 `json:"snr_estimate"` // Combined estimate

	// Quality
	NoiseLevel string `json:"noise_level"` // low, medium, high, very_high
}

// AudioQuality на основе метрик
type AudioQuality struct {
	Score       int    `json:"score"`
	Level       string `json:"level"`
	IsTooQuiet  bool   `json:"is_too_quiet"`
	IsTooLoud   bool   `json:"is_too_loud"`
	IsClipping  bool   `json:"is_clipping"`
	HasDCOffset bool   `json:"has_dc_offset"`
}

// GetStats собирает все метрики
func GetStats(path string) (*AudioStats, error) {
	stats := &AudioStats{}

	// Method 1: Sox stats
	getSoxStats(path, stats)

	// SNR from Sox (RMS Pk - RMS Tr)
	if stats.RMSTrDB < 0 && stats.RMSPkDB < 0 && stats.RMSTrDB != stats.RMSPkDB {
		snr := stats.RMSPkDB - stats.RMSTrDB
		if snr > 0 && snr < 100 {
			stats.SNRSox = snr
		}
	}

	// Method 2: Spectral (ffmpeg astats)
	stats.SNRSpectral = getSpectralSNR(path)

	// Method 3: VAD-based (silence detection)
	stats.SNRVad = getVADBasedSNR(path)

	// Method 4: WADA-SNR (Go native, самый точный для речи)
	if snr, err := WADASNR(path); err == nil && snr > 0 && snr < 100 {
		stats.SNRWada = snr
	}

	// Combined estimate (взвешенное среднее всех методов)
	stats.SNREstimate = combineSNRAll(stats.SNRSox, stats.SNRSpectral, stats.SNRVad, stats.SNRWada)
	stats.NoiseLevel = classifyNoise(stats.SNREstimate)

	return stats, nil
}

// getSoxStats через sox stats
func getSoxStats(path string, stats *AudioStats) error {
	cmd := exec.Command("sox", path, "-n", "stats")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		key := strings.Join(parts[:len(parts)-1], " ")
		val := parts[len(parts)-1]

		switch key {
		case "DC offset":
			stats.DCOffset, _ = strconv.ParseFloat(val, 64)
		case "Min level":
			stats.MinLevel, _ = strconv.ParseFloat(val, 64)
		case "Max level":
			stats.MaxLevel, _ = strconv.ParseFloat(val, 64)
		case "Pk lev dB":
			stats.PkLevDB, _ = strconv.ParseFloat(val, 64)
		case "RMS lev dB":
			stats.RMSLevDB, _ = strconv.ParseFloat(val, 64)
		case "RMS Pk dB":
			stats.RMSPkDB, _ = strconv.ParseFloat(val, 64)
		case "RMS Tr dB":
			stats.RMSTrDB, _ = strconv.ParseFloat(val, 64)
		case "Crest factor":
			stats.CrestFactor, _ = strconv.ParseFloat(val, 64)
		case "Flat factor":
			stats.FlatFactor, _ = strconv.ParseFloat(val, 64)
		case "Pk count":
			stats.PkCount, _ = strconv.Atoi(val)
		case "Bit-depth":
			stats.BitDepth = val
		case "Num samples":
			stats.NumSamples = val
		case "Length s":
			stats.LengthSec, _ = strconv.ParseFloat(val, 64)
		}
	}

	cmd.Wait()
	return scanner.Err()
}

// getSpectralSNR через ffmpeg astats
func getSpectralSNR(path string) float64 {
	cmd := exec.Command("ffmpeg",
		"-i", path,
		"-af", "astats=metadata=1:reset=1",
		"-f", "null", "-",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	var rmsLevel, peakLevel float64
	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		if strings.Contains(line, "RMS level dB:") {
			parts := strings.Split(line, ":")
			if len(parts) >= 2 {
				rmsLevel, _ = strconv.ParseFloat(strings.TrimSpace(parts[len(parts)-1]), 64)
			}
		}
		if strings.Contains(line, "Peak level dB:") {
			parts := strings.Split(line, ":")
			if len(parts) >= 2 {
				peakLevel, _ = strconv.ParseFloat(strings.TrimSpace(parts[len(parts)-1]), 64)
			}
		}
	}

	// Dynamic range как proxy для SNR
	// Crest factor в dB — разница между пиком и RMS
	// Для чистой речи обычно 12-18 dB
	if rmsLevel < 0 && peakLevel < 0 {
		crest := peakLevel - rmsLevel
		return crest * 1.5 // эмпирический коэффициент
	}

	return 0
}

// getVADBasedSNR через silencedetect
func getVADBasedSNR(path string) float64 {
	cmd := exec.Command("ffmpeg",
		"-i", path,
		"-af", "silencedetect=noise=-40dB:d=0.1",
		"-f", "null", "-",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	// Считаем количество silence_start событий
	silenceCount := strings.Count(string(output), "silence_start")

	// Если много пауз — вероятно чистая запись
	// Мало пауз — либо непрерывная речь, либо шум
	if silenceCount > 5 {
		return 25 // Хорошее качество
	} else if silenceCount > 2 {
		return 18
	}
	return 12
}

// combineSNRAll комбинирует все методы с весами
func combineSNRAll(soxSNR, spectralSNR, vadSNR, wadaSNR float64) float64 {
	values := []float64{}
	weights := []float64{}

	// Фильтруем только валидные значения
	if isValidSNR(soxSNR) {
		values = append(values, soxSNR)
		weights = append(weights, 1.0)
	}

	if isValidSNR(spectralSNR) {
		values = append(values, spectralSNR)
		weights = append(weights, 1.5)
	}

	if isValidSNR(vadSNR) {
		values = append(values, vadSNR)
		weights = append(weights, 0.5)
	}

	if isValidSNR(wadaSNR) {
		values = append(values, wadaSNR)
		weights = append(weights, 2.5)
	}

	if len(values) == 0 {
		return 0
	}

	var sum, weightSum float64
	for i, v := range values {
		sum += v * weights[i]
		weightSum += weights[i]
	}

	result := sum / weightSum

	// Финальная проверка
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return 0
	}

	return math.Round(result*10) / 10
}

// isValidSNR проверяет что SNR значение валидное
func isValidSNR(snr float64) bool {
	if math.IsNaN(snr) || math.IsInf(snr, 0) {
		return false
	}
	if snr <= 0 || snr > 100 {
		return false
	}
	return true
}

// classifyNoise классифицирует уровень шума
func classifyNoise(snr float64) string {
	if snr >= 30 {
		return "low" // Чистая запись
	} else if snr >= 20 {
		return "medium" // Небольшой шум
	} else if snr >= 10 {
		return "high" // Заметный шум
	}
	return "very_high" // Сильный шум
}

// Quality анализ качества аудио
func (s *AudioStats) Quality() AudioQuality {
	q := AudioQuality{}

	q.IsTooQuiet = s.RMSLevDB < -40
	q.IsTooLoud = s.RMSLevDB > -10
	q.IsClipping = s.FlatFactor > 0
	q.HasDCOffset = s.DCOffset > 0.01 || s.DCOffset < -0.01

	// Расчёт score (0-100)
	score := 100

	// Штраф за тихий сигнал
	if s.RMSLevDB < -40 {
		score -= 30
	} else if s.RMSLevDB < -35 {
		score -= 15
	}

	// Штраф за громкий сигнал
	if s.RMSLevDB > -10 {
		score -= 20
	}

	// Штраф за клиппинг
	if s.FlatFactor > 0 {
		score -= int(s.FlatFactor * 10)
	}

	// Штраф за DC offset
	if q.HasDCOffset {
		score -= 10
	}

	// Бонус/штраф за SNR
	if s.SNREstimate >= 30 {
		score += 10
	} else if s.SNREstimate < 15 {
		score -= 20
	}

	// Оптимальный RMS для речи: -25 to -18 dB
	if s.RMSLevDB >= -25 && s.RMSLevDB <= -18 {
		score += 10
	}

	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	q.Score = score

	if score >= 80 {
		q.Level = "good"
	} else if score >= 50 {
		q.Level = "medium"
	} else {
		q.Level = "poor"
	}

	return q
}

// ToJSON для сохранения в БД
func (s *AudioStats) ToJSON() string {
	b, _ := json.Marshal(s)
	return string(b)
}
```

## File: ./internal/audio/wada.go
```go
package audio

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
)

// WADA-SNR: Waveform Amplitude Distribution Analysis
// Для речи распределение близко к Лапласу (gamma ≈ 0.707)
// Для шума — к Гауссу (gamma ≈ 0.798)

func WADASNR(path string) (float64, error) {
	samples, err := readWavSamples(path)
	if err != nil {
		return 0, err
	}

	if len(samples) < 1000 {
		return 0, errors.New("audio too short")
	}

	// Убираем тишину (порог 1% от максимума)
	threshold := 0.01
	var filtered []float64
	for _, s := range samples {
		if math.Abs(s) > threshold {
			filtered = append(filtered, s)
		}
	}

	if len(filtered) < 500 {
		return 0, errors.New("not enough non-silent samples")
	}

	// Вычисляем mean absolute и RMS
	var sumAbs, sumSq float64
	for _, s := range filtered {
		sumAbs += math.Abs(s)
		sumSq += s * s
	}

	n := float64(len(filtered))
	meanAbs := sumAbs / n
	rms := math.Sqrt(sumSq / n)

	if rms < 1e-10 {
		return 0, errors.New("signal too quiet")
	}

	// Gamma: отношение mean|x| к RMS
	// Лаплас: sqrt(2)/2 ≈ 0.707
	// Гаусс: sqrt(2/pi) ≈ 0.798
	gamma := meanAbs / rms

	// WADA-SNR формула
	// gamma = 0.707 → чистая речь → высокий SNR
	// gamma = 0.798 → шум → низкий SNR
	diff := gamma - 0.707
	if diff < 0.001 {
		diff = 0.001 // Защита от log(0)
	}

	snr := -10 * math.Log10(diff/0.091)

	// Ограничиваем разумными пределами
	if snr < 0 {
		snr = 0
	}
	if snr > 50 {
		snr = 50
	}

	return math.Round(snr*10) / 10, nil
}

// readWavSamples читает WAV и возвращает нормализованные сэмплы [-1, 1]
func readWavSamples(path string) ([]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Читаем WAV заголовок
	header := make([]byte, 44)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}

	// Проверяем RIFF/WAVE
	if string(header[0:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return nil, errors.New("not a valid WAV file")
	}

	// Парсим формат
	audioFormat := binary.LittleEndian.Uint16(header[20:22])
	numChannels := binary.LittleEndian.Uint16(header[22:24])
	bitsPerSample := binary.LittleEndian.Uint16(header[34:36])

	if audioFormat != 1 {
		return nil, errors.New("only PCM format supported")
	}

	// Ищем data chunk
	var dataSize uint32
	for {
		chunkHeader := make([]byte, 8)
		if _, err := io.ReadFull(f, chunkHeader); err != nil {
			return nil, errors.New("data chunk not found")
		}

		chunkID := string(chunkHeader[0:4])
		chunkSize := binary.LittleEndian.Uint32(chunkHeader[4:8])

		if chunkID == "data" {
			dataSize = chunkSize
			break
		}

		// Пропускаем неизвестные chunks
		if _, err := f.Seek(int64(chunkSize), io.SeekCurrent); err != nil {
			return nil, err
		}
	}

	// Читаем данные
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}

	// Конвертируем в float64
	bytesPerSample := int(bitsPerSample / 8)
	numSamples := len(data) / bytesPerSample / int(numChannels)
	samples := make([]float64, numSamples)

	for i := 0; i < numSamples; i++ {
		offset := i * bytesPerSample * int(numChannels)

		var sample float64
		switch bitsPerSample {
		case 8:
			// 8-bit unsigned
			sample = (float64(data[offset]) - 128) / 128
		case 16:
			// 16-bit signed
			val := int16(binary.LittleEndian.Uint16(data[offset : offset+2]))
			sample = float64(val) / 32768
		case 24:
			// 24-bit signed
			b := data[offset : offset+3]
			val := int32(b[0]) | int32(b[1])<<8 | int32(b[2])<<16
			if val&0x800000 != 0 {
				val |= ^0xFFFFFF // Sign extend
			}
			sample = float64(val) / 8388608
		case 32:
			// 32-bit signed
			val := int32(binary.LittleEndian.Uint32(data[offset : offset+4]))
			sample = float64(val) / 2147483648
		default:
			return nil, errors.New("unsupported bit depth")
		}

		samples[i] = sample
	}

	return samples, nil
}
```

## File: ./internal/config/config.go
```go
package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Data     DataConfig
	Kaldi    KaldiConfig
	Whisper  WhisperConfig
	Workers  WorkersConfig
}

type ServerConfig struct {
	Addr string
}

type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
}

type DataConfig struct {
	Dir string
}

type KaldiConfig struct {
	ModelDir string
	Host     string
	Key      string
}

type WhisperConfig struct {
	LocalURL    string
	Lang        string
	OpenAIKey   string
	OpenAIModel string
}

type WorkersConfig struct {
	Scan int
	ASR  int
}

func Load(envFile string) (*Config, error) {
	godotenv.Load(envFile)

	return &Config{
		Server: ServerConfig{
			Addr: getEnv("SERVER_ADDR", ":8082"),
		},
		Database: DatabaseConfig{
			Host:     getEnv("DB_HOST", "127.0.0.1"),
			Port:     getEnvInt("DB_PORT", 53306),
			User:     getEnv("DB_USER", "root"),
			Password: getEnv("DB_PASSWORD", ""),
			Name:     getEnv("DB_NAME", "label1"),
		},
		Data: DataConfig{
			Dir: getEnv("DATA_DIR", ""),
		},
		Kaldi: KaldiConfig{
			ModelDir: getEnv("KALDI_MODEL_DIR", ""),
			Host:     getEnv("ASR_HOST", ""),
			Key:      getEnv("ASR_KEY", ""),
		},
		Whisper: WhisperConfig{
			LocalURL:    getEnv("WHISPER_LOCAL_URL", ""),
			Lang:        getEnv("WHISPER_LOCAL_LANG", "az"),
			OpenAIKey:   getEnv("WHISPER_OPENAI_KEY", ""),
			OpenAIModel: getEnv("WHISPER_OPENAI_MODEL", "whisper-1"),
		},
		Workers: WorkersConfig{
			Scan: getEnvInt("SCAN_WORKERS", 10),
			ASR:  getEnvInt("ASR_WORKERS", 5),
		},
	}, nil
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}
```

## File: ./internal/db/asr.go
```go
package db

func (db *DB) UpdateASR(id int64, transcription string, wer, cer float64) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_asr = ?, wer = ?, cer = ?, 
		    asr_status = 'processed', processed_at = NOW()
		WHERE id = ?`,
		transcription, wer, cer, id)
	return err
}

// UpdateASRNoLM сохраняет результат Kaldi без LM
func (db *DB) UpdateASRNoLM(id int64, transcription string, wer, cer float64) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_asr_nolm = ?, wer_nolm = ?, cer_nolm = ?, 
		    asr_nolm_status = 'processed'
		WHERE id = ?`,
		transcription, wer, cer, id)
	return err
}

// UpdateASRNoLMError помечает файл как ошибочный для NoLM
func (db *DB) UpdateASRNoLMError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET asr_nolm_status = 'error' WHERE id = ?`, id)
	return err
}

// UpdateASRNoLMMetrics обновляет только WER/CER для NoLM
func (db *DB) UpdateASRNoLMMetrics(id int64, wer, cer float64) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET wer_nolm = ?, cer_nolm = ? WHERE id = ?`, wer, cer, id)
	return err
}

func (db *DB) UpdateASRMetrics(id int64, wer, cer float64) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET wer = ?, cer = ? WHERE id = ?`, wer, cer, id)
	return err
}
```

## File: ./internal/db/mariadb.go
```go
package db

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	statsCache     map[string]interface{}
	statsCacheTime time.Time
	statsCacheMu   sync.RWMutex
)

func (db *DB) StatsExtendedCached() (map[string]interface{}, error) {
	statsCacheMu.RLock()
	if statsCache != nil && time.Since(statsCacheTime) < 30*time.Second {
		defer statsCacheMu.RUnlock()
		return statsCache, nil
	}
	statsCacheMu.RUnlock()

	// Получаем свежие данные
	stats, err := db.StatsExtended()
	if err != nil {
		return nil, err
	}

	statsCacheMu.Lock()
	statsCache = stats
	statsCacheTime = time.Now()
	statsCacheMu.Unlock()

	return stats, nil
}

type AudioFile struct {
	ID                         int64     `json:"id"`
	UserID                     string    `json:"user_id"`
	ChapterID                  string    `json:"chapter_id"`
	MergedID                   int64     `json:"merged_id"`
	FilePath                   string    `json:"file_path"`
	FileHash                   string    `json:"file_hash"`
	DurationSec                float64   `json:"duration_sec"`
	SampleRate                 int       `json:"sample_rate"`
	Channels                   int       `json:"channels"`
	BitDepth                   int       `json:"bit_depth"`
	FileSize                   int64     `json:"file_size"`
	SNRDB                      float64   `json:"snr_db"`
	SNRSox                     float64   `json:"snr_sox"`
	SNRWada                    float64   `json:"snr_wada"`
	NoiseLevel                 string    `json:"noise_level"`
	RMSDB                      float64   `json:"rms_db"`
	AudioMetadata              string    `json:"audio_metadata"`
	TranscriptionOriginal      string    `json:"transcription_original"`
	TranscriptionASR           string    `json:"transcription_asr"`
	TranscriptionWhisperLocal  string    `json:"transcription_whisper_local"`
	TranscriptionWhisperOpenAI string    `json:"transcription_whisper_openai"`
	WER                        float64   `json:"wer"`
	CER                        float64   `json:"cer"`
	WERWhisperLocal            float64   `json:"wer_whisper_local"`
	CERWhisperLocal            float64   `json:"cer_whisper_local"`
	WERWhisperOpenAI           float64   `json:"wer_whisper_openai"`
	CERWhisperOpenAI           float64   `json:"cer_whisper_openai"`
	ASRStatus                  string    `json:"asr_status"`
	WhisperLocalStatus         string    `json:"whisper_local_status"`
	WhisperOpenAIStatus        string    `json:"whisper_openai_status"`
	ReviewStatus               string    `json:"review_status"`
	CreatedAt                  time.Time `json:"created_at"`

	// Kaldi NoLM
	TranscriptionASRNoLM string  `json:"transcription_asr_nolm"`
	WERNoLM              float64 `json:"wer_nolm"`
	CERNoLM              float64 `json:"cer_nolm"`
	ASRNoLMStatus        string  `json:"asr_nolm_status"`

	// Verification
	OperatorVerified bool       `json:"operator_verified"`
	VerifiedAt       *time.Time `json:"verified_at"`
	OriginalEdited   bool       `json:"original_edited"`

	// Silence & Merge  <-- ДОБАВИТЬ ЭТИ ПОЛЯ
	HasTrailingSilence bool   `json:"has_trailing_silence"`
	SilenceAdded       bool   `json:"silence_added"`
	ParentIDs          string `json:"parent_ids,omitempty"`

	Active bool `json:"active"`
}

// AudioFileRecalc - структура для пересчёта WER/CER
type AudioFileRecalc struct {
	ID                         int64
	TranscriptionOriginal      string
	TranscriptionASR           string
	TranscriptionASRNoLM       string
	TranscriptionWhisperLocal  string
	TranscriptionWhisperOpenAI string
}

type FileListResult struct {
	Files []AudioFile `json:"files"`
	Total int64       `json:"total"`
	Page  int         `json:"page"`
	Limit int         `json:"limit"`
}

type DB struct {
	conn *sql.DB
}

func New(host string, port int, user, password, dbname string) (*DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true",
		user, password, host, port, dbname)

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(50)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(5 * time.Minute)

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	return &DB{conn: conn}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) ExistsByHash(hash string) (bool, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM audio_files WHERE file_hash = ?", hash).Scan(&count)
	return count > 0, err
}

func (db *DB) ExistsByPath(path string) (bool, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM audio_files WHERE file_path = ?", path).Scan(&count)
	return count > 0, err
}

func (db *DB) Insert(af *AudioFile) (int64, error) {
	res, err := db.conn.Exec(`
		INSERT INTO audio_files 
		(user_id, chapter_id, file_path, file_hash, duration_sec, 
		 snr_db, snr_sox, snr_wada, noise_level, rms_db,
		 sample_rate, channels, bit_depth, file_size, audio_metadata, 
		 transcription_original, asr_status, asr_nolm_status, whisper_local_status, whisper_openai_status, review_status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'pending', 'pending', 'pending', 'pending')`,
		af.UserID, af.ChapterID, af.FilePath, af.FileHash, af.DurationSec,
		af.SNRDB, af.SNRSox, af.SNRWada, af.NoiseLevel, af.RMSDB,
		af.SampleRate, af.Channels, af.BitDepth, af.FileSize,
		af.AudioMetadata, af.TranscriptionOriginal)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *DB) UpdateError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files SET asr_status = 'error', 
		audio_metadata = JSON_SET(COALESCE(audio_metadata, '{}'), '$.error', ?)
		WHERE id = ?`, errMsg, id)
	return err
}

func (db *DB) GetPending(limit int) ([]AudioFile, error) {
	query := `
		SELECT id, file_path, file_hash, transcription_original 
		FROM audio_files 
		WHERE asr_status = 'pending'`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath, &af.FileHash, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

func (db *DB) Stats() (total, pending, processed, errors int, err error) {
	err = db.conn.QueryRow(`
		SELECT 
			COUNT(*),
			COALESCE(SUM(CASE WHEN asr_status = 'pending' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN asr_status = 'processed' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN asr_status = 'error' THEN 1 ELSE 0 END), 0)
		FROM audio_files`).Scan(&total, &pending, &processed, &errors)
	return
}

// StatsExtended возвращает расширенную статистику включая верификацию и все pending
func (db *DB) StatsExtended() (map[string]interface{}, error) {
	result := make(map[string]interface{})

	var total, pending, processed, errors int
	var verified, needsReview int
	var pendingNoLM, processedNoLM int
	var pendingWhisperLocal, processedWhisperLocal int
	var pendingWhisperOpenAI, processedWhisperOpenAI int

	db.conn.QueryRow(`
        SELECT 
            COUNT(*),
            COALESCE(SUM(CASE WHEN asr_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN asr_status = 'processed' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN asr_status = 'error' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN operator_verified = 1 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN wer > 0.15 AND operator_verified = 0 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN asr_nolm_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN asr_nolm_status = 'processed' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN whisper_local_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN whisper_local_status = 'processed' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN whisper_openai_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN whisper_openai_status = 'processed' THEN 1 ELSE 0 END), 0)
        FROM audio_files`).Scan(&total, &pending, &processed, &errors, &verified, &needsReview,
		&pendingNoLM, &processedNoLM, &pendingWhisperLocal, &processedWhisperLocal,
		&pendingWhisperOpenAI, &processedWhisperOpenAI)

	result["total"] = total
	result["pending"] = pending
	result["processed"] = processed
	result["errors"] = errors
	result["verified"] = verified
	result["needs_review"] = needsReview
	result["pending_nolm"] = pendingNoLM
	result["processed_nolm"] = processedNoLM
	result["pending_whisper_local"] = pendingWhisperLocal
	result["processed_whisper_local"] = processedWhisperLocal
	result["pending_whisper_openai"] = pendingWhisperOpenAI
	result["processed_whisper_openai"] = processedWhisperOpenAI

	return result, nil
}

func (db *DB) AvgMetrics() (avgWER, avgCER float64, err error) {
	err = db.conn.QueryRow(`
		SELECT 
			COALESCE(AVG(wer), 0),
			COALESCE(AVG(cer), 0)
		FROM audio_files 
		WHERE asr_status = 'processed'`).Scan(&avgWER, &avgCER)
	return
}

func (db *DB) GetFiles(page, limit int) ([]AudioFile, int, error) {
	var total int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM audio_files").Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * limit
	rows, err := db.conn.Query(`
		SELECT id, user_id, chapter_id, file_path, file_hash, duration_sec,
		       snr_db, rms_db, transcription_original, transcription_asr, 
		       wer, cer, asr_status
		FROM audio_files 
		ORDER BY id DESC
		LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		var transASR sql.NullString
		var wer, cer sql.NullFloat64
		err := rows.Scan(&af.ID, &af.UserID, &af.ChapterID, &af.FilePath,
			&af.FileHash, &af.DurationSec, &af.SNRDB, &af.RMSDB,
			&af.TranscriptionOriginal, &transASR, &wer, &cer, &af.ASRStatus)
		if err != nil {
			return nil, 0, err
		}
		if transASR.Valid {
			af.TranscriptionASR = transASR.String
		}
		if wer.Valid {
			af.WER = wer.Float64
		}
		if cer.Valid {
			af.CER = cer.Float64
		}
		files = append(files, af)
	}
	return files, total, nil
}

// GetFile - обновлённая версия с NoLM и верификацией
func (db *DB) GetFile(id int64) (*AudioFile, error) {
	var af AudioFile
	var transASR, transASRNoLM, transWhisperLocal, transWhisperOpenAI sql.NullString
	var wer, cer, werNoLM, cerNoLM, werWL, cerWL, werWO, cerWO sql.NullFloat64
	var verifiedAt sql.NullTime
	var asrNoLMStatus sql.NullString

	err := db.conn.QueryRow(`
		SELECT id, user_id, chapter_id, file_path, file_hash, duration_sec,
		       COALESCE(snr_db, 0), COALESCE(rms_db, 0), sample_rate, channels, bit_depth, file_size,
		       COALESCE(audio_metadata, ''), COALESCE(transcription_original, ''), 
		       transcription_asr, transcription_asr_nolm,
		       transcription_whisper_local, transcription_whisper_openai,
		       wer, cer, wer_nolm, cer_nolm,
		       wer_whisper_local, cer_whisper_local,
		       wer_whisper_openai, cer_whisper_openai,
		       asr_status, asr_nolm_status,
		       COALESCE(whisper_local_status, 'pending'), COALESCE(whisper_openai_status, 'pending'),
		       COALESCE(review_status, 'pending'),
		       COALESCE(operator_verified, 0), verified_at, COALESCE(original_edited, 0),
		       created_at
		FROM audio_files WHERE id = ?`, id).Scan(
		&af.ID, &af.UserID, &af.ChapterID, &af.FilePath, &af.FileHash,
		&af.DurationSec, &af.SNRDB, &af.RMSDB, &af.SampleRate, &af.Channels,
		&af.BitDepth, &af.FileSize, &af.AudioMetadata, &af.TranscriptionOriginal,
		&transASR, &transASRNoLM, &transWhisperLocal, &transWhisperOpenAI,
		&wer, &cer, &werNoLM, &cerNoLM, &werWL, &cerWL, &werWO, &cerWO,
		&af.ASRStatus, &asrNoLMStatus,
		&af.WhisperLocalStatus, &af.WhisperOpenAIStatus,
		&af.ReviewStatus,
		&af.OperatorVerified, &verifiedAt, &af.OriginalEdited,
		&af.CreatedAt)
	if err != nil {
		return nil, err
	}

	if transASR.Valid {
		af.TranscriptionASR = transASR.String
	}
	if transASRNoLM.Valid {
		af.TranscriptionASRNoLM = transASRNoLM.String
	}
	if transWhisperLocal.Valid {
		af.TranscriptionWhisperLocal = transWhisperLocal.String
	}
	if transWhisperOpenAI.Valid {
		af.TranscriptionWhisperOpenAI = transWhisperOpenAI.String
	}
	if wer.Valid {
		af.WER = wer.Float64
	}
	if cer.Valid {
		af.CER = cer.Float64
	}
	if werNoLM.Valid {
		af.WERNoLM = werNoLM.Float64
	}
	if cerNoLM.Valid {
		af.CERNoLM = cerNoLM.Float64
	}
	if werWL.Valid {
		af.WERWhisperLocal = werWL.Float64
	}
	if cerWL.Valid {
		af.CERWhisperLocal = cerWL.Float64
	}
	if werWO.Valid {
		af.WERWhisperOpenAI = werWO.Float64
	}
	if cerWO.Valid {
		af.CERWhisperOpenAI = cerWO.Float64
	}
	if asrNoLMStatus.Valid {
		af.ASRNoLMStatus = asrNoLMStatus.String
	} else {
		af.ASRNoLMStatus = "pending"
	}
	if verifiedAt.Valid {
		af.VerifiedAt = &verifiedAt.Time
	}

	return &af, nil
}

func (db *DB) GetFilesWithTotal(page, limit int, minWER float64) (*FileListResult, error) {
	offset := (page - 1) * limit

	// Count total
	var total int64
	countQuery := "SELECT COUNT(*) FROM audio_files"
	if minWER > 0 {
		countQuery += fmt.Sprintf(" WHERE wer >= %f", minWER)
	}
	db.conn.QueryRow(countQuery).Scan(&total)

	// Get files
	query := `SELECT id, user_id, chapter_id, file_path, file_hash, 
	          duration_sec, sample_rate, channels, bit_depth, file_size,
	          COALESCE(snr_db, 0), COALESCE(rms_db, 0),
	          COALESCE(transcription_original, ''), COALESCE(transcription_asr, ''),
	          COALESCE(transcription_whisper_local, ''), COALESCE(transcription_whisper_openai, ''),
	          COALESCE(wer, 0), COALESCE(cer, 0),
	          COALESCE(wer_whisper_local, 0), COALESCE(cer_whisper_local, 0),
	          COALESCE(wer_whisper_openai, 0), COALESCE(cer_whisper_openai, 0),
	          asr_status
	          FROM audio_files`

	if minWER > 0 {
		query += fmt.Sprintf(" WHERE wer >= %f", minWER)
	}
	query += " ORDER BY id DESC LIMIT ? OFFSET ?"

	rows, err := db.conn.Query(query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		err := rows.Scan(
			&af.ID, &af.UserID, &af.ChapterID, &af.FilePath, &af.FileHash,
			&af.DurationSec, &af.SampleRate, &af.Channels, &af.BitDepth, &af.FileSize,
			&af.SNRDB, &af.RMSDB,
			&af.TranscriptionOriginal, &af.TranscriptionASR,
			&af.TranscriptionWhisperLocal, &af.TranscriptionWhisperOpenAI,
			&af.WER, &af.CER,
			&af.WERWhisperLocal, &af.CERWhisperLocal,
			&af.WERWhisperOpenAI, &af.CERWhisperOpenAI,
			&af.ASRStatus,
		)
		if err != nil {
			return nil, err
		}
		files = append(files, af)
	}

	return &FileListResult{
		Files: files,
		Total: total,
		Page:  page,
		Limit: limit,
	}, nil
}

// GetAllFilePaths возвращает все пути файлов из базы
func (db *DB) GetAllFilePaths() (map[string]bool, error) {
	rows, err := db.conn.Query("SELECT file_path FROM audio_files")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	paths := make(map[string]bool)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		paths[path] = true
	}
	return paths, nil
}

func (db *DB) GetSpeakers() ([]string, error) {
	rows, err := db.conn.Query("SELECT DISTINCT user_id FROM audio_files ORDER BY user_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var speakers []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		speakers = append(speakers, s)
	}
	return speakers, nil
}

// GetFilesFiltered - обновлённая версия с фильтрами verified и asr_nolm_status
func (db *DB) GetFilesFiltered(page, limit int, speaker, werOp string, werValue float64, durOp string, durValue float64, asrStatus, asrNoLMStatus,
	whisperLocalStatus, whisperOpenaiStatus, verified, merged, active string) (*FileListResult, error) {
	offset := (page - 1) * limit

	var conditions []string
	var args []interface{}

	if speaker != "" {
		conditions = append(conditions, "user_id = ?")
		args = append(args, speaker)
	}

	// WER filter
	if werOp != "" && werValue >= 0 {
		werDecimal := werValue / 100.0
		switch werOp {
		case "lt":
			conditions = append(conditions, "wer < ?")
			args = append(args, werDecimal)
		case "gt":
			conditions = append(conditions, "wer > ?")
			args = append(args, werDecimal)
		case "eq":
			conditions = append(conditions, "wer = ?")
			args = append(args, werDecimal)
		}
	}

	// Duration filter
	if durOp != "" && durValue > 0 {
		switch durOp {
		case "lt":
			conditions = append(conditions, "duration_sec < ?")
			args = append(args, durValue)
		case "gt":
			conditions = append(conditions, "duration_sec > ?")
			args = append(args, durValue)
		}
	}

	// Status filters
	if asrStatus != "" {
		conditions = append(conditions, "asr_status = ?")
		args = append(args, asrStatus)
	}
	if asrNoLMStatus != "" {
		conditions = append(conditions, "asr_nolm_status = ?")
		args = append(args, asrNoLMStatus)
	}
	if whisperLocalStatus != "" {
		conditions = append(conditions, "whisper_local_status = ?")
		args = append(args, whisperLocalStatus)
	}
	if whisperOpenaiStatus != "" {
		conditions = append(conditions, "whisper_openai_status = ?")
		args = append(args, whisperOpenaiStatus)
	}

	// Verified filter
	switch verified {
	case "yes", "1":
		conditions = append(conditions, "operator_verified = 1")
	case "no", "0":
		conditions = append(conditions, "operator_verified = 0")
	}

	// Merged filter - добавь после verified filter
	switch merged {
	case "final":
		// НЕ добавляем active=1 здесь — это делает active фильтр ниже
	case "merged":
		conditions = append(conditions, "parent_ids IS NOT NULL")
	case "sources":
		conditions = append(conditions, "merged_id > 0")
	case "never":
		conditions = append(conditions, "merged_id = 0 AND parent_ids IS NULL")
	case "all":
		// Все файлы - без фильтра по merged
		// НЕТ default — пусть active фильтр решает
	}

	// Active filter — ОТДЕЛЬНО
	switch active {
	case "all":
		// показать все — ничего не добавляем
	case "no", "0":
		conditions = append(conditions, "active = 0")
	default:
		// по умолчанию только активные
		conditions = append(conditions, "active = 1")
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	var total int64
	countQuery := "SELECT COUNT(*) FROM audio_files " + whereClause
	db.conn.QueryRow(countQuery, args...).Scan(&total)

	query := `SELECT id, user_id, chapter_id, file_path, file_hash, 
              duration_sec, sample_rate, channels, bit_depth, file_size,
              COALESCE(snr_db, 0), COALESCE(rms_db, 0),
              COALESCE(transcription_original, ''), COALESCE(transcription_asr, ''),
              COALESCE(transcription_asr_nolm, ''),
              COALESCE(transcription_whisper_local, ''), COALESCE(transcription_whisper_openai, ''),
              COALESCE(wer, 0), COALESCE(cer, 0),
              COALESCE(wer_nolm, 0), COALESCE(cer_nolm, 0),
              COALESCE(wer_whisper_local, 0), COALESCE(cer_whisper_local, 0),
              COALESCE(wer_whisper_openai, 0), COALESCE(cer_whisper_openai, 0),
              asr_status, COALESCE(asr_nolm_status, 'pending'),
              COALESCE(whisper_local_status, 'pending'), COALESCE(whisper_openai_status, 'pending'),
              COALESCE(operator_verified, 0), COALESCE(original_edited, 0)
              FROM audio_files ` + whereClause + ` ORDER BY id DESC LIMIT ? OFFSET ?`

	args = append(args, limit, offset)
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		err := rows.Scan(
			&af.ID, &af.UserID, &af.ChapterID, &af.FilePath, &af.FileHash,
			&af.DurationSec, &af.SampleRate, &af.Channels, &af.BitDepth, &af.FileSize,
			&af.SNRDB, &af.RMSDB,
			&af.TranscriptionOriginal, &af.TranscriptionASR,
			&af.TranscriptionASRNoLM,
			&af.TranscriptionWhisperLocal, &af.TranscriptionWhisperOpenAI,
			&af.WER, &af.CER,
			&af.WERNoLM, &af.CERNoLM,
			&af.WERWhisperLocal, &af.CERWhisperLocal,
			&af.WERWhisperOpenAI, &af.CERWhisperOpenAI,
			&af.ASRStatus, &af.ASRNoLMStatus,
			&af.WhisperLocalStatus, &af.WhisperOpenAIStatus,
			&af.OperatorVerified, &af.OriginalEdited,
		)
		if err != nil {
			return nil, err
		}
		files = append(files, af)
	}

	return &FileListResult{
		Files: files,
		Total: total,
		Page:  page,
		Limit: limit,
	}, nil
}

// AvgMetricsAll - обновлённая версия с NoLM метриками
func (db *DB) AvgMetricsAll_old() (map[string]float64, error) {
	result := make(map[string]float64)

	var kaldiWer, kaldiCer float64
	var kaldiNoLMWer, kaldiNoLMCer float64
	var whisperLocalWer, whisperLocalCer float64
	var whisperOpenaiWer, whisperOpenaiCer float64

	// Kaldi
	db.conn.QueryRow(`
        SELECT COALESCE(AVG(wer), 0), COALESCE(AVG(cer), 0)
        FROM audio_files 
        WHERE asr_status = 'processed'`).Scan(&kaldiWer, &kaldiCer)

	// Kaldi NoLM
	db.conn.QueryRow(`
        SELECT COALESCE(AVG(wer_nolm), 0), COALESCE(AVG(cer_nolm), 0)
        FROM audio_files 
        WHERE asr_nolm_status = 'processed'`).Scan(&kaldiNoLMWer, &kaldiNoLMCer)

	// Whisper Local
	db.conn.QueryRow(`
        SELECT COALESCE(AVG(wer_whisper_local), 0), COALESCE(AVG(cer_whisper_local), 0)
        FROM audio_files 
        WHERE whisper_local_status = 'processed'`).Scan(&whisperLocalWer, &whisperLocalCer)

	// Whisper OpenAI
	db.conn.QueryRow(`
        SELECT COALESCE(AVG(wer_whisper_openai), 0), COALESCE(AVG(cer_whisper_openai), 0)
        FROM audio_files 
        WHERE whisper_openai_status = 'processed'`).Scan(&whisperOpenaiWer, &whisperOpenaiCer)

	result["kaldi_wer"] = kaldiWer
	result["kaldi_cer"] = kaldiCer
	result["kaldi_nolm_wer"] = kaldiNoLMWer
	result["kaldi_nolm_cer"] = kaldiNoLMCer
	result["whisper_local_wer"] = whisperLocalWer
	result["whisper_local_cer"] = whisperLocalCer
	result["whisper_openai_wer"] = whisperOpenaiWer
	result["whisper_openai_cer"] = whisperOpenaiCer

	return result, nil
}

func (db *DB) AvgMetricsAll() (map[string]float64, error) {
	result := make(map[string]float64)

	var kaldiWer, kaldiCer float64
	var kaldiNoLMWer, kaldiNoLMCer float64
	var whisperLocalWer, whisperLocalCer float64
	var whisperOpenaiWer, whisperOpenaiCer float64

	db.conn.QueryRow(`
        SELECT 
            COALESCE(AVG(CASE WHEN asr_status = 'processed' THEN wer END), 0),
            COALESCE(AVG(CASE WHEN asr_status = 'processed' THEN cer END), 0),
            COALESCE(AVG(CASE WHEN asr_nolm_status = 'processed' THEN wer_nolm END), 0),
            COALESCE(AVG(CASE WHEN asr_nolm_status = 'processed' THEN cer_nolm END), 0),
            COALESCE(AVG(CASE WHEN whisper_local_status = 'processed' THEN wer_whisper_local END), 0),
            COALESCE(AVG(CASE WHEN whisper_local_status = 'processed' THEN cer_whisper_local END), 0),
            COALESCE(AVG(CASE WHEN whisper_openai_status = 'processed' THEN wer_whisper_openai END), 0),
            COALESCE(AVG(CASE WHEN whisper_openai_status = 'processed' THEN cer_whisper_openai END), 0)
        FROM audio_files
    `).Scan(&kaldiWer, &kaldiCer, &kaldiNoLMWer, &kaldiNoLMCer,
		&whisperLocalWer, &whisperLocalCer, &whisperOpenaiWer, &whisperOpenaiCer)

	result["kaldi_wer"] = kaldiWer
	result["kaldi_cer"] = kaldiCer
	result["kaldi_nolm_wer"] = kaldiNoLMWer
	result["kaldi_nolm_cer"] = kaldiNoLMCer
	result["whisper_local_wer"] = whisperLocalWer
	result["whisper_local_cer"] = whisperLocalCer
	result["whisper_openai_wer"] = whisperOpenaiWer
	result["whisper_openai_cer"] = whisperOpenaiCer

	return result, nil
}

// GetFileForRecalc - обновлённая версия с NoLM
func (db *DB) GetFileForRecalc(id int64) (*AudioFileRecalc, error) {
	var af AudioFileRecalc
	err := db.conn.QueryRow(`
        SELECT id, 
               COALESCE(transcription_original, ''),
               COALESCE(transcription_asr, ''),
               COALESCE(transcription_asr_nolm, ''),
               COALESCE(transcription_whisper_local, ''),
               COALESCE(transcription_whisper_openai, '')
        FROM audio_files WHERE id = ?`, id).Scan(
		&af.ID, &af.TranscriptionOriginal, &af.TranscriptionASR,
		&af.TranscriptionASRNoLM,
		&af.TranscriptionWhisperLocal, &af.TranscriptionWhisperOpenAI)
	if err != nil {
		return nil, err
	}
	return &af, nil
}

// GetAllForRecalc - обновлённая версия с NoLM
func (db *DB) GetAllForRecalc() ([]AudioFileRecalc, error) {
	rows, err := db.conn.Query(`
        SELECT id, 
               COALESCE(transcription_original, ''),
               COALESCE(transcription_asr, ''),
               COALESCE(transcription_asr_nolm, ''),
               COALESCE(transcription_whisper_local, ''),
               COALESCE(transcription_whisper_openai, '')
        FROM audio_files 
        WHERE asr_status = 'processed' 
           OR asr_nolm_status = 'processed'
           OR whisper_local_status = 'processed' 
           OR whisper_openai_status = 'processed'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFileRecalc
	for rows.Next() {
		var af AudioFileRecalc
		if err := rows.Scan(&af.ID, &af.TranscriptionOriginal, &af.TranscriptionASR,
			&af.TranscriptionASRNoLM,
			&af.TranscriptionWhisperLocal, &af.TranscriptionWhisperOpenAI); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

// ============================================================
// МЕТОДЫ ДЛЯ NoLM (Kaldi без языковой модели)
// ============================================================

// GetPendingNoLM возвращает файлы для обработки Kaldi без LM
func (db *DB) GetPendingNoLM(limit int) ([]AudioFile, error) {
	query := `
		SELECT id, file_path, file_hash, transcription_original 
		FROM audio_files 
		WHERE asr_nolm_status = 'pending'`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath, &af.FileHash, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

// ============================================================
// МЕТОДЫ ДЛЯ верификации и редактирования
// ============================================================

// UpdateOriginalTranscription обновляет оригинальную транскрипцию (редактирование оператором)
func (db *DB) UpdateOriginalTranscription(id int64, text string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_original = ?, original_edited = 1
		WHERE id = ?`, text, id)
	return err
}

// SetVerificationStatus устанавливает/снимает статус верификации
func (db *DB) SetVerificationStatus(id int64, verified bool) error {
	var verifiedAt interface{}
	if verified {
		verifiedAt = time.Now()
	} else {
		verifiedAt = nil
	}

	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET operator_verified = ?, verified_at = ?
		WHERE id = ?`, verified, verifiedAt, id)
	return err
}
```

## File: ./internal/db/merge.go
```go
package db

import (
	"database/sql"
	"fmt"
	"strings"
)

// InsertMerged вставляет объединённый файл с parent_ids
func (db *DB) InsertMerged(af *AudioFile, parentIDs string) (int64, error) {
	res, err := db.conn.Exec(`
		INSERT INTO audio_files 
		(user_id, chapter_id, file_path, file_hash, duration_sec, 
		 snr_db, snr_sox, snr_wada, noise_level, rms_db,
		 sample_rate, channels, bit_depth, file_size, audio_metadata, 
		 transcription_original, parent_ids,
		 asr_status, asr_nolm_status, whisper_local_status, whisper_openai_status, review_status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
		        'pending', 'pending', 'pending', 'pending', 'pending')`,
		af.UserID, af.ChapterID, af.FilePath, af.FileHash, af.DurationSec,
		af.SNRDB, af.SNRSox, af.SNRWada, af.NoiseLevel, af.RMSDB,
		af.SampleRate, af.Channels, af.BitDepth, af.FileSize,
		af.AudioMetadata, af.TranscriptionOriginal, parentIDs)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// GetShortFilesBySpeaker получает короткие файлы сгруппированные по спикеру
func (db *DB) GetShortFilesBySpeaker(maxDuration float64, limit int) (map[string][]AudioFile, error) {
	query := `
		SELECT id, user_id, chapter_id, file_path, duration_sec, 
		       transcription_original, has_trailing_silence
		FROM audio_files 
		WHERE duration_sec < ? AND parent_ids IS NULL
		ORDER BY user_id, id`

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query, maxDuration)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]AudioFile)
	for rows.Next() {
		var af AudioFile
		var hasSilence sql.NullBool
		if err := rows.Scan(&af.ID, &af.UserID, &af.ChapterID, &af.FilePath,
			&af.DurationSec, &af.TranscriptionOriginal, &hasSilence); err != nil {
			return nil, err
		}
		if hasSilence.Valid {
			af.HasTrailingSilence = hasSilence.Bool
		}
		result[af.UserID] = append(result[af.UserID], af)
	}

	return result, nil
}

// UpdateSilenceStatus обновляет статус тишины
func (db *DB) UpdateSilenceStatus(id int64, hasSilence bool, silenceAdded bool) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET has_trailing_silence = ?, silence_added = ?
		WHERE id = ?`, hasSilence, silenceAdded, id)
	return err
}

// UpdateFilePath обновляет путь к файлу (после добавления/удаления тишины)
func (db *DB) UpdateFilePath(id int64, newPath string, newDuration float64, newHash string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET file_path = ?, duration_sec = ?, file_hash = ?
		WHERE id = ?`, newPath, newDuration, newHash, id)
	return err
}

// UpdateMergedID помечает файлы как объединённые
func (db *DB) UpdateMergedID(ids []int64, mergedID int64) error {
	if len(ids) == 0 {
		return nil
	}

	// Строим placeholders: ?,?,?
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids)+1)
	args[0] = mergedID

	for i, id := range ids {
		placeholders[i] = "?"
		args[i+1] = id
	}

	query := fmt.Sprintf(`
		UPDATE audio_files 
		SET merged_id = ?
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	_, err := db.conn.Exec(query, args...)
	return err
}

// GetNextChapterID возвращает следующий chapter_id для спикера
func (db *DB) GetNextChapterID(speakerID string) (string, error) {
	var maxChapter sql.NullString

	err := db.conn.QueryRow(`
		SELECT MAX(chapter_id) 
		FROM audio_files 
		WHERE user_id = ?`, speakerID).Scan(&maxChapter)

	if err != nil {
		return "", err
	}

	// Если нет записей - начинаем с 600000001 (чтобы отличались от оригинальных 500000xxx)
	if !maxChapter.Valid || maxChapter.String == "" {
		return "600000001", nil
	}

	// Парсим текущий максимум и увеличиваем
	var maxNum int64
	fmt.Sscanf(maxChapter.String, "%d", &maxNum)

	// Если оригинальные chapter_id в формате 500000xxx,
	// для merged используем 600000xxx
	if maxNum < 600000000 {
		return "600000001", nil
	}

	return fmt.Sprintf("%d", maxNum+1), nil
}

// DeactivateFiles помечает файлы как неактивные (после merge)
func (db *DB) DeactivateFiles(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))

	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`UPDATE audio_files SET active = 0 WHERE id IN (%s)`,
		strings.Join(placeholders, ","))

	_, err := db.conn.Exec(query, args...)
	return err
}
```

## File: ./internal/db/whisper.go
```go
package db

import "fmt"

// === Whisper Local ===

func (db *DB) GetWhisperLocalPending(limit int) ([]AudioFile, error) {
	query := `SELECT id, file_path, transcription_original 
	          FROM audio_files 
	          WHERE whisper_local_status = 'pending'`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

func (db *DB) UpdateWhisperLocal(id int64, transcription string, wer, cer float64) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_whisper_local = ?, 
		    wer_whisper_local = ?, 
		    cer_whisper_local = ?, 
		    whisper_local_status = 'processed'
		WHERE id = ?`,
		transcription, wer, cer, id)
	return err
}

func (db *DB) UpdateWhisperLocalError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files SET whisper_local_status = 'error' WHERE id = ?`, id)
	return err
}

// === Whisper OpenAI ===

// GetWhisperOpenAIPendingAll получает ВСЕ pending файлы для OpenAI (без фильтра)
func (db *DB) GetWhisperOpenAIPendingAll(limit int) ([]AudioFile, error) {
	query := `SELECT id, file_path, transcription_original 
	          FROM audio_files 
	          WHERE whisper_openai_status = 'pending'`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

// GetWhisperOpenAIPending получает файлы где локальный WER > minLocalWER
func (db *DB) GetWhisperOpenAIPending(limit int, minLocalWER float64) ([]AudioFile, error) {
	query := `SELECT id, file_path, transcription_original 
	          FROM audio_files 
	          WHERE whisper_openai_status = 'pending'
	            AND whisper_local_status = 'processed'
	            AND wer_whisper_local > ?`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query, minLocalWER)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

func (db *DB) UpdateWhisperOpenAI(id int64, transcription string, wer, cer float64) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_whisper_openai = ?, 
		    wer_whisper_openai = ?, 
		    cer_whisper_openai = ?, 
		    whisper_openai_status = 'processed'
		WHERE id = ?`,
		transcription, wer, cer, id)
	return err
}

func (db *DB) UpdateWhisperOpenAIError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files SET whisper_openai_status = 'error' WHERE id = ?`, id)
	return err
}

func (db *DB) UpdateWhisperLocalMetrics(id int64, wer, cer float64) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET wer_whisper_local = ?, cer_whisper_local = ? WHERE id = ?`, wer, cer, id)
	return err
}

func (db *DB) UpdateWhisperOpenAIMetrics(id int64, wer, cer float64) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET wer_whisper_openai = ?, cer_whisper_openai = ? WHERE id = ?`, wer, cer, id)
	return err
}
```

## File: ./internal/metrics/wer.go
```go
package metrics

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// normalizeText - убирает пунктуацию, приводит к нижнему регистру
func normalizeText(text string) string {
	// Приводим к нижнему регистру
	text = strings.ToLower(text)

	// Убираем знаки препинания и спецсимволы
	reg := regexp.MustCompile(`[.,!?;:"""'''\-–—()[\]{}«»…/\\@#$%^&*+=<>|~` + "`" + `]`)
	text = reg.ReplaceAllString(text, " ")

	// Убираем множественные пробелы
	spaceReg := regexp.MustCompile(`\s+`)
	text = spaceReg.ReplaceAllString(text, " ")

	return strings.TrimSpace(text)
}

// WER - Word Error Rate
func WER(reference, hypothesis string) float64 {
	// Нормализуем оба текста
	reference = normalizeText(reference)
	hypothesis = normalizeText(hypothesis)

	refWords := strings.Fields(reference)
	hypWords := strings.Fields(hypothesis)

	if len(refWords) == 0 {
		if len(hypWords) == 0 {
			return 0
		}
		return 1
	}

	d := levenshteinWords(refWords, hypWords)
	return float64(d) / float64(len(refWords))
}

// CER - Character Error Rate
func CER(reference, hypothesis string) float64 {
	// Нормализуем оба текста
	reference = normalizeText(reference)
	hypothesis = normalizeText(hypothesis)

	// Убираем пробелы для CER
	ref := strings.ReplaceAll(reference, " ", "")
	hyp := strings.ReplaceAll(hypothesis, " ", "")

	if utf8.RuneCountInString(ref) == 0 {
		if utf8.RuneCountInString(hyp) == 0 {
			return 0
		}
		return 1
	}

	d := levenshteinRunes([]rune(ref), []rune(hyp))
	return float64(d) / float64(utf8.RuneCountInString(ref))
}

func levenshteinWords(a, b []string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)

	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min(
				prev[j]+1,      // deletion
				curr[j-1]+1,    // insertion
				prev[j-1]+cost, // substitution
			)
		}
		prev, curr = curr, prev
	}

	return prev[len(b)]
}

func levenshteinRunes(a, b []rune) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)

	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min(
				prev[j]+1,
				curr[j-1]+1,
				prev[j-1]+cost,
			)
		}
		prev, curr = curr, prev
	}

	return prev[len(b)]
}
```

## File: ./internal/scanner/librispeech.go
```go
package scanner

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

type AudioTask struct {
	UserID        string
	ChapterID     string
	WavPath       string
	Transcription string
}

// Сканирует LibriSpeech структуру
func ScanLibriSpeech(rootDir string, limit int) ([]AudioTask, error) {
	var tasks []AudioTask
	count := 0

	users, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, err
	}

	for _, user := range users {
		if !user.IsDir() {
			continue
		}
		userID := user.Name()
		userPath := filepath.Join(rootDir, userID)

		chapters, err := os.ReadDir(userPath)
		if err != nil {
			continue
		}

		for _, chapter := range chapters {
			if !chapter.IsDir() {
				continue
			}
			chapterID := chapter.Name()
			chapterPath := filepath.Join(userPath, chapterID)

			transFile := filepath.Join(chapterPath, userID+"-"+chapterID+".trans.txt")
			transcriptions, err := parseTransFile(transFile)
			if err != nil {
				continue
			}

			for id, text := range transcriptions {
				wavPath := filepath.Join(chapterPath, id+".wav")
				if _, err := os.Stat(wavPath); err == nil {
					tasks = append(tasks, AudioTask{
						UserID:        userID,
						ChapterID:     chapterID,
						WavPath:       wavPath,
						Transcription: text,
					})
					count++
					if limit > 0 && count >= limit {
						return tasks, nil
					}
				}
			}
		}
	}

	return tasks, nil
}

func parseTransFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	result := make(map[string]string)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}

	return result, scanner.Err()
}

func CountFiles(rootDir string) (int, error) {
	count := 0
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".wav") {
			count++
		}
		return nil
	})
	return count, err
}
```

## File: ./internal/service/asr.go
```go
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
```

## File: ./internal/service/asr_gpu.go
```go
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

// ASRGPUService обрабатывает файлы через Kaldi GPU batch
type ASRGPUService struct {
	db             *db.DB
	decoder        *asr.KaldiDecoder
	defaultWorkers int
	batchSize      int
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

func NewASRGPUService(database *db.DB, modelDir string, batchSize int) (*ASRGPUService, error) {
	decoder, err := asr.NewKaldiDecoder(modelDir)
	if err != nil {
		return nil, err
	}

	if batchSize <= 0 {
		batchSize = 32
	}

	return &ASRGPUService{
		db:        database,
		decoder:   decoder,
		batchSize: batchSize,
	}, nil
}

func (s *ASRGPUService) Start(limit int) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("ASR GPU already running")
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

func (s *ASRGPUService) Stop() {
	atomic.StoreInt32(&s.stopFlag, 1)
}

func (s *ASRGPUService) setLastError(err string) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

func (s *ASRGPUService) addWER(wer float64) {
	s.mu.Lock()
	s.totalWER += wer
	s.mu.Unlock()
}

func (s *ASRGPUService) Status() ASRStatus {
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

func (s *ASRGPUService) run(limit int) {
	defer atomic.StoreInt32(&s.running, 0)

	files, err := s.db.GetPending(limit)
	if err != nil {
		s.setLastError("get pending: " + err.Error())
		log.Printf("ASR GPU get pending error: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("ASR GPU: no pending files")
		return
	}

	atomic.StoreInt64(&s.total, int64(len(files)))
	log.Printf("ASR GPU: processing %d files in batches of %d", len(files), s.batchSize)

	// Обрабатываем батчами
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

	log.Printf("ASR GPU complete: processed=%d errors=%d avgWER=%.2f%%",
		processed,
		atomic.LoadInt64(&s.errors),
		avgWER)
}

func (s *ASRGPUService) processBatch(files []db.AudioFile) {
	// Собираем пути
	paths := make([]string, len(files))
	fileMap := make(map[string]*db.AudioFile)

	for i, f := range files {
		paths[i] = f.FilePath
		fileMap[f.FilePath] = &files[i]
	}

	// GPU batch декодинг
	results, err := s.decoder.DecodeBatchGPU(paths)
	if err != nil {
		s.setLastError("GPU batch decode: " + err.Error())
		log.Printf("ASR GPU batch error: %v", err)
		for _, f := range files {
			s.db.UpdateError(f.ID, err.Error())
			atomic.AddInt64(&s.errors, 1)
		}
		return
	}

	// Обрабатываем результаты
	for path, result := range results {
		file := fileMap[path]
		if file == nil {
			continue
		}

		if !result.Success {
			s.setLastError(result.Error)
			s.db.UpdateError(file.ID, result.Error)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		wer := metrics.WER(file.TranscriptionOriginal, result.Text)
		cer := metrics.CER(file.TranscriptionOriginal, result.Text)

		err := s.db.UpdateASR(file.ID, result.Text, wer, cer)
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
```

## File: ./internal/service/asr_gpu_nolm.go
```go
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
```

## File: ./internal/service/asr_nolm.go
```go
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

// ASRNoLMService обрабатывает файлы через Kaldi без языковой модели (lm-scale=0)
type ASRNoLMService struct {
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

func NewASRNoLMService(database *db.DB, modelDir string, defaultWorkers int) (*ASRNoLMService, error) {
	decoder, err := asr.NewKaldiDecoderNoLM(modelDir)
	if err != nil {
		return nil, err
	}

	return &ASRNoLMService{
		db:             database,
		decoder:        decoder,
		defaultWorkers: defaultWorkers,
	}, nil
}

func (s *ASRNoLMService) Start(limit, workers int) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("ASR NoLM already running")
	}

	if workers <= 0 {
		workers = s.defaultWorkers
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

	go s.run(limit, workers)
	return nil
}

func (s *ASRNoLMService) Stop() {
	atomic.StoreInt32(&s.stopFlag, 1)
}

func (s *ASRNoLMService) setLastError(err string) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

func (s *ASRNoLMService) addWER(wer float64) {
	s.mu.Lock()
	s.totalWER += wer
	s.mu.Unlock()
}

func (s *ASRNoLMService) Status() ASRStatus {
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

func (s *ASRNoLMService) run(limit, workers int) {
	defer atomic.StoreInt32(&s.running, 0)

	files, err := s.db.GetPendingNoLM(limit)
	if err != nil {
		s.setLastError("get pending: " + err.Error())
		log.Printf("ASR NoLM get pending error: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("ASR NoLM: no pending files")
		return
	}

	atomic.StoreInt64(&s.total, int64(len(files)))
	log.Printf("ASR NoLM: processing %d files with %d workers (lm-scale=0)", len(files), workers)

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

	log.Printf("ASR NoLM complete: processed=%d errors=%d avgWER=%.2f%%",
		processed,
		atomic.LoadInt64(&s.errors),
		avgWER)
}

func (s *ASRNoLMService) worker(wg *sync.WaitGroup, tasks <-chan db.AudioFile) {
	defer wg.Done()

	for file := range tasks {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			return
		}

		result, err := s.decoder.Decode(file.FilePath)
		if err != nil {
			s.setLastError("decode: " + err.Error())
			log.Printf("ASR NoLM error %s: %v", file.FilePath, err)
			s.db.UpdateASRNoLMError(file.ID, err.Error())
			atomic.AddInt64(&s.errors, 1)
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

		err = s.db.UpdateASRNoLM(file.ID, result.Text, wer, cer)
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

func (s *ASRNoLMService) ProcessSingle(filePath string) (string, error) {
	result, err := s.decoder.Decode(filePath)
	if err != nil {
		return "", err
	}
	if !result.Success {
		return "", errors.New(result.Error)
	}
	return result.Text, nil
}
```

## File: ./internal/service/merge.go
```go
package service

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"audio-labeler/internal/audio"
	"audio-labeler/internal/db"
)

type MergeService struct {
	db        *db.DB
	outputDir string
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

		// Проверяем что все от одного спикера
		if speakerID == "" {
			speakerID = file.UserID
		} else if file.UserID != speakerID {
			return nil, fmt.Errorf("all files must be from same speaker (expected %s, got %s)", speakerID, file.UserID)
		}

		// Проверяем WER = 0 (или не обработан)
		if file.ASRStatus == "processed" && file.WER > 0 {
			return nil, fmt.Errorf("file %d has WER %.2f%% (must be 0%% to merge)", id, file.WER*100)
		}

		files = append(files, file)
	}

	// Получаем следующий chapter_id для этого спикера
	nextChapterID, err := s.db.GetNextChapterID(speakerID)
	if err != nil {
		return nil, fmt.Errorf("get next chapter_id failed: %w", err)
	}

	// Собираем пути и транскрипции
	paths := make([]string, len(files))
	transcriptions := make([]string, len(files))
	idStrings := make([]string, len(files))
	var totalDuration float64

	for i, f := range files {
		paths[i] = f.FilePath
		transcriptions[i] = f.TranscriptionOriginal
		idStrings[i] = fmt.Sprintf("%d", f.ID)
		totalDuration += f.DurationSec
	}

	// Создаём структуру LibriSpeech:
	// /data/merged/speaker_id/chapter_id/speaker_id-chapter_id-0000.wav
	chapterDir := filepath.Join(outputDir, speakerID, nextChapterID)
	if err := os.MkdirAll(chapterDir, 0755); err != nil {
		return nil, fmt.Errorf("create dir failed: %w", err)
	}

	// Имя файла в формате LibriSpeech
	baseName := fmt.Sprintf("%s-%s-0000", speakerID, nextChapterID)
	outputPath := filepath.Join(chapterDir, baseName+".wav")
	transPath := filepath.Join(chapterDir, fmt.Sprintf("%s-%s.trans.txt", speakerID, nextChapterID))

	// Склеиваем аудио
	if err := audio.MergeAudioFiles(paths, outputPath, 150); err != nil {
		return nil, fmt.Errorf("merge failed: %w", err)
	}

	// Объединяем транскрипции (убираем лишние пробелы)
	mergedTranscription := strings.TrimSpace(strings.Join(transcriptions, " "))
	parentIDs := strings.Join(idStrings, "|")

	// Создаём trans.txt в формате LibriSpeech
	transContent := fmt.Sprintf("%s %s\n", baseName, mergedTranscription)
	if err := os.WriteFile(transPath, []byte(transContent), 0644); err != nil {
		return nil, fmt.Errorf("write trans.txt failed: %w", err)
	}

	// Получаем метаданные нового файла
	meta, err := audio.GetMetadata(outputPath)
	if err != nil {
		return nil, fmt.Errorf("get metadata failed: %w", err)
	}

	stats, _ := audio.GetStats(outputPath)

	// Создаём новую запись в БД
	newFile := &db.AudioFile{
		UserID:                speakerID,
		ChapterID:             nextChapterID,
		FilePath:              outputPath,
		FileHash:              "",
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

	// Hash
	hash, _ := audio.MD5File(outputPath)
	newFile.FileHash = hash

	// Вставляем с parent_ids
	newID, err := s.db.InsertMerged(newFile, parentIDs)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	// Помечаем исходные файлы как объединённые
	if err := s.db.UpdateMergedID(ids, newID); err != nil {
		log.Printf("Warning: failed to update merged_id for source files: %v", err)
	}

	// Деактивируем исходные файлы
	if err := s.db.DeactivateFiles(ids); err != nil {
		log.Printf("Warning: failed to deactivate source files: %v", err)
	}

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
```

## File: ./internal/service/scanner.go
```go
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
```

## File: ./internal/service/whisper_local.go
```go
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

type WhisperLocalStatus struct {
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

type WhisperLocalService struct {
	db             *db.DB
	client         *asr.WhisperLocalClient
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

func NewWhisperLocalService(database *db.DB, baseURL, language string, defaultWorkers int) *WhisperLocalService {
	return &WhisperLocalService{
		db:             database,
		client:         asr.NewWhisperLocalClient(baseURL, language),
		defaultWorkers: defaultWorkers,
	}
}

func (s *WhisperLocalService) Start(limit, workers int) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("Whisper Local already running")
	}

	if workers <= 0 {
		workers = s.defaultWorkers
	}

	if err := s.client.Health(); err != nil {
		atomic.StoreInt32(&s.running, 0)
		return errors.New("Whisper Local not available: " + err.Error())
	}

	atomic.StoreInt64(&s.processed, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt32(&s.stopFlag, 0)
	s.totalWER = 0
	s.lastError = ""
	s.startTime = time.Now()

	go s.run(limit, workers)
	return nil
}

func (s *WhisperLocalService) Stop() {
	atomic.StoreInt32(&s.stopFlag, 1)
}

func (s *WhisperLocalService) setLastError(err string) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

func (s *WhisperLocalService) addWER(wer float64) {
	s.mu.Lock()
	s.totalWER += wer
	s.mu.Unlock()
}

func (s *WhisperLocalService) Status() WhisperLocalStatus {
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
		avgWER = s.totalWER / float64(p) * 100
	}
	lastErr := s.lastError
	s.mu.Unlock()

	return WhisperLocalStatus{
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

func (s *WhisperLocalService) run(limit, workers int) {
	defer atomic.StoreInt32(&s.running, 0)

	files, err := s.db.GetWhisperLocalPending(limit)
	if err != nil {
		s.setLastError("get pending: " + err.Error())
		log.Printf("Whisper Local get pending error: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("Whisper Local: no pending files")
		return
	}

	atomic.StoreInt64(&s.total, int64(len(files)))
	log.Printf("Whisper Local: processing %d files with %d workers", len(files), workers)

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

	log.Printf("Whisper Local complete: processed=%d errors=%d avgWER=%.2f%%",
		processed, atomic.LoadInt64(&s.errors), avgWER)
}

func (s *WhisperLocalService) worker(wg *sync.WaitGroup, tasks <-chan db.AudioFile) {
	defer wg.Done()

	for file := range tasks {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			return
		}

		result, err := s.client.Transcribe(file.FilePath)
		if err != nil {
			s.setLastError("transcribe: " + err.Error())
			log.Printf("Whisper Local error %s: %v", file.FilePath, err)
			s.db.UpdateWhisperLocalError(file.ID, err.Error())
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		if !result.Success {
			s.setLastError(result.Error)
			s.db.UpdateWhisperLocalError(file.ID, result.Error)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		wer := metrics.WER(file.TranscriptionOriginal, result.Text)
		cer := metrics.CER(file.TranscriptionOriginal, result.Text)

		err = s.db.UpdateWhisperLocal(file.ID, result.Text, wer, cer)
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

func (s *WhisperLocalService) ProcessSingle(filePath string) (string, error) {
	result, err := s.client.Transcribe(filePath)
	if err != nil {
		return "", err
	}
	if !result.Success {
		return "", errors.New(result.Error)
	}
	return result.Text, nil
}
```

## File: ./internal/service/whisper_openai.go
```go
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

type WhisperOpenAIStatus struct {
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

type WhisperOpenAIService struct {
	db             *db.DB
	client         *asr.WhisperOpenAIClient
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

func NewWhisperOpenAIService(database *db.DB, apiKey, model, language string, defaultWorkers int) *WhisperOpenAIService {
	return &WhisperOpenAIService{
		db:             database,
		client:         asr.NewWhisperOpenAIClient(apiKey, model, language),
		defaultWorkers: defaultWorkers,
	}
}

func (s *WhisperOpenAIService) Start(limit, workers int, minLocalWER float64) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("Whisper OpenAI already running")
	}

	if workers <= 0 {
		workers = s.defaultWorkers
	}

	if err := s.client.Health(); err != nil {
		atomic.StoreInt32(&s.running, 0)
		return errors.New("Whisper OpenAI not available: " + err.Error())
	}

	atomic.StoreInt64(&s.processed, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt32(&s.stopFlag, 0)
	s.totalWER = 0
	s.lastError = ""
	s.startTime = time.Now()

	go s.run(limit, workers, minLocalWER)
	return nil
}

func (s *WhisperOpenAIService) Stop() {
	atomic.StoreInt32(&s.stopFlag, 1)
}

func (s *WhisperOpenAIService) setLastError(err string) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

func (s *WhisperOpenAIService) addWER(wer float64) {
	s.mu.Lock()
	s.totalWER += wer
	s.mu.Unlock()
}

func (s *WhisperOpenAIService) Status() WhisperOpenAIStatus {
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
		avgWER = s.totalWER / float64(p) * 100
	}
	lastErr := s.lastError
	s.mu.Unlock()

	return WhisperOpenAIStatus{
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

func (s *WhisperOpenAIService) run(limit, workers int, minLocalWER float64) {
	defer atomic.StoreInt32(&s.running, 0)

	// Получаем файлы где локальный Whisper уже обработал и WER > minLocalWER
	files, err := s.db.GetWhisperOpenAIPending(limit, minLocalWER)
	if err != nil {
		s.setLastError("get pending: " + err.Error())
		log.Printf("Whisper OpenAI get pending error: %v", err)
		return
	}

	if len(files) == 0 {
		log.Printf("Whisper OpenAI: no pending files (minLocalWER=%.2f%%)", minLocalWER*100)
		return
	}

	atomic.StoreInt64(&s.total, int64(len(files)))
	log.Printf("Whisper OpenAI: processing %d files with %d workers (minLocalWER=%.2f%%)",
		len(files), workers, minLocalWER*100)

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

	log.Printf("Whisper OpenAI complete: processed=%d errors=%d avgWER=%.2f%%",
		processed, atomic.LoadInt64(&s.errors), avgWER)
}

func (s *WhisperOpenAIService) worker(wg *sync.WaitGroup, tasks <-chan db.AudioFile) {
	defer wg.Done()

	for file := range tasks {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			return
		}

		result, err := s.client.Transcribe(file.FilePath)
		if err != nil {
			s.setLastError("transcribe: " + err.Error())
			log.Printf("Whisper OpenAI error %s: %v", file.FilePath, err)
			s.db.UpdateWhisperOpenAIError(file.ID, err.Error())
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		if !result.Success {
			s.setLastError(result.Error)
			s.db.UpdateWhisperOpenAIError(file.ID, result.Error)
			atomic.AddInt64(&s.errors, 1)
			continue
		}

		wer := metrics.WER(file.TranscriptionOriginal, result.Text)
		cer := metrics.CER(file.TranscriptionOriginal, result.Text)

		err = s.db.UpdateWhisperOpenAI(file.ID, result.Text, wer, cer)
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

// StartForced запускает обработку ВСЕХ pending файлов без фильтра
func (s *WhisperOpenAIService) StartForced(limit, workers int) error {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return errors.New("Whisper OpenAI already running")
	}

	if workers <= 0 {
		workers = s.defaultWorkers
	}

	if err := s.client.Health(); err != nil {
		atomic.StoreInt32(&s.running, 0)
		return errors.New("Whisper OpenAI not available: " + err.Error())
	}

	atomic.StoreInt64(&s.processed, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt32(&s.stopFlag, 0)
	s.totalWER = 0
	s.lastError = ""
	s.startTime = time.Now()

	go s.runForced(limit, workers)
	return nil
}

func (s *WhisperOpenAIService) runForced(limit, workers int) {
	defer atomic.StoreInt32(&s.running, 0)

	files, err := s.db.GetWhisperOpenAIPendingAll(limit)
	if err != nil {
		s.setLastError("get pending: " + err.Error())
		log.Printf("Whisper OpenAI get pending error: %v", err)
		return
	}

	if len(files) == 0 {
		log.Println("Whisper OpenAI FORCED: no pending files")
		return
	}

	atomic.StoreInt64(&s.total, int64(len(files)))
	log.Printf("Whisper OpenAI FORCED: processing %d files with %d workers", len(files), workers)

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

	log.Printf("Whisper OpenAI FORCED complete: processed=%d errors=%d avgWER=%.2f%%",
		processed, atomic.LoadInt64(&s.errors), avgWER)
}

func (s *WhisperOpenAIService) ProcessSingle(filePath string) (string, error) {
	result, err := s.client.Transcribe(filePath)
	if err != nil {
		return "", err
	}
	if !result.Success {
		return "", errors.New(result.Error)
	}
	return result.Text, nil
}
```

## File: ./scripts/create_snapshots.sh
```bash
#!/bin/bash
OUTPUT="snapshots/labeler_snapshot_$(date +%Y%m%d_%H%M%S).md"

echo "# Perl Compiler Snapshot - $(date)" > "$OUTPUT"
echo "" >> "$OUTPUT"

# Project structure
echo "## Project Structure" >> "$OUTPUT"
echo '```' >> "$OUTPUT"
find . -type f \( -name "*.go" -o -name "*.pl" -o -name "*.sh" \) | grep -v vendor | sort >> "$OUTPUT"
echo '```' >> "$OUTPUT"
echo "" >> "$OUTPUT"

# All Go files
for f in $(find . -name "*.go" | grep -v vendor | sort); do
    echo "## File: $f" >> "$OUTPUT"
    echo '```go' >> "$OUTPUT"
    cat "$f" >> "$OUTPUT"
    echo '```' >> "$OUTPUT"
    echo "" >> "$OUTPUT"
done

# Test files
for f in $(find . -name "*.pl" | sort); do
    echo "## File: $f" >> "$OUTPUT"
    echo '```perl' >> "$OUTPUT"
    cat "$f" >> "$OUTPUT"
    echo '```' >> "$OUTPUT"
    echo "" >> "$OUTPUT"
done

# Shell scripts
for f in $(find . -name "*.sh" | sort); do
    echo "## File: $f" >> "$OUTPUT"
    echo '```bash' >> "$OUTPUT"
    cat "$f" >> "$OUTPUT"
    echo '```' >> "$OUTPUT"
    echo "" >> "$OUTPUT"
done

echo "Snapshot created: $OUTPUT"
wc -l "$OUTPUT"```

