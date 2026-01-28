package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"audio-labeler/internal/audio"
	"audio-labeler/internal/db"
	"audio-labeler/internal/metrics"
	"audio-labeler/internal/service"
)

type Handlers struct {
	db              *db.DB
	scanner         *service.Scanner
	asr             *service.ASRService
	asrNoLM         *service.ASRNoLMService
	asrGPU          *service.ASRGPUService
	asrGPUNoLM      *service.ASRGPUNoLMService
	whisperLocal    *service.WhisperLocalService
	whisperOpenAI   *service.WhisperOpenAIService
	mergeService    *service.MergeService
	segmentHandlers *SegmentHandlers
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

// === Health handler ===

func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	asrOK := h.asr != nil
	h.success(w, map[string]interface{}{
		"status": "ok",
		"asr":    asrOK,
	})
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

// TrimAudio - POST /api/files/{id}/trim
// Обрезает аудио, оставляя только выбранный диапазон
func (h *Handlers) TrimAudio(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	var req struct {
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if req.End <= req.Start {
		h.error(w, http.StatusBadRequest, "end must be greater than start")
		return
	}

	file, err := h.db.GetFile(id)
	if err != nil {
		h.error(w, http.StatusNotFound, "file not found")
		return
	}

	// Создаём временный файл
	tmpPath := file.FilePath + ".tmp.wav"
	duration := req.End - req.Start

	cmd := exec.Command("ffmpeg", "-y",
		"-i", file.FilePath,
		"-ss", fmt.Sprintf("%.3f", req.Start),
		"-t", fmt.Sprintf("%.3f", duration),
		"-c:a", "pcm_s16le",
		"-ar", fmt.Sprintf("%d", file.SampleRate),
		"-ac", fmt.Sprintf("%d", file.Channels),
		tmpPath,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		h.error(w, http.StatusInternalServerError, "ffmpeg error: "+string(output))
		return
	}

	// Заменяем оригинал
	if err := os.Rename(tmpPath, file.FilePath); err != nil {
		h.error(w, http.StatusInternalServerError, "rename error: "+err.Error())
		return
	}

	// Вычисляем новый hash
	newHash, _ := audio.MD5File(file.FilePath)

	// Обновляем в БД
	err = h.db.UpdateFilePath(id, file.FilePath, duration, newHash)
	if err != nil {
		h.error(w, http.StatusInternalServerError, "db error: "+err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message":      "Audio trimmed",
		"new_duration": duration,
		"start":        req.Start,
		"end":          req.End,
	})
}
