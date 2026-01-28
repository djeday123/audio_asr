package api

import (
	"audio-labeler/internal/audio"
	"log"
	"net/http"
	"strconv"
	"sync"
)

// AnalyzeFile - POST /api/files/{id}/analyze
// Запускает анализ аудио (SNR, RMS, noise level)
func (h *Handlers) AnalyzeFile(w http.ResponseWriter, r *http.Request) {
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

	// Запускаем анализ
	stats, err := audio.GetStats(file.FilePath)
	if err != nil {
		h.error(w, http.StatusInternalServerError, "analyze error: "+err.Error())
		return
	}

	// Обновляем в БД
	err = h.db.UpdateAudioStats(id, stats)
	if err != nil {
		h.error(w, http.StatusInternalServerError, "db update error: "+err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"snr_db":       stats.SNREstimate,
		"snr_sox":      stats.SNRSox,
		"snr_wada":     stats.SNRWada,
		"snr_spectral": stats.SNRSpectral,
		"noise_level":  stats.NoiseLevel,
		"rms_db":       stats.RMSLevDB,
	})
}

var analyzeRunning bool
var analyzeProcessed int
var analyzeTotal int
var analyzeMu sync.Mutex

// AnalyzeStart - POST /api/analyze/start
func (h *Handlers) AnalyzeStart(w http.ResponseWriter, r *http.Request) {
	analyzeMu.Lock()
	if analyzeRunning {
		analyzeMu.Unlock()
		h.error(w, http.StatusConflict, "Analyze already running")
		return
	}
	analyzeMu.Unlock()

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}

	force := r.URL.Query().Get("force") == "1"

	files, err := h.db.GetFilesForAnalyze(limit, force)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	if len(files) == 0 {
		h.success(w, map[string]interface{}{
			"message": "No files to analyze",
			"queued":  0,
		})
		return
	}

	analyzeMu.Lock()
	analyzeRunning = true
	analyzeProcessed = 0
	analyzeTotal = len(files)
	analyzeMu.Unlock()

	// Запускаем в фоне
	go func() {
		for _, file := range files {
			stats, err := audio.GetStats(file.FilePath)
			if err != nil {
				log.Printf("Analyze error for %d: %v", file.ID, err)
			} else {
				h.db.UpdateAudioStats(file.ID, stats)
				log.Printf("Analyzed file %d: SNR=%.1f, Noise=%s", file.ID, stats.SNREstimate, stats.NoiseLevel)
			}

			analyzeMu.Lock()
			analyzeProcessed++
			analyzeMu.Unlock()
		}

		analyzeMu.Lock()
		analyzeRunning = false
		analyzeMu.Unlock()
		log.Printf("Analyze complete: %d files", len(files))
	}()

	h.success(w, map[string]interface{}{
		"message": "Analyze started",
		"queued":  len(files),
	})
}

// AnalyzeStatus - GET /api/analyze/status
func (h *Handlers) AnalyzeStatus(w http.ResponseWriter, r *http.Request) {
	analyzeMu.Lock()
	defer analyzeMu.Unlock()

	var percent float64
	if analyzeTotal > 0 {
		percent = float64(analyzeProcessed) / float64(analyzeTotal) * 100
	}

	h.success(w, map[string]interface{}{
		"running":   analyzeRunning,
		"processed": analyzeProcessed,
		"total":     analyzeTotal,
		"percent":   percent,
	})
}
