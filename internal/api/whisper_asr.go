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
