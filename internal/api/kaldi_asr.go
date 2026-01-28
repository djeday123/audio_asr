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
