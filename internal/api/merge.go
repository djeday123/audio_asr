package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

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

// ========================================
// MERGE QUEUE
// ========================================

// AddToMergeQueue - POST /api/merge/queue
// Body: {"ids": "462754|462919|462999|462748"}
func (h *Handlers) AddToMergeQueue(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.IDs == "" {
		h.error(w, http.StatusBadRequest, "ids required")
		return
	}

	queueID, err := h.mergeService.AddToQueue(req.IDs)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"queue_id": queueID,
		"ids":      req.IDs,
		"status":   "pending",
	})
}

// MergeFromString - POST /api/merge/now
// Выполняет merge сразу без очереди
func (h *Handlers) MergeFromString(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid json")
		return
	}

	result, err := h.mergeService.ProcessSingleFromString(req.IDs)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	h.success(w, result)
}

// ProcessMergeQueue - POST /api/merge/queue/start
func (h *Handlers) ProcessMergeQueue(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}

	err := h.mergeService.ProcessMergeQueue(limit)
	if err != nil {
		h.error(w, http.StatusConflict, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message": "Merge queue processing started",
		"limit":   limit,
	})
}

// MergeQueueStatus - GET /api/merge/queue/status
func (h *Handlers) MergeQueueStatus(w http.ResponseWriter, r *http.Request) {
	h.success(w, h.mergeService.QueueStatus())
}

// StopMergeQueue - POST /api/merge/queue/stop
func (h *Handlers) StopMergeQueue(w http.ResponseWriter, r *http.Request) {
	h.mergeService.StopQueue()
	h.success(w, "Merge queue stopped")
}

// ListMergeQueue - GET /api/merge/queue
func (h *Handlers) ListMergeQueue(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 20
	}
	status := r.URL.Query().Get("status")

	items, total, err := h.db.GetMergeQueueList(page, limit, status)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"items": items,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}

// AddBatchToMergeQueue - POST /api/merge/queue/batch
// Body: {"lines": "462743|462714\n463174|462973\n..."}
func (h *Handlers) AddBatchToMergeQueue(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Lines string `json:"lines"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.Lines == "" {
		h.error(w, http.StatusBadRequest, "lines required")
		return
	}

	// Разбиваем по строкам
	lines := strings.Split(req.Lines, "\n")
	var idsStrings []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			idsStrings = append(idsStrings, line)
		}
	}

	if len(idsStrings) == 0 {
		h.error(w, http.StatusBadRequest, "no valid lines")
		return
	}

	results, err := h.db.AddBatchToMergeQueue(idsStrings)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Считаем статистику
	var added, skipped, errors int
	for _, r := range results {
		switch r["status"] {
		case "pending":
			added++
		case "skipped":
			skipped++
		case "error":
			errors++
		}
	}

	h.success(w, map[string]interface{}{
		"results": results,
		"total":   len(idsStrings),
		"added":   added,
		"skipped": skipped,
		"errors":  errors,
	})
}
