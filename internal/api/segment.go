package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"audio-labeler/internal/audio"
	"audio-labeler/internal/segment"
)

// SegmentHandlers - handlers для работы с сегментами
type SegmentHandlers struct {
	repo   *segment.Repository
	client *segment.Client
}

func NewSegmentHandlers(repo *segment.Repository, client *segment.Client) *SegmentHandlers {
	return &SegmentHandlers{
		repo:   repo,
		client: client,
	}
}

// DiarizeFile - POST /api/files/{id}/diarize
func (sh *SegmentHandlers) DiarizeFile(w http.ResponseWriter, r *http.Request, h *Handlers) {
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

	var minSpeakers, maxSpeakers *int
	if v := r.URL.Query().Get("min_speakers"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			minSpeakers = &n
		}
	}
	if v := r.URL.Query().Get("max_speakers"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			maxSpeakers = &n
		}
	}

	if err := sh.client.Health(); err != nil {
		h.error(w, http.StatusServiceUnavailable, "pyannote not available: "+err.Error())
		return
	}

	log.Printf("Diarize file ID=%d path=%s", id, file.FilePath)

	result, err := sh.client.Diarize(file.FilePath, minSpeakers, maxSpeakers)
	if err != nil {
		log.Printf("Diarize error ID=%d: %v", id, err)
		h.error(w, http.StatusInternalServerError, "diarize failed: "+err.Error())
		return
	}

	if err := sh.repo.InsertSegments(id, result.Segments); err != nil {
		h.error(w, http.StatusInternalServerError, "save segments failed: "+err.Error())
		return
	}

	log.Printf("Diarize OK ID=%d: %d segments, %d speakers", id, len(result.Segments), result.NumSpeakers)

	h.success(w, map[string]interface{}{
		"file_id":       id,
		"segments":      len(result.Segments),
		"overlaps":      len(result.Overlaps),
		"num_speakers":  result.NumSpeakers,
		"speaker_stats": result.SpeakerStats,
		"duration":      result.Duration,
	})
}

// GetSegments - GET /api/files/{id}/segments
func (sh *SegmentHandlers) GetSegments(w http.ResponseWriter, r *http.Request, h *Handlers) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	segments, err := sh.repo.GetByAudioFile(id)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, segments)
}

// UpdateSegmentSelection - PUT /api/files/{id}/segments/select
func (sh *SegmentHandlers) UpdateSegmentSelection(w http.ResponseWriter, r *http.Request, h *Handlers) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	var req struct {
		SegmentIDs []int64 `json:"segment_ids"`
		Selected   bool    `json:"selected"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid json")
		return
	}

	if err := sh.repo.UpdateSelection(req.SegmentIDs, req.Selected); err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"file_id":  id,
		"updated":  len(req.SegmentIDs),
		"selected": req.Selected,
	})
}

// UpdateSegmentTranscripts - PUT /api/files/{id}/segments/transcripts
func (sh *SegmentHandlers) UpdateSegmentTranscripts(w http.ResponseWriter, r *http.Request, h *Handlers) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	var req struct {
		Transcripts map[string]string `json:"transcripts"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid json")
		return
	}

	transcripts := make(map[int64]string)
	for k, v := range req.Transcripts {
		segID, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			continue
		}
		transcripts[segID] = v
	}

	if err := sh.repo.UpdateTranscriptsBatch(transcripts); err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("Updated %d segment transcripts for file ID=%d", len(transcripts), id)

	h.success(w, map[string]interface{}{
		"file_id": id,
		"updated": len(transcripts),
	})
}

// ApplySegmentTranscripts - POST /api/files/{id}/segments/apply
func (sh *SegmentHandlers) ApplySegmentTranscripts(w http.ResponseWriter, r *http.Request, h *Handlers) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	combined, err := sh.repo.CombineTranscripts(id)
	if err != nil {
		h.error(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.db.UpdateOriginalTranscription(id, combined); err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	log.Printf("Applied segments to file ID=%d: %q", id, combined)

	h.success(w, map[string]interface{}{
		"file_id":       id,
		"transcription": combined,
	})
}

// CheckSegments - GET /api/files/{id}/segments/check
func (sh *SegmentHandlers) CheckSegments(w http.ResponseWriter, r *http.Request, h *Handlers) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		h.error(w, http.StatusBadRequest, "invalid id")
		return
	}

	has, err := sh.repo.HasSegments(id)
	if err != nil {
		h.error(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"file_id":      id,
		"has_segments": has,
	})
}

// PyannoteHealth - GET /api/pyannote/health
func (sh *SegmentHandlers) PyannoteHealth(w http.ResponseWriter, r *http.Request, h *Handlers) {
	if sh.client == nil {
		h.error(w, http.StatusServiceUnavailable, "pyannote not configured")
		return
	}

	if err := sh.client.Health(); err != nil {
		h.error(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	h.success(w, map[string]string{"status": "ok"})
}

type ExportGroup struct {
	Start      float64 `json:"start"`
	End        float64 `json:"end"`
	Transcript string  `json:"transcript"`
	Speaker    string  `json:"speaker"`
}

type ExportRequest struct {
	Groups []ExportGroup `json:"groups"`
}

// ExportSegments - POST /api/files/{id}/segments/export
// Split audio by boundaries and save in LibriSpeech structure
// ExportSegments - POST /api/files/{id}/segments/export
func (sh *SegmentHandlers) ExportSegments(w http.ResponseWriter, r *http.Request, h *Handlers) {
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

	var req ExportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, http.StatusBadRequest, "invalid json")
		return
	}

	if len(req.Groups) == 0 {
		h.error(w, http.StatusBadRequest, "no groups to export")
		return
	}

	baseDir := "/data/processed_labeler/split"
	speaker := file.UserID
	if speaker == "" {
		speaker = "unknown"
	}

	// Определяем chapter
	var chapter string

	// Если файл уже из split (chapter начинается с 9) — используем тот же chapter
	if strings.HasPrefix(file.ChapterID, "9") {
		chapter = file.ChapterID
	} else {
		// Новый split — получаем следующий chapter ID
		chapter, err = h.db.GetNextSplitChapter(speaker)
		if err != nil {
			log.Printf("Failed to get next chapter: %v", err)
			chapter = fmt.Sprintf("9%d", id)
		}
	}

	outDir := filepath.Join(baseDir, speaker, chapter)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		h.error(w, http.StatusInternalServerError, "failed to create output dir: "+err.Error())
		return
	}

	// Находим следующий номер файла в этом chapter
	startIdx, err := h.db.GetNextFileIndex(speaker, chapter)
	if err != nil {
		startIdx = 1
	}

	var createdFiles []int64
	var transLines []string

	for i, group := range req.Groups {
		fileIdx := startIdx + i
		outName := fmt.Sprintf("%s-%s-%04d.wav", speaker, chapter, fileIdx)
		outPath := filepath.Join(outDir, outName)

		duration := group.End - group.Start
		cmd := exec.Command("ffmpeg", "-y",
			"-i", file.FilePath,
			"-ss", fmt.Sprintf("%.3f", group.Start),
			"-t", fmt.Sprintf("%.3f", duration),
			"-c:a", "pcm_s16le",
			"-ar", "8000",
			"-ac", "1",
			outPath,
		)

		if output, err := cmd.CombinedOutput(); err != nil {
			log.Printf("ffmpeg error for group %d: %v\n%s", i, err, string(output))
			continue
		}

		hash, _ := audio.MD5File(outPath)

		newID, err := h.db.InsertSplitFile(outPath, speaker, chapter, group.Transcript, duration, id, hash)
		if err != nil {
			log.Printf("DB insert error for %s: %v", outPath, err)
			continue
		}

		createdFiles = append(createdFiles, newID)

		baseName := strings.TrimSuffix(outName, ".wav")
		transLines = append(transLines, fmt.Sprintf("%s %s", baseName, group.Transcript))

		log.Printf("Created split file: %s (ID=%d, %.2fs)", outPath, newID, duration)
	}

	// Дописываем в trans.txt (append mode)
	transPath := filepath.Join(outDir, fmt.Sprintf("%s-%s.trans.txt", speaker, chapter))
	f, err := os.OpenFile(transPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		f.WriteString(strings.Join(transLines, "\n") + "\n")
		f.Close()
	}

	h.db.MarkAsSplitSource(id)

	h.success(w, map[string]interface{}{
		"created":    len(createdFiles),
		"file_ids":   createdFiles,
		"output_dir": outDir,
		"chapter":    chapter,
	})
}
