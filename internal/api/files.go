package api

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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
	noiseLevel := r.URL.Query().Get("noise_level")
	textSearch := r.URL.Query().Get("text")
	chapter := r.URL.Query().Get("chapter")

	result, err := h.db.GetFilesFiltered(page, limit, speaker, werOp, werValue, durOp, durValue,
		asrStatus, asrNoLMStatus, whisperLocalStatus, whisperOpenaiStatus,
		verified, merged, active, noiseLevel, textSearch, chapter)
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
// DELETE File handler
// ============================================================
// DeleteFile - DELETE /api/files/{id}
func (h *Handlers) DeleteFile(w http.ResponseWriter, r *http.Request) {
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

	// Получаем директорию и имя файла
	dir := filepath.Dir(file.FilePath)
	baseName := strings.TrimSuffix(filepath.Base(file.FilePath), ".wav") // 1001217-920379637-0002

	// Формируем имя trans файла: speaker-chapter.trans.txt
	parts := strings.Split(baseName, "-")
	var transPath string
	if len(parts) >= 3 {
		// 1001217-920379637-0002 -> 1001217-920379637.trans.txt
		transPath = filepath.Join(dir, parts[0]+"-"+parts[1]+".trans.txt")
	}

	// 1. Удаляем физический аудио файл
	if err := os.Remove(file.FilePath); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: could not delete file %s: %v", file.FilePath, err)
	}

	// 2. Удаляем строку из trans.txt
	if transPath != "" {
		if err := removeLineFromTranscript(transPath, baseName); err != nil {
			log.Printf("Warning: could not update trans file: %v", err)
		}
	}

	// 3. Проверяем, остались ли wav файлы в папке
	wavFiles, _ := filepath.Glob(filepath.Join(dir, "*.wav"))
	if len(wavFiles) == 0 {
		// Удаляем trans файл и папку
		if transPath != "" {
			os.Remove(transPath)
		}
		os.Remove(dir) // Удалит только если пустая
		log.Printf("Removed empty directory: %s", dir)
	}

	// 4. Удаляем из БД
	if err := h.db.DeleteFile(id); err != nil {
		h.error(w, http.StatusInternalServerError, "db error: "+err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"message":   "File deleted",
		"id":        id,
		"file_path": file.FilePath,
	})
}

// removeLineFromTranscript удаляет строку с указанным префиксом из trans файла
func removeLineFromTranscript(transPath, prefix string) error {
	data, err := os.ReadFile(transPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	lines := strings.Split(string(data), "\n")
	var newLines []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		// Пропускаем строку которая начинается с нашего prefix
		if strings.HasPrefix(trimmed, prefix+" ") {
			log.Printf("Removing transcript line: %s", prefix)
			continue
		}
		newLines = append(newLines, line)
	}

	// Записываем обратно
	result := strings.Join(newLines, "\n")
	if len(newLines) > 0 {
		result += "\n"
	}

	return os.WriteFile(transPath, []byte(result), 0644)
}
