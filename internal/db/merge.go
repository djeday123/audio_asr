package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// MergeQueueItem - запись в очереди merge
type MergeQueueItem struct {
	ID                  int64      `json:"id"`
	IDsString           string     `json:"ids_string"`
	UserID              string     `json:"user_id"`
	Status              string     `json:"status"`
	MergedFileID        *int64     `json:"merged_file_id"`
	MergedFilePath      *string    `json:"merged_file_path"`
	MergedDuration      *float64   `json:"merged_duration"`
	MergedTranscription *string    `json:"merged_transcription"`
	ErrorMessage        *string    `json:"error_message"`
	CreatedAt           time.Time  `json:"created_at"`
	ProcessedAt         *time.Time `json:"processed_at"`
}

// ParseMergeIDs парсит строку IDs в slice int64
func ParseMergeIDs(idsString string) ([]int64, error) {
	parts := strings.Split(idsString, "|")
	ids := make([]int64, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var id int64
		_, err := fmt.Sscanf(p, "%d", &id)
		if err != nil {
			return nil, fmt.Errorf("invalid ID: %s", p)
		}
		ids = append(ids, id)
	}

	return ids, nil
}

// AddToMergeQueue добавляет строку IDs в очередь
func (db *DB) AddToMergeQueue(idsString string) (int64, error) {
	// Парсим первый ID чтобы получить user_id
	ids := strings.Split(idsString, "|")
	if len(ids) < 2 {
		return 0, fmt.Errorf("need at least 2 IDs, got %d", len(ids))
	}

	// Получаем user_id из первого файла
	var userID string
	err := db.conn.QueryRow("SELECT user_id FROM audio_files WHERE id = ?", ids[0]).Scan(&userID)
	if err != nil {
		return 0, fmt.Errorf("first file not found: %w", err)
	}

	res, err := db.conn.Exec(`
		INSERT INTO merge_queue (ids_string, user_id, status)
		VALUES (?, ?, 'pending')
	`, idsString, userID)
	if err != nil {
		return 0, err
	}

	return res.LastInsertId()
}

// GetPendingMergeQueue возвращает pending записи из очереди
func (db *DB) GetPendingMergeQueue(limit int) ([]MergeQueueItem, error) {
	query := `
		SELECT id, ids_string, user_id, status, merged_file_id, 
		       merged_file_path, merged_duration, merged_transcription,
		       error_message, created_at, processed_at
		FROM merge_queue 
		WHERE status = 'pending'
		ORDER BY id
	`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []MergeQueueItem
	for rows.Next() {
		var item MergeQueueItem
		err := rows.Scan(
			&item.ID, &item.IDsString, &item.UserID, &item.Status,
			&item.MergedFileID, &item.MergedFilePath, &item.MergedDuration,
			&item.MergedTranscription, &item.ErrorMessage,
			&item.CreatedAt, &item.ProcessedAt,
		)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

// UpdateMergeQueueStatus обновляет статус записи
func (db *DB) UpdateMergeQueueStatus(id int64, status string) error {
	_, err := db.conn.Exec(`
		UPDATE merge_queue SET status = ? WHERE id = ?
	`, status, id)
	return err
}

// UpdateMergeQueueCompleted помечает как завершённый
func (db *DB) UpdateMergeQueueCompleted(id int64, mergedFileID int64, filePath string, duration float64, transcription string) error {
	_, err := db.conn.Exec(`
		UPDATE merge_queue SET 
			status = 'completed',
			merged_file_id = ?,
			merged_file_path = ?,
			merged_duration = ?,
			merged_transcription = ?,
			processed_at = NOW()
		WHERE id = ?
	`, mergedFileID, filePath, duration, transcription, id)
	return err
}

// UpdateMergeQueueError помечает как ошибочный
func (db *DB) UpdateMergeQueueError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`
		UPDATE merge_queue SET 
			status = 'error',
			error_message = ?,
			processed_at = NOW()
		WHERE id = ?
	`, errMsg, id)
	return err
}

// GetMergeQueueItem возвращает одну запись
func (db *DB) GetMergeQueueItem(id int64) (*MergeQueueItem, error) {
	var item MergeQueueItem
	err := db.conn.QueryRow(`
		SELECT id, ids_string, user_id, status, merged_file_id, 
		       merged_file_path, merged_duration, merged_transcription,
		       error_message, created_at, processed_at
		FROM merge_queue WHERE id = ?
	`, id).Scan(
		&item.ID, &item.IDsString, &item.UserID, &item.Status,
		&item.MergedFileID, &item.MergedFilePath, &item.MergedDuration,
		&item.MergedTranscription, &item.ErrorMessage,
		&item.CreatedAt, &item.ProcessedAt,
	)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// GetMergeQueueList возвращает список с пагинацией
func (db *DB) GetMergeQueueList(page, limit int, status string) ([]MergeQueueItem, int64, error) {
	offset := (page - 1) * limit

	// Count
	countQuery := "SELECT COUNT(*) FROM merge_queue"
	var args []interface{}
	if status != "" && status != "all" {
		countQuery += " WHERE status = ?"
		args = append(args, status)
	}

	var total int64
	db.conn.QueryRow(countQuery, args...).Scan(&total)

	// Select
	query := `
		SELECT id, ids_string, user_id, status, merged_file_id, 
		       merged_file_path, merged_duration, merged_transcription,
		       error_message, created_at, processed_at
		FROM merge_queue
	`
	if status != "" && status != "all" {
		query += " WHERE status = ?"
	}
	query += " ORDER BY id DESC LIMIT ? OFFSET ?"

	if status != "" && status != "all" {
		args = append(args, limit, offset)
	} else {
		args = []interface{}{limit, offset}
	}

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var items []MergeQueueItem
	for rows.Next() {
		var item MergeQueueItem
		err := rows.Scan(
			&item.ID, &item.IDsString, &item.UserID, &item.Status,
			&item.MergedFileID, &item.MergedFilePath, &item.MergedDuration,
			&item.MergedTranscription, &item.ErrorMessage,
			&item.CreatedAt, &item.ProcessedAt,
		)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}

	return items, total, nil
}

// CheckFilesForMerge проверяет файлы перед merge
func (db *DB) CheckFilesForMerge(ids []int64) (string, error) {
	if len(ids) < 2 {
		return "", fmt.Errorf("need at least 2 files")
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	// Проверяем что все файлы существуют и от одного спикера
	query := fmt.Sprintf(`
		SELECT id, user_id, active, wer, asr_status 
		FROM audio_files 
		WHERE id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var userID string
	found := 0

	for rows.Next() {
		var id int64
		var uid string
		var active bool
		var wer sql.NullFloat64
		var asrStatus string

		if err := rows.Scan(&id, &uid, &active, &wer, &asrStatus); err != nil {
			return "", err
		}

		found++

		if userID == "" {
			userID = uid
		} else if userID != uid {
			return "", fmt.Errorf("files from different speakers: %s vs %s", userID, uid)
		}

		if !active {
			return "", fmt.Errorf("file %d is not active", id)
		}

		// Проверка WER=0 только если файл обработан
		if asrStatus == "processed" && wer.Valid && wer.Float64 > 0.001 {
			return "", fmt.Errorf("file %d has WER %.2f%% (must be 0%%)", id, wer.Float64*100)
		}
	}

	if found != len(ids) {
		return "", fmt.Errorf("some files not found: expected %d, got %d", len(ids), found)
	}

	return userID, nil
}

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

// CheckMergeExists проверяет был ли уже такой merge
// Ищет по exact match parent_ids или в merge_queue
func (db *DB) CheckMergeExists(idsString string) (bool, int64, error) {
	// Нормализуем строку (сортируем IDs для консистентности)
	ids, err := ParseMergeIDs(idsString)
	if err != nil {
		return false, 0, err
	}

	// Проверяем в audio_files по parent_ids
	var existingID int64
	err = db.conn.QueryRow(`
		SELECT id FROM audio_files 
		WHERE parent_ids = ? AND active = 1
		LIMIT 1
	`, idsString).Scan(&existingID)

	if err == nil {
		return true, existingID, nil
	}

	// Проверяем в merge_queue (pending или completed)
	err = db.conn.QueryRow(`
		SELECT merged_file_id FROM merge_queue 
		WHERE ids_string = ? AND status IN ('pending', 'processing', 'completed')
		LIMIT 1
	`, idsString).Scan(&existingID)

	if err == nil {
		return true, existingID, nil
	}

	// Проверяем что исходные файлы не были уже merged
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	var alreadyMergedID int64
	query := fmt.Sprintf(`
		SELECT id FROM audio_files 
		WHERE id IN (%s) AND merged_id > 0
		LIMIT 1
	`, strings.Join(placeholders, ","))

	err = db.conn.QueryRow(query, args...).Scan(&alreadyMergedID)
	if err == nil {
		return true, alreadyMergedID, fmt.Errorf("file %d already merged", alreadyMergedID)
	}

	return false, 0, nil
}

// AddBatchToMergeQueue добавляет множество строк в очередь
func (db *DB) AddBatchToMergeQueue(idsStrings []string) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, 0, len(idsStrings))

	for _, idsString := range idsStrings {
		idsString = strings.TrimSpace(idsString)
		if idsString == "" {
			continue
		}

		result := map[string]interface{}{
			"ids":    idsString,
			"status": "pending",
		}

		// Проверяем дубликат
		exists, existingID, err := db.CheckMergeExists(idsString)
		if err != nil {
			result["status"] = "error"
			result["error"] = err.Error()
			results = append(results, result)
			continue
		}

		if exists {
			result["status"] = "skipped"
			result["reason"] = "already merged"
			result["existing_id"] = existingID
			results = append(results, result)
			continue
		}

		// Валидация
		ids, err := ParseMergeIDs(idsString)
		if err != nil {
			result["status"] = "error"
			result["error"] = err.Error()
			results = append(results, result)
			continue
		}

		// Проверяем файлы
		_, err = db.CheckFilesForMerge(ids)
		if err != nil {
			result["status"] = "error"
			result["error"] = err.Error()
			results = append(results, result)
			continue
		}

		// Добавляем в очередь
		queueID, err := db.AddToMergeQueue(idsString)
		if err != nil {
			result["status"] = "error"
			result["error"] = err.Error()
			results = append(results, result)
			continue
		}

		result["queue_id"] = queueID
		results = append(results, result)
	}

	return results, nil
}
