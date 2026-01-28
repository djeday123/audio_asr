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
// Возвращает: speakerID, warnings []string, error
func (db *DB) CheckFilesForMerge(ids []int64) (string, []string, error) {
	if len(ids) < 2 {
		return "", nil, fmt.Errorf("need at least 2 files")
	}

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, user_id, active, wer, asr_status, operator_verified, merged_id 
		FROM audio_files 
		WHERE id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()

	var userID string
	var warnings []string
	found := 0

	for rows.Next() {
		var id int64
		var uid string
		var active bool
		var wer sql.NullFloat64
		var asrStatus string
		var verified bool
		var mergedID int64

		if err := rows.Scan(&id, &uid, &active, &wer, &asrStatus, &verified, &mergedID); err != nil {
			return "", nil, err
		}

		found++

		if userID == "" {
			userID = uid
		} else if userID != uid {
			return "", nil, fmt.Errorf("files from different speakers: %s vs %s", userID, uid)
		}

		if !active {
			// Файл неактивен (уже использован в merge) — warning, не блокируем
			warnings = append(warnings, fmt.Sprintf("file %d is inactive (already used in merge %d)", id, mergedID))
			continue
		}

		// Проверка WER
		if asrStatus == "processed" && wer.Valid && wer.Float64 > 0.001 {
			if verified {
				// Verified но WER > 0 — warning, пропускаем
				warnings = append(warnings, fmt.Sprintf("file %d has WER %.2f%% but is verified", id, wer.Float64*100))
			} else {
				// Не verified и WER > 0 — блокируем
				return "", nil, fmt.Errorf("file %d has WER %.2f%% (must be 0%% or verified)", id, wer.Float64*100)
			}
		}
	}

	if found != len(ids) {
		return "", nil, fmt.Errorf("some files not found: expected %d, got %d", len(ids), found)
	}

	return userID, warnings, nil
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

// CheckMergeExists проверяет был ли уже такой merge или в очереди
func (db *DB) CheckMergeExists(idsString string) (bool, int64, string, error) {
	// Проверяем в audio_files по exact match parent_ids
	var existingID int64
	err := db.conn.QueryRow(`
		SELECT id FROM audio_files 
		WHERE parent_ids = ? AND active = 1
		LIMIT 1
	`, idsString).Scan(&existingID)

	if err == nil {
		return true, existingID, "already merged", nil
	}

	// Проверяем в merge_queue
	var queueID int64
	var status string
	err = db.conn.QueryRow(`
		SELECT id, status FROM merge_queue 
		WHERE ids_string = ? AND status IN ('pending', 'processing', 'completed')
		LIMIT 1
	`, idsString).Scan(&queueID, &status)

	if err == nil {
		return true, queueID, fmt.Sprintf("in queue (%s)", status), nil
	}

	return false, 0, "", nil
}

// AddBatchToMergeQueueOptimized - оптимизированная версия с минимумом SQL запросов
// Вместо 3 запросов на каждую строку - делаем 3 запроса на ВСЕ строки
func (db *DB) AddBatchToMergeQueue(idsStrings []string) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, 0, len(idsStrings))

	// ========================================
	// ШАГ 1: Парсим все строки и собираем уникальные ID
	// ========================================
	type parsedItem struct {
		index     int
		idsString string
		ids       []int64
	}
	var parsedItems []parsedItem
	allFileIDs := make(map[int64]bool) // уникальные ID файлов

	for _, idsString := range idsStrings {
		idsString = strings.TrimSpace(idsString)
		if idsString == "" {
			continue
		}

		result := map[string]interface{}{
			"ids":    idsString,
			"status": "pending",
		}

		// Парсим IDs
		ids, err := ParseMergeIDs(idsString)
		if err != nil {
			result["status"] = "error"
			result["error"] = err.Error()
			results = append(results, result)
			continue
		}

		if len(ids) < 2 {
			result["status"] = "error"
			result["error"] = "need at least 2 IDs"
			results = append(results, result)
			continue
		}

		// Собираем уникальные ID
		for _, id := range ids {
			allFileIDs[id] = true
		}

		parsedItems = append(parsedItems, parsedItem{
			index:     len(results),
			idsString: idsString,
			ids:       ids,
		})
		results = append(results, result)
	}

	if len(parsedItems) == 0 {
		return results, nil
	}

	// ========================================
	// ШАГ 2: ОДИН запрос - проверка дубликатов в audio_files
	// ========================================
	allIdsStrings := make([]string, len(parsedItems))
	for i, item := range parsedItems {
		allIdsStrings[i] = item.idsString
	}

	existingMerged := make(map[string]int64) // idsString -> merged file ID
	if len(allIdsStrings) > 0 {
		placeholders := make([]string, len(allIdsStrings))
		args := make([]interface{}, len(allIdsStrings))
		for i, s := range allIdsStrings {
			placeholders[i] = "?"
			args[i] = s
		}

		query := fmt.Sprintf(`
			SELECT parent_ids, id FROM audio_files 
			WHERE parent_ids IN (%s) AND active = 1
		`, strings.Join(placeholders, ","))

		rows, err := db.conn.Query(query, args...)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var parentIDs string
				var id int64
				rows.Scan(&parentIDs, &id)
				existingMerged[parentIDs] = id
			}
		}
	}

	// ========================================
	// ШАГ 3: ОДИН запрос - проверка дубликатов в merge_queue
	// ========================================
	existingQueue := make(map[string]struct {
		id     int64
		status string
	})
	if len(allIdsStrings) > 0 {
		placeholders := make([]string, len(allIdsStrings))
		args := make([]interface{}, len(allIdsStrings))
		for i, s := range allIdsStrings {
			placeholders[i] = "?"
			args[i] = s
		}

		query := fmt.Sprintf(`
			SELECT ids_string, id, status FROM merge_queue 
			WHERE ids_string IN (%s) AND status IN ('pending', 'processing', 'completed')
		`, strings.Join(placeholders, ","))

		rows, err := db.conn.Query(query, args...)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var idsStr string
				var id int64
				var status string
				rows.Scan(&idsStr, &id, &status)
				existingQueue[idsStr] = struct {
					id     int64
					status string
				}{id, status}
			}
		}
	}

	// ========================================
	// ШАГ 4: ОДИН запрос - получаем данные всех файлов
	// ========================================
	type fileInfo struct {
		id        int64
		userID    string
		active    bool
		wer       sql.NullFloat64
		asrStatus sql.NullString
		verified  bool
		mergedID  sql.NullInt64
	}
	filesData := make(map[int64]fileInfo)

	if len(allFileIDs) > 0 {
		fileIDsList := make([]int64, 0, len(allFileIDs))
		for id := range allFileIDs {
			fileIDsList = append(fileIDsList, id)
		}

		placeholders := make([]string, len(fileIDsList))
		args := make([]interface{}, len(fileIDsList))
		for i, id := range fileIDsList {
			placeholders[i] = "?"
			args[i] = id
		}

		query := fmt.Sprintf(`
			SELECT id, user_id, active, wer, asr_status, operator_verified, COALESCE(merged_id, 0) 
			FROM audio_files 
			WHERE id IN (%s)
		`, strings.Join(placeholders, ","))

		rows, err := db.conn.Query(query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch files: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var f fileInfo
			var mergedID int64
			if err := rows.Scan(&f.id, &f.userID, &f.active, &f.wer, &f.asrStatus, &f.verified, &mergedID); err != nil {
				return nil, fmt.Errorf("scan error for file: %w", err)
			}
			f.mergedID = sql.NullInt64{Int64: mergedID, Valid: mergedID > 0}
			filesData[f.id] = f
		}
	}

	// ========================================
	// ШАГ 5: Валидация каждой строки (в памяти, без SQL)
	// ========================================
	type validItem struct {
		index     int
		idsString string
		userID    string
		warnings  []string
	}
	var validItems []validItem

	for _, item := range parsedItems {
		result := results[item.index]

		// Проверка дубликата в audio_files
		if mergedID, exists := existingMerged[item.idsString]; exists {
			result["status"] = "skipped"
			result["reason"] = "already merged"
			result["existing_id"] = mergedID
			continue
		}

		// Проверка дубликата в queue
		if queueItem, exists := existingQueue[item.idsString]; exists {
			result["status"] = "skipped"
			result["reason"] = fmt.Sprintf("in queue (%s)", queueItem.status)
			result["existing_id"] = queueItem.id
			continue
		}

		// Валидация файлов
		var userID string
		var warnings []string
		var validationErr error

		for _, id := range item.ids {
			f, exists := filesData[id]
			if !exists {
				validationErr = fmt.Errorf("file %d not found", id)
				break
			}

			if userID == "" {
				userID = f.userID
			} else if userID != f.userID {
				validationErr = fmt.Errorf("files from different speakers: %s vs %s", userID, f.userID)
				break
			}

			if !f.active {
				mergedID := int64(0)
				if f.mergedID.Valid {
					mergedID = f.mergedID.Int64
				}
				warnings = append(warnings, fmt.Sprintf("file %d is inactive (already used in merge %d)", id, mergedID))
				continue
			}

			// Проверка WER
			asrStatus := ""
			if f.asrStatus.Valid {
				asrStatus = f.asrStatus.String
			}
			if asrStatus == "processed" && f.wer.Valid && f.wer.Float64 > 0.001 {
				if f.verified {
					warnings = append(warnings, fmt.Sprintf("file %d has WER %.2f%% but is verified", id, f.wer.Float64*100))
				} else {
					validationErr = fmt.Errorf("file %d has WER %.2f%% (must be 0%% or verified)", id, f.wer.Float64*100)
					break
				}
			}
		}

		if validationErr != nil {
			result["status"] = "error"
			result["error"] = validationErr.Error()
			continue
		}

		if len(warnings) > 0 {
			result["warnings"] = warnings
		}

		validItems = append(validItems, validItem{
			index:     item.index,
			idsString: item.idsString,
			userID:    userID,
			warnings:  warnings,
		})
	}

	// ========================================
	// ШАГ 6: Batch INSERT всех валидных записей
	// ========================================
	if len(validItems) > 0 {
		valueStrings := make([]string, len(validItems))
		valueArgs := make([]interface{}, 0, len(validItems)*2)

		for i, item := range validItems {
			valueStrings[i] = "(?, ?, 'pending')"
			valueArgs = append(valueArgs, item.idsString, item.userID)
		}

		query := fmt.Sprintf(`
			INSERT INTO merge_queue (ids_string, user_id, status) 
			VALUES %s
		`, strings.Join(valueStrings, ", "))

		res, err := db.conn.Exec(query, valueArgs...)
		if err != nil {
			for _, item := range validItems {
				results[item.index]["status"] = "error"
				results[item.index]["error"] = "batch insert failed: " + err.Error()
			}
			return results, nil
		}

		firstID, _ := res.LastInsertId()
		for i, item := range validItems {
			results[item.index]["queue_id"] = firstID + int64(i)
		}
	}

	return results, nil
}

// GetFileIncludingInactive получает файл даже если он неактивен
func (db *DB) GetFileIncludingInactive(id int64) (*AudioFile, error) {
	var file AudioFile

	// Nullable string поля
	var noiseLevel, audioMetadata sql.NullString
	var transcriptionASR, transcriptionASRNoLM sql.NullString
	var transcriptionWhisperLocal, transcriptionWhisperOpenAI sql.NullString
	var parentIDs sql.NullString

	// Nullable числовые поля
	var mergedID sql.NullInt64
	var wer, cer, werNoLM, cerNoLM sql.NullFloat64
	var werWhisperLocal, cerWhisperLocal sql.NullFloat64
	var werWhisperOpenAI, cerWhisperOpenAI sql.NullFloat64
	var snrDB, snrSox, snrWada, rmsDB sql.NullFloat64

	err := db.conn.QueryRow(`
		SELECT id, user_id, chapter_id, file_path, file_hash, duration_sec,
		       snr_db, snr_sox, snr_wada, noise_level, rms_db,
		       sample_rate, channels, bit_depth, file_size, audio_metadata,
		       transcription_original, transcription_asr, transcription_asr_nolm,
		       transcription_whisper_local, transcription_whisper_openai,
		       wer, cer, wer_nolm, cer_nolm, 
		       wer_whisper_local, cer_whisper_local,
		       wer_whisper_openai, cer_whisper_openai,
		       asr_status, asr_nolm_status, whisper_local_status, whisper_openai_status,
		       operator_verified, original_edited, active, merged_id, parent_ids
		FROM audio_files WHERE id = ?
	`, id).Scan(
		&file.ID, &file.UserID, &file.ChapterID, &file.FilePath, &file.FileHash, &file.DurationSec,
		&snrDB, &snrSox, &snrWada, &noiseLevel, &rmsDB,
		&file.SampleRate, &file.Channels, &file.BitDepth, &file.FileSize, &audioMetadata,
		&file.TranscriptionOriginal, &transcriptionASR, &transcriptionASRNoLM,
		&transcriptionWhisperLocal, &transcriptionWhisperOpenAI,
		&wer, &cer, &werNoLM, &cerNoLM,
		&werWhisperLocal, &cerWhisperLocal,
		&werWhisperOpenAI, &cerWhisperOpenAI,
		&file.ASRStatus, &file.ASRNoLMStatus, &file.WhisperLocalStatus, &file.WhisperOpenAIStatus,
		&file.OperatorVerified, &file.OriginalEdited, &file.Active, &mergedID, &parentIDs,
	)
	if err != nil {
		return nil, err
	}

	// Конвертируем Null типы
	if noiseLevel.Valid {
		file.NoiseLevel = noiseLevel.String
	}
	if audioMetadata.Valid {
		file.AudioMetadata = audioMetadata.String
	}
	if transcriptionASR.Valid {
		file.TranscriptionASR = transcriptionASR.String
	}
	if transcriptionASRNoLM.Valid {
		file.TranscriptionASRNoLM = transcriptionASRNoLM.String
	}
	if transcriptionWhisperLocal.Valid {
		file.TranscriptionWhisperLocal = transcriptionWhisperLocal.String
	}
	if transcriptionWhisperOpenAI.Valid {
		file.TranscriptionWhisperOpenAI = transcriptionWhisperOpenAI.String
	}
	if parentIDs.Valid {
		file.ParentIDs = parentIDs.String
	}
	if mergedID.Valid {
		file.MergedID = mergedID.Int64
	}
	if snrDB.Valid {
		file.SNRDB = snrDB.Float64
	}
	if snrSox.Valid {
		file.SNRSox = snrSox.Float64
	}
	if snrWada.Valid {
		file.SNRWada = snrWada.Float64
	}
	if rmsDB.Valid {
		file.RMSDB = rmsDB.Float64
	}
	if wer.Valid {
		file.WER = wer.Float64
	}
	if cer.Valid {
		file.CER = cer.Float64
	}
	if werNoLM.Valid {
		file.WERNoLM = werNoLM.Float64
	}
	if cerNoLM.Valid {
		file.CERNoLM = cerNoLM.Float64
	}
	if werWhisperLocal.Valid {
		file.WERWhisperLocal = werWhisperLocal.Float64
	}
	if cerWhisperLocal.Valid {
		file.CERWhisperLocal = cerWhisperLocal.Float64
	}
	if werWhisperOpenAI.Valid {
		file.WERWhisperOpenAI = werWhisperOpenAI.Float64
	}
	if cerWhisperOpenAI.Valid {
		file.CERWhisperOpenAI = cerWhisperOpenAI.Float64
	}

	return &file, nil
}

// DeleteMergeQueueItem удаляет одну запись из очереди
func (db *DB) DeleteMergeQueueItem(id int64) error {
	_, err := db.conn.Exec(`DELETE FROM merge_queue WHERE id = ?`, id)
	return err
}

// ClearMergeQueue очищает очередь по статусу
func (db *DB) ClearMergeQueue(status string) (int64, error) {
	var res sql.Result
	var err error

	if status == "all" {
		res, err = db.conn.Exec(`DELETE FROM merge_queue`)
	} else {
		res, err = db.conn.Exec(`DELETE FROM merge_queue WHERE status = ?`, status)
	}

	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}
