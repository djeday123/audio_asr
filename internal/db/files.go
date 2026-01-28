package db

import (
	"audio-labeler/internal/audio"
	"database/sql"
	"fmt"
	"strings"
)

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
		       COALESCE(snr_db, 0), COALESCE(rms_db, 0), COALESCE(sample_rate, 0), COALESCE(channels, 0), COALESCE(bit_depth, 0), COALESCE(file_size, 0),
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

// GetFilesFiltered - обновлённая версия с фильтрами verified и asr_nolm_status
func (db *DB) GetFilesFiltered(page, limit int, speaker, werOp string, werValue float64, durOp string, durValue float64, asrStatus, asrNoLMStatus,
	whisperLocalStatus, whisperOpenaiStatus, verified, merged, active, noiseLevel, textSearch, chapter string) (*FileListResult, error) {

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

	// Merged filter
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

	switch noiseLevel {
	case "low":
		conditions = append(conditions, "noise_level = 'low'")
	case "medium":
		conditions = append(conditions, "noise_level = 'medium'")
	case "high":
		conditions = append(conditions, "noise_level = 'high'")
	case "very_high":
		conditions = append(conditions, "noise_level = 'very_high'")
	case "none":
		conditions = append(conditions, "(noise_level IS NULL OR noise_level = '')")
	}

	if textSearch != "" {
		conditions = append(conditions, "transcription_original LIKE ?")
		args = append(args, "%"+textSearch+"%")
	}

	if chapter != "" {
		conditions = append(conditions, "chapter_id = ?")
		args = append(args, chapter)
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	var total int64
	countQuery := "SELECT COUNT(*) FROM audio_files " + whereClause
	db.conn.QueryRow(countQuery, args...).Scan(&total)

	query := `SELECT id, user_id, chapter_id, file_path, file_hash, 
          duration_sec, sample_rate, channels, COALESCE(bit_depth, 0), COALESCE(file_size, 0),
          COALESCE(snr_db, 0), COALESCE(snr_sox, 0), COALESCE(snr_wada, 0), COALESCE(snr_spectral, 0),
          COALESCE(rms_db, 0), COALESCE(noise_level, ''),
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
			&af.SNRDB, &af.SNRSox, &af.SNRWada, &af.SNRSpectral,
			&af.RMSDB, &af.NoiseLevel,
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

// UpdateAudioStats обновляет статистику аудио
func (d *DB) UpdateAudioStats(id int64, stats *audio.AudioStats) error {
	quality := stats.Quality()
	metadata := stats.ToJSON()

	_, err := d.conn.Exec(`
		UPDATE audio_files SET
			snr_db = ?,
			snr_sox = ?,
			snr_wada = ?,
			snr_spectral = ?,
			noise_level = ?,
			rms_db = ?,
			audio_quality_score = ?,
			audio_quality_level = ?,
			audio_metadata = ?
		WHERE id = ?
	`, stats.SNREstimate, stats.SNRSox, stats.SNRWada, stats.SNRSpectral,
		stats.NoiseLevel, stats.RMSLevDB,
		quality.Score, quality.Level, metadata, id)
	return err
}

// GetFilesForAnalyze возвращает файлы для анализа
// force=true — все файлы, force=false — только без SNR
func (db *DB) GetFilesForAnalyze(limit int, force bool) ([]AudioFile, error) {
	var query string
	if force {
		query = `SELECT id, file_path FROM audio_files WHERE active = 1 LIMIT ?`
	} else {
		query = `SELECT id, file_path FROM audio_files WHERE (snr_db IS NULL OR snr_db = 0) AND active = 1 LIMIT ?`
	}

	rows, err := db.conn.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

// DeleteFile удаляет файл из БД
func (db *DB) DeleteFile(id int64) error {
	_, err := db.conn.Exec("DELETE FROM audio_files WHERE id = ?", id)
	return err
}
