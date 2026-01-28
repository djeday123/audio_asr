package db

import "fmt"

// === Whisper Local ===

func (db *DB) GetWhisperLocalPending(limit int) ([]AudioFile, error) {
	query := `SELECT id, file_path, transcription_original 
	          FROM audio_files 
	          WHERE whisper_local_status = 'pending'`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

func (db *DB) UpdateWhisperLocal(id int64, transcription string, wer, cer float64) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_whisper_local = ?, 
		    wer_whisper_local = ?, 
		    cer_whisper_local = ?, 
		    whisper_local_status = 'processed'
		WHERE id = ?`,
		transcription, wer, cer, id)
	return err
}

func (db *DB) UpdateWhisperLocalError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files SET whisper_local_status = 'error' WHERE id = ?`, id)
	return err
}

// === Whisper OpenAI ===

// GetWhisperOpenAIPendingAll получает ВСЕ pending файлы для OpenAI (без фильтра)
func (db *DB) GetWhisperOpenAIPendingAll(limit int) ([]AudioFile, error) {
	query := `SELECT id, file_path, transcription_original 
	          FROM audio_files 
	          WHERE whisper_openai_status = 'pending'`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

// GetWhisperOpenAIPending получает файлы где локальный WER > minLocalWER
func (db *DB) GetWhisperOpenAIPending(limit int, minLocalWER float64) ([]AudioFile, error) {
	query := `SELECT id, file_path, transcription_original 
	          FROM audio_files 
	          WHERE whisper_openai_status = 'pending'
	            AND whisper_local_status = 'processed'
	            AND wer_whisper_local > ?`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.conn.Query(query, minLocalWER)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFile
	for rows.Next() {
		var af AudioFile
		if err := rows.Scan(&af.ID, &af.FilePath, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

func (db *DB) UpdateWhisperOpenAI(id int64, transcription string, wer, cer float64) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_whisper_openai = ?, 
		    wer_whisper_openai = ?, 
		    cer_whisper_openai = ?, 
		    whisper_openai_status = 'processed'
		WHERE id = ?`,
		transcription, wer, cer, id)
	return err
}

func (db *DB) UpdateWhisperOpenAIError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files SET whisper_openai_status = 'error' WHERE id = ?`, id)
	return err
}

func (db *DB) UpdateWhisperLocalMetrics(id int64, wer, cer float64) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET wer_whisper_local = ?, cer_whisper_local = ? WHERE id = ?`, wer, cer, id)
	return err
}

func (db *DB) UpdateWhisperOpenAIMetrics(id int64, wer, cer float64) error {
	_, err := db.conn.Exec(`UPDATE audio_files SET wer_whisper_openai = ?, cer_whisper_openai = ? WHERE id = ?`, wer, cer, id)
	return err
}
