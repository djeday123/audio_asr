package db

import (
	"fmt"
	"os"
	"strconv"
)

// InsertSplitFile добавляет нарезанный файл в БД
// InsertSplitFile добавляет нарезанный файл в БД
func (d *DB) InsertSplitFile(filePath, userID, chapterID, transcript string, duration float64, sourceID int64, fileHash string) (int64, error) {
	chapterInt, _ := strconv.ParseInt(chapterID, 10, 64)

	var fileSize int64
	if fi, err := os.Stat(filePath); err == nil {
		fileSize = fi.Size()
	}

	result, err := d.conn.Exec(`
		INSERT INTO audio_files (
			file_path, 
			user_id, 
			chapter_id, 
			chapter_id_int,
			parent_ids,
			transcription_original, 
			duration_sec, 
			sample_rate, 
			channels, 
			bit_depth,
			file_size,
			file_hash,
			audio_metadata,
			asr_status, 
			asr_nolm_status, 
			whisper_local_status, 
			whisper_openai_status,
			review_status,
			split_source_id, 
			active
		) VALUES (?, ?, ?, ?, ?, ?, ?, 8000, 1, 16, ?, ?, '{}', 'pending', 'pending', 'pending', 'pending', 'pending', ?, 1)
	`, filePath, userID, chapterID, chapterInt, fmt.Sprintf("%d", sourceID), transcript, duration, fileSize, fileHash, sourceID)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// MarkAsSplitSource помечает файл как источник для split
func (d *DB) MarkAsSplitSource(id int64) error {
	_, err := d.conn.Exec("UPDATE audio_files SET split_source_id  = 1 WHERE id = ?", id)
	return err
}

// GetNextFileIndex возвращает следующий индекс файла в chapter
func (d *DB) GetNextFileIndex(userID, chapterID string) (int, error) {
	var count int
	err := d.conn.QueryRow(`
		SELECT COUNT(*) FROM audio_files 
		WHERE user_id = ? AND chapter_id = ?
	`, userID, chapterID).Scan(&count)
	if err != nil {
		return 1, err
	}
	return count + 1, nil
}
