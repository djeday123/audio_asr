package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	statsCache     map[string]interface{}
	statsCacheTime time.Time
	statsCacheMu   sync.RWMutex
)

func (db *DB) StatsExtendedCached() (map[string]interface{}, error) {
	statsCacheMu.RLock()
	if statsCache != nil && time.Since(statsCacheTime) < 30*time.Second {
		defer statsCacheMu.RUnlock()
		return statsCache, nil
	}
	statsCacheMu.RUnlock()

	// Получаем свежие данные
	stats, err := db.StatsExtended()
	if err != nil {
		return nil, err
	}

	statsCacheMu.Lock()
	statsCache = stats
	statsCacheTime = time.Now()
	statsCacheMu.Unlock()

	return stats, nil
}

type AudioFile struct {
	ID                         int64     `json:"id"`
	UserID                     string    `json:"user_id"`
	ChapterID                  string    `json:"chapter_id"`
	MergedID                   int64     `json:"merged_id"`
	FilePath                   string    `json:"file_path"`
	FileHash                   string    `json:"file_hash"`
	DurationSec                float64   `json:"duration_sec"`
	SampleRate                 int       `json:"sample_rate"`
	Channels                   int       `json:"channels"`
	BitDepth                   int       `json:"bit_depth"`
	FileSize                   int64     `json:"file_size"`
	SNRDB                      float64   `json:"snr_db"`
	SNRSox                     float64   `json:"snr_sox"`
	SNRSpectral                float64   `json:"snr_spectral"`
	SNRWada                    float64   `json:"snr_wada"`
	NoiseLevel                 string    `json:"noise_level"`
	RMSDB                      float64   `json:"rms_db"`
	AudioMetadata              string    `json:"audio_metadata"`
	TranscriptionOriginal      string    `json:"transcription_original"`
	TranscriptionASR           string    `json:"transcription_asr"`
	TranscriptionWhisperLocal  string    `json:"transcription_whisper_local"`
	TranscriptionWhisperOpenAI string    `json:"transcription_whisper_openai"`
	WER                        float64   `json:"wer"`
	CER                        float64   `json:"cer"`
	WERWhisperLocal            float64   `json:"wer_whisper_local"`
	CERWhisperLocal            float64   `json:"cer_whisper_local"`
	WERWhisperOpenAI           float64   `json:"wer_whisper_openai"`
	CERWhisperOpenAI           float64   `json:"cer_whisper_openai"`
	ASRStatus                  string    `json:"asr_status"`
	WhisperLocalStatus         string    `json:"whisper_local_status"`
	WhisperOpenAIStatus        string    `json:"whisper_openai_status"`
	ReviewStatus               string    `json:"review_status"`
	CreatedAt                  time.Time `json:"created_at"`

	// Kaldi NoLM
	TranscriptionASRNoLM string  `json:"transcription_asr_nolm"`
	WERNoLM              float64 `json:"wer_nolm"`
	CERNoLM              float64 `json:"cer_nolm"`
	ASRNoLMStatus        string  `json:"asr_nolm_status"`

	// Verification
	OperatorVerified bool       `json:"operator_verified"`
	VerifiedAt       *time.Time `json:"verified_at"`
	OriginalEdited   bool       `json:"original_edited"`

	// Silence & Merge  <-- ДОБАВИТЬ ЭТИ ПОЛЯ
	HasTrailingSilence bool   `json:"has_trailing_silence"`
	SilenceAdded       bool   `json:"silence_added"`
	ParentIDs          string `json:"parent_ids,omitempty"`

	Active bool `json:"active"`
}

// AudioFileRecalc - структура для пересчёта WER/CER
type AudioFileRecalc struct {
	ID                         int64
	TranscriptionOriginal      string
	TranscriptionASR           string
	TranscriptionASRNoLM       string
	TranscriptionWhisperLocal  string
	TranscriptionWhisperOpenAI string
}

type FileListResult struct {
	Files []AudioFile `json:"files"`
	Total int64       `json:"total"`
	Page  int         `json:"page"`
	Limit int         `json:"limit"`
}

type DB struct {
	conn *sql.DB
}

func New(host string, port int, user, password, dbname string) (*DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true",
		user, password, host, port, dbname)

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(50)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(5 * time.Minute)

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	return &DB{conn: conn}, nil
}

func (d *DB) DB() *sql.DB {
	return d.conn
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) ExistsByHash(hash string) (bool, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM audio_files WHERE file_hash = ?", hash).Scan(&count)
	return count > 0, err
}

func (db *DB) ExistsByPath(path string) (bool, error) {
	var count int
	err := db.conn.QueryRow("SELECT COUNT(*) FROM audio_files WHERE file_path = ?", path).Scan(&count)
	return count > 0, err
}

func (db *DB) Insert(af *AudioFile) (int64, error) {
	res, err := db.conn.Exec(`
		INSERT INTO audio_files 
		(user_id, chapter_id, file_path, file_hash, duration_sec, 
		 snr_db, snr_sox, snr_wada, noise_level, rms_db,
		 sample_rate, channels, bit_depth, file_size, audio_metadata, 
		 transcription_original, asr_status, asr_nolm_status, whisper_local_status, whisper_openai_status, review_status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'pending', 'pending', 'pending', 'pending')`,
		af.UserID, af.ChapterID, af.FilePath, af.FileHash, af.DurationSec,
		af.SNRDB, af.SNRSox, af.SNRWada, af.NoiseLevel, af.RMSDB,
		af.SampleRate, af.Channels, af.BitDepth, af.FileSize,
		af.AudioMetadata, af.TranscriptionOriginal)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *DB) UpdateError(id int64, errMsg string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files SET asr_status = 'error', 
		audio_metadata = JSON_SET(COALESCE(audio_metadata, '{}'), '$.error', ?)
		WHERE id = ?`, errMsg, id)
	return err
}

func (db *DB) GetPending(limit int) ([]AudioFile, error) {
	query := `
		SELECT id, file_path, file_hash, transcription_original 
		FROM audio_files 
		WHERE asr_status = 'pending'`

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
		if err := rows.Scan(&af.ID, &af.FilePath, &af.FileHash, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

func (db *DB) Stats() (total, pending, processed, errors int, err error) {
	err = db.conn.QueryRow(`
		SELECT 
			COUNT(*),
			COALESCE(SUM(CASE WHEN asr_status = 'pending' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN asr_status = 'processed' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN asr_status = 'error' THEN 1 ELSE 0 END), 0)
		FROM audio_files`).Scan(&total, &pending, &processed, &errors)
	return
}

func (db *DB) GetSpeakers() ([]string, error) {
	rows, err := db.conn.Query("SELECT DISTINCT user_id FROM audio_files ORDER BY user_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var speakers []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		speakers = append(speakers, s)
	}
	return speakers, nil
}

// StatsExtended возвращает расширенную статистику включая верификацию и все pending
func (db *DB) StatsExtended() (map[string]interface{}, error) {
	result := make(map[string]interface{})

	var total, pending, processed, errors int
	var verified, needsReview int
	var pendingNoLM, processedNoLM int
	var pendingWhisperLocal, processedWhisperLocal int
	var pendingWhisperOpenAI, processedWhisperOpenAI int

	db.conn.QueryRow(`
        SELECT 
            COUNT(*),
            COALESCE(SUM(CASE WHEN asr_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN asr_status = 'processed' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN asr_status = 'error' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN operator_verified = 1 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN wer > 0.15 AND operator_verified = 0 THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN asr_nolm_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN asr_nolm_status = 'processed' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN whisper_local_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN whisper_local_status = 'processed' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN whisper_openai_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN whisper_openai_status = 'processed' THEN 1 ELSE 0 END), 0)
        FROM audio_files`).Scan(&total, &pending, &processed, &errors, &verified, &needsReview,
		&pendingNoLM, &processedNoLM, &pendingWhisperLocal, &processedWhisperLocal,
		&pendingWhisperOpenAI, &processedWhisperOpenAI)

	result["total"] = total
	result["pending"] = pending
	result["processed"] = processed
	result["errors"] = errors
	result["verified"] = verified
	result["needs_review"] = needsReview
	result["pending_nolm"] = pendingNoLM
	result["processed_nolm"] = processedNoLM
	result["pending_whisper_local"] = pendingWhisperLocal
	result["processed_whisper_local"] = processedWhisperLocal
	result["pending_whisper_openai"] = pendingWhisperOpenAI
	result["processed_whisper_openai"] = processedWhisperOpenAI

	return result, nil
}

func (db *DB) AvgMetrics() (avgWER, avgCER float64, err error) {
	err = db.conn.QueryRow(`
		SELECT 
			COALESCE(AVG(wer), 0),
			COALESCE(AVG(cer), 0)
		FROM audio_files 
		WHERE asr_status = 'processed'`).Scan(&avgWER, &avgCER)
	return
}

// AvgMetricsAll - обновлённая версия с NoLM метриками
func (db *DB) AvgMetricsAll_old() (map[string]float64, error) {
	result := make(map[string]float64)

	var kaldiWer, kaldiCer float64
	var kaldiNoLMWer, kaldiNoLMCer float64
	var whisperLocalWer, whisperLocalCer float64
	var whisperOpenaiWer, whisperOpenaiCer float64

	// Kaldi
	db.conn.QueryRow(`
        SELECT COALESCE(AVG(wer), 0), COALESCE(AVG(cer), 0)
        FROM audio_files 
        WHERE asr_status = 'processed'`).Scan(&kaldiWer, &kaldiCer)

	// Kaldi NoLM
	db.conn.QueryRow(`
        SELECT COALESCE(AVG(wer_nolm), 0), COALESCE(AVG(cer_nolm), 0)
        FROM audio_files 
        WHERE asr_nolm_status = 'processed'`).Scan(&kaldiNoLMWer, &kaldiNoLMCer)

	// Whisper Local
	db.conn.QueryRow(`
        SELECT COALESCE(AVG(wer_whisper_local), 0), COALESCE(AVG(cer_whisper_local), 0)
        FROM audio_files 
        WHERE whisper_local_status = 'processed'`).Scan(&whisperLocalWer, &whisperLocalCer)

	// Whisper OpenAI
	db.conn.QueryRow(`
        SELECT COALESCE(AVG(wer_whisper_openai), 0), COALESCE(AVG(cer_whisper_openai), 0)
        FROM audio_files 
        WHERE whisper_openai_status = 'processed'`).Scan(&whisperOpenaiWer, &whisperOpenaiCer)

	result["kaldi_wer"] = kaldiWer
	result["kaldi_cer"] = kaldiCer
	result["kaldi_nolm_wer"] = kaldiNoLMWer
	result["kaldi_nolm_cer"] = kaldiNoLMCer
	result["whisper_local_wer"] = whisperLocalWer
	result["whisper_local_cer"] = whisperLocalCer
	result["whisper_openai_wer"] = whisperOpenaiWer
	result["whisper_openai_cer"] = whisperOpenaiCer

	return result, nil
}

func (db *DB) AvgMetricsAll() (map[string]float64, error) {
	result := make(map[string]float64)

	var kaldiWer, kaldiCer float64
	var kaldiNoLMWer, kaldiNoLMCer float64
	var whisperLocalWer, whisperLocalCer float64
	var whisperOpenaiWer, whisperOpenaiCer float64

	db.conn.QueryRow(`
        SELECT 
            COALESCE(AVG(CASE WHEN asr_status = 'processed' THEN wer END), 0),
            COALESCE(AVG(CASE WHEN asr_status = 'processed' THEN cer END), 0),
            COALESCE(AVG(CASE WHEN asr_nolm_status = 'processed' THEN wer_nolm END), 0),
            COALESCE(AVG(CASE WHEN asr_nolm_status = 'processed' THEN cer_nolm END), 0),
            COALESCE(AVG(CASE WHEN whisper_local_status = 'processed' THEN wer_whisper_local END), 0),
            COALESCE(AVG(CASE WHEN whisper_local_status = 'processed' THEN cer_whisper_local END), 0),
            COALESCE(AVG(CASE WHEN whisper_openai_status = 'processed' THEN wer_whisper_openai END), 0),
            COALESCE(AVG(CASE WHEN whisper_openai_status = 'processed' THEN cer_whisper_openai END), 0)
        FROM audio_files
    `).Scan(&kaldiWer, &kaldiCer, &kaldiNoLMWer, &kaldiNoLMCer,
		&whisperLocalWer, &whisperLocalCer, &whisperOpenaiWer, &whisperOpenaiCer)

	result["kaldi_wer"] = kaldiWer
	result["kaldi_cer"] = kaldiCer
	result["kaldi_nolm_wer"] = kaldiNoLMWer
	result["kaldi_nolm_cer"] = kaldiNoLMCer
	result["whisper_local_wer"] = whisperLocalWer
	result["whisper_local_cer"] = whisperLocalCer
	result["whisper_openai_wer"] = whisperOpenaiWer
	result["whisper_openai_cer"] = whisperOpenaiCer

	return result, nil
}

// GetFileForRecalc - обновлённая версия с NoLM
func (db *DB) GetFileForRecalc(id int64) (*AudioFileRecalc, error) {
	var af AudioFileRecalc
	err := db.conn.QueryRow(`
        SELECT id, 
               COALESCE(transcription_original, ''),
               COALESCE(transcription_asr, ''),
               COALESCE(transcription_asr_nolm, ''),
               COALESCE(transcription_whisper_local, ''),
               COALESCE(transcription_whisper_openai, '')
        FROM audio_files WHERE id = ?`, id).Scan(
		&af.ID, &af.TranscriptionOriginal, &af.TranscriptionASR,
		&af.TranscriptionASRNoLM,
		&af.TranscriptionWhisperLocal, &af.TranscriptionWhisperOpenAI)
	if err != nil {
		return nil, err
	}
	return &af, nil
}

// GetAllForRecalc - обновлённая версия с NoLM
func (db *DB) GetAllForRecalc() ([]AudioFileRecalc, error) {
	rows, err := db.conn.Query(`
        SELECT id, 
               COALESCE(transcription_original, ''),
               COALESCE(transcription_asr, ''),
               COALESCE(transcription_asr_nolm, ''),
               COALESCE(transcription_whisper_local, ''),
               COALESCE(transcription_whisper_openai, '')
        FROM audio_files 
        WHERE asr_status = 'processed' 
           OR asr_nolm_status = 'processed'
           OR whisper_local_status = 'processed' 
           OR whisper_openai_status = 'processed'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []AudioFileRecalc
	for rows.Next() {
		var af AudioFileRecalc
		if err := rows.Scan(&af.ID, &af.TranscriptionOriginal, &af.TranscriptionASR,
			&af.TranscriptionASRNoLM,
			&af.TranscriptionWhisperLocal, &af.TranscriptionWhisperOpenAI); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

// ============================================================
// МЕТОДЫ ДЛЯ NoLM (Kaldi без языковой модели)
// ============================================================

// GetPendingNoLM возвращает файлы для обработки Kaldi без LM
func (db *DB) GetPendingNoLM(limit int) ([]AudioFile, error) {
	query := `
		SELECT id, file_path, file_hash, transcription_original 
		FROM audio_files 
		WHERE asr_nolm_status = 'pending'`

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
		if err := rows.Scan(&af.ID, &af.FilePath, &af.FileHash, &af.TranscriptionOriginal); err != nil {
			return nil, err
		}
		files = append(files, af)
	}
	return files, nil
}

// ============================================================
// МЕТОДЫ ДЛЯ верификации и редактирования
// ============================================================

// UpdateOriginalTranscription обновляет оригинальную транскрипцию (редактирование оператором)
func (db *DB) UpdateOriginalTranscription(id int64, text string) error {
	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET transcription_original = ?, original_edited = 1
		WHERE id = ?`, text, id)
	return err
}

// SetVerificationStatus устанавливает/снимает статус верификации
func (db *DB) SetVerificationStatus(id int64, verified bool) error {
	var verifiedAt interface{}
	if verified {
		verifiedAt = time.Now()
	} else {
		verifiedAt = nil
	}

	_, err := db.conn.Exec(`
		UPDATE audio_files 
		SET operator_verified = ?, verified_at = ?
		WHERE id = ?`, verified, verifiedAt, id)
	return err
}

// GetNextSplitChapter возвращает следующий chapter ID для split файлов
// Формат: 9XXXXXXX где XXXXXXX = max_chapter + 1
func (d *DB) GetNextSplitChapter(userID string) (string, error) {
	var maxChapter sql.NullString

	// Ищем максимальный chapter с префиксом 9 для этого спикера
	err := d.conn.QueryRow(`
		SELECT MAX(chapter_id) 
		FROM audio_files 
		WHERE user_id = ? AND chapter_id LIKE '9%'
	`, userID).Scan(&maxChapter)

	if err != nil {
		return "", err
	}

	if !maxChapter.Valid || maxChapter.String == "" {
		// Первый split для этого спикера — берём max обычный chapter + 1
		var maxNormal sql.NullInt64
		err := d.conn.QueryRow(`
			SELECT MAX(CAST(chapter_id AS UNSIGNED)) 
			FROM audio_files 
			WHERE user_id = ? AND chapter_id NOT LIKE '9%'
		`, userID).Scan(&maxNormal)

		if err != nil || !maxNormal.Valid {
			return "90000001", nil // default
		}

		return fmt.Sprintf("9%d", maxNormal.Int64+1), nil
	}

	// Есть split chapters — берём max + 1
	// Убираем префикс 9, парсим число, +1, добавляем 9
	numStr := strings.TrimPrefix(maxChapter.String, "9")
	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return fmt.Sprintf("9%d", time.Now().Unix()), nil // fallback
	}

	return fmt.Sprintf("9%d", num+1), nil
}
