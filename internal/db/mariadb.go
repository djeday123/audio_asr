package db

import (
    "database/sql"
    "fmt"
    "time"

    _ "github.com/go-sql-driver/mysql"
)

type AudioFile struct {
    ID                    int64
    FilePath              string
    FileHash              string
    DurationSec           float64
    SampleRate            int
    Channels              int
    BitDepth              int
    FileSize              int64
    AudioMetadata         string // JSON
    TranscriptionOriginal string
    TranscriptionASR      string
    WER                   float64
    CER                   float64
    ASRStatus             string
    ReviewStatus          string
    CreatedAt             time.Time
    ProcessedAt           sql.NullTime
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

func (db *DB) Close() error {
    return db.conn.Close()
}

// Проверка существует ли файл по хешу
func (db *DB) ExistsByHash(hash string) (bool, error) {
    var count int
    err := db.conn.QueryRow("SELECT COUNT(*) FROM audio_files WHERE file_hash = ?", hash).Scan(&count)
    return count > 0, err
}

// Проверка существует ли файл по пути
func (db *DB) ExistsByPath(path string) (bool, error) {
    var count int
    err := db.conn.QueryRow("SELECT COUNT(*) FROM audio_files WHERE file_path = ?", path).Scan(&count)
    return count > 0, err
}

// Вставка нового файла
func (db *DB) Insert(af *AudioFile) (int64, error) {
    res, err := db.conn.Exec(`
        INSERT INTO audio_files 
        (file_path, file_hash, duration_sec, sample_rate, channels, bit_depth, 
         file_size, audio_metadata, transcription_original, asr_status, review_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'pending')`,
        af.FilePath, af.FileHash, af.DurationSec, af.SampleRate, af.Channels,
        af.BitDepth, af.FileSize, af.AudioMetadata, af.TranscriptionOriginal)
    if err != nil {
        return 0, err
    }
    return res.LastInsertId()
}

// Обновление результата ASR
func (db *DB) UpdateASR(id int64, transcription string, wer, cer float64) error {
    _, err := db.conn.Exec(`
        UPDATE audio_files 
        SET transcription_asr = ?, wer = ?, cer = ?, 
            asr_status = 'processed', processed_at = NOW()
        WHERE id = ?`,
        transcription, wer, cer, id)
    return err
}

// Обновление статуса ошибки
func (db *DB) UpdateError(id int64, errMsg string) error {
    _, err := db.conn.Exec(`
        UPDATE audio_files SET asr_status = 'error', audio_metadata = JSON_SET(COALESCE(audio_metadata, '{}'), '$.error', ?)
        WHERE id = ?`, errMsg, id)
    return err
}

// Получить файлы для обработки
func (db *DB) GetPending(limit int) ([]AudioFile, error) {
    rows, err := db.conn.Query(`
        SELECT id, file_path, file_hash, transcription_original 
        FROM audio_files 
        WHERE asr_status = 'pending' 
        LIMIT ?`, limit)
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

// Статистика
func (db *DB) Stats() (total, pending, processed, errors int, err error) {
    err = db.conn.QueryRow(`
        SELECT 
            COUNT(*),
            SUM(CASE WHEN asr_status = 'pending' THEN 1 ELSE 0 END),
            SUM(CASE WHEN asr_status = 'processed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN asr_status = 'error' THEN 1 ELSE 0 END)
        FROM audio_files`).Scan(&total, &pending, &processed, &errors)
    return
}
