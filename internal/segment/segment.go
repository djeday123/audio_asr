package segment

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Segment - сегмент из pyannote
type Segment struct {
	ID          int64   `json:"id"`
	AudioFileID int64   `json:"audio_file_id"`
	StartTime   float64 `json:"start"`
	EndTime     float64 `json:"end"`
	Speaker     string  `json:"speaker"`
	HasOverlap  bool    `json:"has_overlap"`
	Selected    bool    `json:"selected"`
	Transcript  string  `json:"transcript"`
}

// Overlap - пересечение сегментов
type Overlap struct {
	Start    float64  `json:"start"`
	End      float64  `json:"end"`
	Speakers []string `json:"speakers"`
}

// SpeakerStat - статистика по спикеру
type SpeakerStat struct {
	Duration float64 `json:"duration"`
	Segments int     `json:"segments"`
}

// DiarizeRequest - запрос к pyannote API
type DiarizeRequest struct {
	AudioPath   string `json:"audio_path"`
	MinSpeakers *int   `json:"min_speakers,omitempty"`
	MaxSpeakers *int   `json:"max_speakers,omitempty"`
}

// DiarizeResponse - ответ от pyannote API
type DiarizeResponse struct {
	Segments     []PyannoteSegment      `json:"segments"`
	Overlaps     []Overlap              `json:"overlaps"`
	SpeakerStats map[string]SpeakerStat `json:"speaker_stats"`
	NumSpeakers  int                    `json:"num_speakers"`
	Duration     float64                `json:"duration"`
}

// PyannoteSegment - сегмент от pyannote
type PyannoteSegment struct {
	Start      float64 `json:"start"`
	End        float64 `json:"end"`
	Speaker    string  `json:"speaker"`
	HasOverlap bool    `json:"has_overlap"`
}

// ========================================
// Client - клиент для pyannote API
// ========================================

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
		},
	}
}

func (c *Client) Health() error {
	resp, err := c.httpClient.Get(c.baseURL + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pyannote unhealthy: %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) Diarize(audioPath string, minSpeakers, maxSpeakers *int) (*DiarizeResponse, error) {
	req := DiarizeRequest{
		AudioPath:   audioPath,
		MinSpeakers: minSpeakers,
		MaxSpeakers: maxSpeakers,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Post(
		c.baseURL+"/diarize",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("diarize failed: %d - %s", resp.StatusCode, string(bodyBytes))
	}

	var result DiarizeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ========================================
// Repository - работа с БД
// ========================================

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) CreateTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS audio_segments (
		id INT AUTO_INCREMENT PRIMARY KEY,
		audio_file_id INT NOT NULL,
		start_time DECIMAL(10,3) NOT NULL,
		end_time DECIMAL(10,3) NOT NULL,
		speaker VARCHAR(50),
		has_overlap TINYINT(1) DEFAULT 0,
		selected TINYINT(1) DEFAULT 0,
		transcript TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_audio_file (audio_file_id)
	)`
	_, err := r.db.Exec(query)
	return err
}

func (r *Repository) DeleteByAudioFile(audioFileID int64) error {
	_, err := r.db.Exec("DELETE FROM audio_segments WHERE audio_file_id = ?", audioFileID)
	return err
}

func (r *Repository) InsertSegments(audioFileID int64, segments []PyannoteSegment) error {
	if err := r.DeleteByAudioFile(audioFileID); err != nil {
		return err
	}

	stmt, err := r.db.Prepare(`
		INSERT INTO audio_segments (audio_file_id, start_time, end_time, speaker, has_overlap)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, seg := range segments {
		_, err := stmt.Exec(audioFileID, seg.Start, seg.End, seg.Speaker, seg.HasOverlap)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) GetByAudioFile(audioFileID int64) ([]Segment, error) {
	rows, err := r.db.Query(`
		SELECT id, audio_file_id, start_time, end_time, speaker, has_overlap, selected, COALESCE(transcript, '')
		FROM audio_segments
		WHERE audio_file_id = ?
		ORDER BY start_time
	`, audioFileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var segments []Segment
	for rows.Next() {
		var s Segment
		err := rows.Scan(&s.ID, &s.AudioFileID, &s.StartTime, &s.EndTime,
			&s.Speaker, &s.HasOverlap, &s.Selected, &s.Transcript)
		if err != nil {
			return nil, err
		}
		segments = append(segments, s)
	}

	return segments, nil
}

func (r *Repository) UpdateSelection(segmentIDs []int64, selected bool) error {
	if len(segmentIDs) == 0 {
		return nil
	}

	query := "UPDATE audio_segments SET selected = ? WHERE id IN ("
	args := []interface{}{selected}
	for i, id := range segmentIDs {
		if i > 0 {
			query += ","
		}
		query += "?"
		args = append(args, id)
	}
	query += ")"

	_, err := r.db.Exec(query, args...)
	return err
}

func (r *Repository) UpdateTranscript(segmentID int64, transcript string) error {
	_, err := r.db.Exec(
		"UPDATE audio_segments SET transcript = ? WHERE id = ?",
		transcript, segmentID,
	)
	return err
}

func (r *Repository) UpdateTranscriptsBatch(transcripts map[int64]string) error {
	if len(transcripts) == 0 {
		return nil
	}

	stmt, err := r.db.Prepare("UPDATE audio_segments SET transcript = ? WHERE id = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for id, text := range transcripts {
		_, err := stmt.Exec(text, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Repository) GetSelected(audioFileID int64) ([]Segment, error) {
	rows, err := r.db.Query(`
		SELECT id, audio_file_id, start_time, end_time, speaker, has_overlap, selected, COALESCE(transcript, '')
		FROM audio_segments
		WHERE audio_file_id = ? AND selected = 1
		ORDER BY start_time
	`, audioFileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var segments []Segment
	for rows.Next() {
		var s Segment
		err := rows.Scan(&s.ID, &s.AudioFileID, &s.StartTime, &s.EndTime,
			&s.Speaker, &s.HasOverlap, &s.Selected, &s.Transcript)
		if err != nil {
			return nil, err
		}
		segments = append(segments, s)
	}

	return segments, nil
}

func (r *Repository) HasSegments(audioFileID int64) (bool, error) {
	var count int
	err := r.db.QueryRow(
		"SELECT COUNT(*) FROM audio_segments WHERE audio_file_id = ?",
		audioFileID,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *Repository) CombineTranscripts(audioFileID int64) (string, error) {
	segments, err := r.GetSelected(audioFileID)
	if err != nil {
		return "", err
	}

	if len(segments) == 0 {
		return "", fmt.Errorf("no selected segments")
	}

	var result string
	for i, seg := range segments {
		if seg.Transcript != "" {
			if i > 0 && result != "" {
				result += " "
			}
			result += seg.Transcript
		}
	}

	return result, nil
}
