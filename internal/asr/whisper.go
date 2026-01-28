package asr

import (
	"audio-labeler/internal/audio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// === Whisper Local Client (faster-whisper) ===

type WhisperLocalClient struct {
	baseURL    string
	language   string
	httpClient *http.Client
}

func NewWhisperLocalClient(baseURL, language string) *WhisperLocalClient {
	if language == "" {
		language = "az"
	}
	return &WhisperLocalClient{
		baseURL:  baseURL,
		language: language,
		httpClient: &http.Client{
			Timeout: 300 * time.Second,
		},
	}
}

func (c *WhisperLocalClient) Transcribe(audioPath string) (*DecodeResult, error) {
	start := time.Now()

	file, err := os.Open(audioPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("audio", filepath.Base(audioPath))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(part, file); err != nil {
		return nil, err
	}

	writer.WriteField("language", c.language)
	writer.Close()

	req, err := http.NewRequest("POST", c.baseURL+"/transcribe", &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("status %d: %s", resp.StatusCode, string(body)),
		}, nil
	}

	var result struct {
		Text     string  `json:"text"`
		Duration float64 `json:"duration"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Seconds()
	duration := result.Duration
	if duration == 0 {
		duration, _ = audio.GetAudioDuration(audioPath)
	}

	rtf := 0.0
	if duration > 0 {
		rtf = elapsed / duration
	}

	return &DecodeResult{
		Text:           result.Text,
		Duration:       duration,
		ProcessingTime: elapsed,
		RTF:            rtf,
		Success:        true,
	}, nil
}

func (c *WhisperLocalClient) Health() error {
	resp, err := c.httpClient.Get(c.baseURL + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// === Whisper OpenAI Client ===

type WhisperOpenAIClient struct {
	baseURL    string
	apiKey     string
	model      string
	language   string
	httpClient *http.Client
}

func NewWhisperOpenAIClient(apiKey, model, language string) *WhisperOpenAIClient {
	if model == "" {
		model = "whisper-1"
	}
	if language == "" {
		language = "az"
	}
	return &WhisperOpenAIClient{
		baseURL:  "https://api.openai.com/v1",
		apiKey:   apiKey,
		model:    model,
		language: language,
		httpClient: &http.Client{
			Timeout: 300 * time.Second,
		},
	}
}

func (c *WhisperOpenAIClient) Transcribe(audioPath string) (*DecodeResult, error) {
	start := time.Now()

	file, err := os.Open(audioPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	part, err := writer.CreateFormFile("file", filepath.Base(audioPath))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(part, file); err != nil {
		return nil, err
	}

	writer.WriteField("model", c.model)
	writer.WriteField("language", c.language)
	writer.Close()

	req, err := http.NewRequest("POST", c.baseURL+"/audio/transcriptions", &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("status %d: %s", resp.StatusCode, string(body)),
		}, nil
	}

	var result struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	elapsed := time.Since(start).Seconds()
	duration, _ := audio.GetAudioDuration(audioPath)

	rtf := 0.0
	if duration > 0 {
		rtf = elapsed / duration
	}

	return &DecodeResult{
		Text:           result.Text,
		Duration:       duration,
		ProcessingTime: elapsed,
		RTF:            rtf,
		Success:        true,
	}, nil
}

func (c *WhisperOpenAIClient) Health() error {
	req, _ := http.NewRequest("GET", c.baseURL+"/models", nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
