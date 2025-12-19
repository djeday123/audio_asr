package asr

import (
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

type Client struct {
    baseURL    string
    apiKey     string
    httpClient *http.Client
}

type Response struct {
    Success        bool    `json:"success"`
    Text           string  `json:"text"`
    Filename       string  `json:"filename"`
    Duration       float64 `json:"duration"`
    ProcessingTime float64 `json:"processing_time"`
    RTF            float64 `json:"rtf"`
    Error          string  `json:"error,omitempty"`
}

func NewClient(baseURL, apiKey string, timeout time.Duration) *Client {
    return &Client{
        baseURL: baseURL,
        apiKey:  apiKey,
        httpClient: &http.Client{
            Timeout: timeout,
        },
    }
}

func (c *Client) Recognize(audioPath string) (*Response, error) {
    // Открываем файл
    f, err := os.Open(audioPath)
    if err != nil {
        return nil, fmt.Errorf("open file: %w", err)
    }
    defer f.Close()

    // Создаём multipart form
    var buf bytes.Buffer
    writer := multipart.NewWriter(&buf)
    
    part, err := writer.CreateFormFile("audio", filepath.Base(audioPath))
    if err != nil {
        return nil, fmt.Errorf("create form file: %w", err)
    }
    
    if _, err := io.Copy(part, f); err != nil {
        return nil, fmt.Errorf("copy file: %w", err)
    }
    writer.Close()

    // Создаём запрос
    req, err := http.NewRequest("POST", c.baseURL+"/recognize", &buf)
    if err != nil {
        return nil, fmt.Errorf("create request: %w", err)
    }
    
    req.Header.Set("Content-Type", writer.FormDataContentType())
    req.Header.Set("X-API-Key", c.apiKey)

    // Отправляем
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return nil, fmt.Errorf("do request: %w", err)
    }
    defer resp.Body.Close()

    // Парсим ответ
    var result Response
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return nil, fmt.Errorf("decode response: %w", err)
    }

    if !result.Success {
        return nil, fmt.Errorf("asr error: %s", result.Error)
    }

    return &result, nil
}