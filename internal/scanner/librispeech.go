package scanner

import (
    "bufio"
    "os"
    "path/filepath"
    "strings"
)

type AudioTask struct {
    WavPath      string
    Transcription string
}

// Сканирует LibriSpeech структуру и возвращает канал с задачами
func ScanLibriSpeech(rootDir string) (<-chan AudioTask, <-chan error) {
    tasks := make(chan AudioTask, 1000)
    errs := make(chan error, 1)
    
    go func() {
        defer close(tasks)
        defer close(errs)
        
        err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
            if err != nil {
                return err
            }
            
            if !info.IsDir() && strings.HasSuffix(path, ".trans.txt") {
                if err := parseTransFile(path, tasks); err != nil {
                    return err
                }
            }
            return nil
        })
        
        if err != nil {
            errs <- err
        }
    }()
    
    return tasks, errs
}

func parseTransFile(transPath string, tasks chan<- AudioTask) error {
    f, err := os.Open(transPath)
    if err != nil {
        return err
    }
    defer f.Close()
    
    dir := filepath.Dir(transPath)
    scanner := bufio.NewScanner(f)
    
    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if line == "" {
            continue
        }
        
        // Формат: "ID текст транскрипции"
        parts := strings.SplitN(line, " ", 2)
        if len(parts) != 2 {
            continue
        }
        
        id := parts[0]
        text := parts[1]
        wavPath := filepath.Join(dir, id+".wav")
        
        // Проверяем существование wav
        if _, err := os.Stat(wavPath); err == nil {
            tasks <- AudioTask{
                WavPath:      wavPath,
                Transcription: text,
            }
        }
    }
    
    return scanner.Err()
}

// Подсчёт файлов для прогресса
func CountFiles(rootDir string) (int, error) {
    count := 0
    err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        if !info.IsDir() && strings.HasSuffix(path, ".wav") {
            count++
        }
        return nil
    })
    return count, err
}
