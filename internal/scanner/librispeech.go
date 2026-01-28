package scanner

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

type AudioTask struct {
	UserID        string
	ChapterID     string
	WavPath       string
	Transcription string
}

// Сканирует LibriSpeech структуру
func ScanLibriSpeech(rootDir string, limit int) ([]AudioTask, error) {
	var tasks []AudioTask
	count := 0

	users, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, err
	}

	for _, user := range users {
		if !user.IsDir() {
			continue
		}
		userID := user.Name()
		userPath := filepath.Join(rootDir, userID)

		chapters, err := os.ReadDir(userPath)
		if err != nil {
			continue
		}

		for _, chapter := range chapters {
			if !chapter.IsDir() {
				continue
			}
			chapterID := chapter.Name()
			chapterPath := filepath.Join(userPath, chapterID)

			transFile := filepath.Join(chapterPath, userID+"-"+chapterID+".trans.txt")
			transcriptions, err := parseTransFile(transFile)
			if err != nil {
				continue
			}

			for id, text := range transcriptions {
				wavPath := filepath.Join(chapterPath, id+".wav")
				if _, err := os.Stat(wavPath); err == nil {
					tasks = append(tasks, AudioTask{
						UserID:        userID,
						ChapterID:     chapterID,
						WavPath:       wavPath,
						Transcription: text,
					})
					count++
					if limit > 0 && count >= limit {
						return tasks, nil
					}
				}
			}
		}
	}

	return tasks, nil
}

func parseTransFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	result := make(map[string]string)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}

	return result, scanner.Err()
}

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
