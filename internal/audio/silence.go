package audio

import (
	"audio-labeler/internal/utils"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// SilenceInfo информация о тишине в конце файла
type SilenceInfo struct {
	HasTrailingSilence bool    `json:"has_trailing_silence"`
	SilenceDuration    float64 `json:"silence_duration_ms"` // в миллисекундах
	TotalDuration      float64 `json:"total_duration"`
}

// GetAudioDuration получает длительность аудио через ffprobe
func GetAudioDuration(path string) (float64, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path)

	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	var duration float64
	_, err = fmt.Sscanf(strings.TrimSpace(string(out)), "%f", &duration)
	return duration, err
}

// DetectTrailingSilence проверяет есть ли тишина в конце аудио
// Kaldi требует минимум 100ms тишины в конце
func DetectTrailingSilence(wavPath string, minSilenceMs float64) (*SilenceInfo, error) {
	if minSilenceMs <= 0 {
		minSilenceMs = 100 // 100ms по умолчанию для Kaldi
	}

	// Получаем длительность файла
	duration, err := GetAudioDuration(wavPath)
	if err != nil {
		return nil, err
	}

	// Используем sox для определения тишины в конце
	// reverse -> silence detect -> получаем длину тишины с конца
	cmd := exec.Command("sox", wavPath, "-n", "reverse", "silence", "1", "0.01", "1%", "reverse", "stat")
	output, _ := cmd.CombinedOutput()

	// Альтернативный метод через ffmpeg silencedetect
	silenceDur := detectSilenceFFmpeg(wavPath, duration)

	info := &SilenceInfo{
		TotalDuration:      duration,
		SilenceDuration:    silenceDur * 1000, // в ms
		HasTrailingSilence: silenceDur*1000 >= minSilenceMs,
	}

	_ = output // sox output для дебага если нужно

	return info, nil
}

// detectSilenceFFmpeg определяет тишину в конце через ffmpeg
func detectSilenceFFmpeg(wavPath string, totalDuration float64) float64 {
	// Анализируем последние 500ms файла
	startTime := totalDuration - 0.5
	if startTime < 0 {
		startTime = 0
	}

	cmd := exec.Command("ffmpeg",
		"-ss", fmt.Sprintf("%.3f", startTime),
		"-i", wavPath,
		"-af", "silencedetect=noise=-40dB:d=0.05",
		"-f", "null", "-",
	)

	output, _ := cmd.CombinedOutput()
	outStr := string(output)

	// Ищем silence_end в конце файла
	// [silencedetect @ ...] silence_end: 4.532 | silence_duration: 0.156
	lines := strings.Split(outStr, "\n")
	var lastSilenceDur float64

	for _, line := range lines {
		if strings.Contains(line, "silence_duration:") {
			parts := strings.Split(line, "silence_duration:")
			if len(parts) > 1 {
				durStr := strings.TrimSpace(strings.Split(parts[1], "|")[0])
				if dur, err := strconv.ParseFloat(durStr, 64); err == nil {
					lastSilenceDur = dur
				}
			}
		}
	}

	// Проверяем RMS в последних 100ms
	if lastSilenceDur == 0 {
		lastSilenceDur = checkRMSAtEnd(wavPath, totalDuration)
	}

	return lastSilenceDur
}

// checkRMSAtEnd проверяет RMS уровень в последних N мс
func checkRMSAtEnd(wavPath string, totalDuration float64) float64 {
	// Проверяем последние 100ms
	startTime := totalDuration - 0.1
	if startTime < 0 {
		return 0
	}

	cmd := exec.Command("sox", wavPath, "-n",
		"trim", fmt.Sprintf("%.3f", startTime),
		"stat")

	output, _ := cmd.CombinedOutput()
	outStr := string(output)

	// Ищем RMS amplitude
	for _, line := range strings.Split(outStr, "\n") {
		if strings.Contains(line, "RMS") && strings.Contains(line, "amplitude") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				if rms, err := strconv.ParseFloat(parts[len(parts)-1], 64); err == nil {
					// RMS < 0.01 считаем тишиной
					if rms < 0.01 {
						return 0.1 // 100ms тишины
					}
				}
			}
		}
	}

	return 0
}

// AddTrailingSilence добавляет тишину в конец файла
func AddTrailingSilence(inputPath, outputPath string, silenceMs float64) error {
	if silenceMs <= 0 {
		silenceMs = 100
	}

	// Получаем sample rate исходного файла
	meta, err := GetMetadata(inputPath)
	if err != nil {
		return err
	}

	sampleRate := meta.SampleRate
	if sampleRate == 0 {
		sampleRate = 8000 // fallback для телефонных записей
	}

	// sox input.wav output.wav pad 0 0.1 (добавляет 100ms в конец)
	silenceSec := silenceMs / 1000.0

	cmd := exec.Command("sox", inputPath, outputPath,
		"pad", "0", fmt.Sprintf("%.3f", silenceSec))

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sox pad error: %v, output: %s", err, string(output))
	}

	return nil
}

// RemoveTrailingSilence удаляет тишину с конца файла
func RemoveTrailingSilence(inputPath, outputPath string) error {
	// sox input.wav output.wav reverse silence 1 0.01 1% reverse
	cmd := exec.Command("sox", inputPath, outputPath,
		"reverse", "silence", "1", "0.01", "1%", "reverse")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sox silence remove error: %v, output: %s", err, string(output))
	}

	return nil
}

// MergeAudioFiles склеивает несколько WAV файлов в один с паузами между ними
func MergeAudioFiles(inputPaths []string, outputPath string, pauseMs float64) error {
	if len(inputPaths) == 0 {
		return fmt.Errorf("no input files")
	}

	// Создаём директорию если не существует
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}

	if len(inputPaths) == 1 {
		// Просто копируем
		return utils.CopyFile(inputPaths[0], outputPath)
	}

	// По умолчанию 150ms пауза
	if pauseMs <= 0 {
		pauseMs = 150
	}

	// Получаем sample rate из первого файла
	meta, err := GetMetadata(inputPaths[0])
	if err != nil {
		return err
	}
	sampleRate := meta.SampleRate
	if sampleRate == 0 {
		sampleRate = 16000
	}

	// Создаём временный файл тишины
	tmpDir, err := os.MkdirTemp("", "merge_")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	silencePath := filepath.Join(tmpDir, "silence.wav")
	pauseSec := pauseMs / 1000.0

	// Генерируем тишину: sox -n -r 16000 -c 1 silence.wav trim 0.0 0.15
	cmdSilence := exec.Command("sox", "-n", "-r", fmt.Sprintf("%d", sampleRate),
		"-c", "1", "-b", "16", silencePath, "trim", "0.0", fmt.Sprintf("%.3f", pauseSec))
	if output, err := cmdSilence.CombinedOutput(); err != nil {
		return fmt.Errorf("create silence failed: %v, output: %s", err, string(output))
	}

	// Строим список файлов с тишиной между ними
	// file1 silence file2 silence file3 -> output
	var args []string
	for i, path := range inputPaths {
		args = append(args, path)
		// Добавляем тишину после каждого файла кроме последнего
		if i < len(inputPaths)-1 {
			args = append(args, silencePath)
		}
	}
	args = append(args, outputPath)

	// sox file1.wav silence.wav file2.wav silence.wav file3.wav output.wav
	cmd := exec.Command("sox", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sox concat error: %v, output: %s", err, string(output))
	}

	return nil
}
