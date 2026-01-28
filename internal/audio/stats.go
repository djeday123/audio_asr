package audio

import (
	"bufio"
	"encoding/json"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

type AudioStats struct {
	// Sox metrics
	DCOffset    float64 `json:"dc_offset"`
	MinLevel    float64 `json:"min_level"`
	MaxLevel    float64 `json:"max_level"`
	PkLevDB     float64 `json:"pk_lev_db"`
	RMSLevDB    float64 `json:"rms_lev_db"`
	RMSPkDB     float64 `json:"rms_pk_db"`
	RMSTrDB     float64 `json:"rms_tr_db"`
	CrestFactor float64 `json:"crest_factor"`
	FlatFactor  float64 `json:"flat_factor"`
	PkCount     int     `json:"pk_count"`
	BitDepth    string  `json:"bit_depth"`
	NumSamples  string  `json:"num_samples"`
	LengthSec   float64 `json:"length_sec"`

	// SNR estimates (разные методы)
	SNRSox      float64 `json:"snr_sox"`      // RMS Pk - RMS Tr
	SNRSpectral float64 `json:"snr_spectral"` // ffmpeg astats
	SNRVad      float64 `json:"snr_vad"`      // silence detection
	SNRWada     float64 `json:"snr_wada"`     // WADA algorithm
	SNREstimate float64 `json:"snr_estimate"` // Combined estimate

	// Quality
	NoiseLevel string `json:"noise_level"` // low, medium, high, very_high
}

// AudioQuality на основе метрик
type AudioQuality struct {
	Score       int    `json:"score"`
	Level       string `json:"level"`
	IsTooQuiet  bool   `json:"is_too_quiet"`
	IsTooLoud   bool   `json:"is_too_loud"`
	IsClipping  bool   `json:"is_clipping"`
	HasDCOffset bool   `json:"has_dc_offset"`
}

// GetStats собирает все метрики
func GetStats(path string) (*AudioStats, error) {
	stats := &AudioStats{}

	// Method 1: Sox stats
	getSoxStats(path, stats)

	// SNR from Sox (RMS Pk - RMS Tr)
	// Если RMS Tr = -inf (нет тишины), используем альтернативный метод
	if math.IsInf(stats.RMSTrDB, -1) {
		// Альтернатива: оценка на основе Crest Factor и RMS level
		// Чистая речь: Crest Factor 10-15, RMS -30 to -20 dB
		baseSNR := 20.0

		// Crest Factor 8-18 — идеально для речи
		if stats.CrestFactor >= 8 && stats.CrestFactor <= 18 {
			baseSNR += 10
		} else if stats.CrestFactor >= 5 && stats.CrestFactor <= 25 {
			baseSNR += 5
		}

		// RMS level -35 to -18 — нормальная громкость
		if stats.RMSLevDB >= -35 && stats.RMSLevDB <= -18 {
			baseSNR += 5
		}

		// Flat factor > 0 означает клиппинг — плохо
		if stats.FlatFactor > 0 {
			baseSNR -= 15
		}

		stats.SNRSox = baseSNR
	} else if stats.RMSTrDB < 0 && stats.RMSPkDB < 0 && stats.RMSTrDB != stats.RMSPkDB {
		snr := stats.RMSPkDB - stats.RMSTrDB
		if snr > 0 && snr < 100 {
			stats.SNRSox = snr
		}
	}

	// Method 2: Spectral (ffmpeg astats)
	stats.SNRSpectral = getSpectralSNR(path)

	// Method 3: VAD-based (silence detection)
	stats.SNRVad = getVADBasedSNR(path)

	// Method 4: WADA-SNR (Go native, самый точный для речи)
	if snr, err := WADASNR(path); err == nil && snr > 0 && snr < 100 {
		stats.SNRWada = snr
	}

	// Combined estimate (взвешенное среднее всех методов)
	stats.SNREstimate = combineSNRAll(stats.SNRSox, stats.SNRSpectral, stats.SNRVad, stats.SNRWada)
	stats.NoiseLevel = classifyNoise(stats.SNREstimate)

	return stats, nil
}

// getSoxStats через sox stats
func getSoxStats(path string, stats *AudioStats) error {
	cmd := exec.Command("sox", path, "-n", "stats")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		key := strings.Join(parts[:len(parts)-1], " ")
		val := parts[len(parts)-1]

		switch key {
		case "DC offset":
			stats.DCOffset, _ = strconv.ParseFloat(val, 64)
		case "Min level":
			stats.MinLevel, _ = strconv.ParseFloat(val, 64)
		case "Max level":
			stats.MaxLevel, _ = strconv.ParseFloat(val, 64)
		case "Pk lev dB":
			stats.PkLevDB, _ = strconv.ParseFloat(val, 64)
		case "RMS lev dB":
			stats.RMSLevDB, _ = strconv.ParseFloat(val, 64)
		case "RMS Pk dB":
			stats.RMSPkDB, _ = strconv.ParseFloat(val, 64)
		case "RMS Tr dB":
			stats.RMSTrDB, _ = strconv.ParseFloat(val, 64)
		case "Crest factor":
			stats.CrestFactor, _ = strconv.ParseFloat(val, 64)
		case "Flat factor":
			stats.FlatFactor, _ = strconv.ParseFloat(val, 64)
		case "Pk count":
			stats.PkCount, _ = strconv.Atoi(val)
		case "Bit-depth":
			stats.BitDepth = val
		case "Num samples":
			stats.NumSamples = val
		case "Length s":
			stats.LengthSec, _ = strconv.ParseFloat(val, 64)
		}
	}

	cmd.Wait()
	return scanner.Err()
}

// getSpectralSNR через ffmpeg astats
func getSpectralSNR(path string) float64 {
	cmd := exec.Command("ffmpeg",
		"-i", path,
		"-af", "astats",
		"-f", "null", "-",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	var rmsLevel, peakLevel float64
	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		if strings.Contains(line, "RMS level dB:") {
			idx := strings.LastIndex(line, ":")
			if idx >= 0 && idx < len(line)-1 {
				valStr := strings.TrimSpace(line[idx+1:])
				rmsLevel, _ = strconv.ParseFloat(valStr, 64)
			}
		}
		if strings.Contains(line, "Peak level dB:") {
			idx := strings.LastIndex(line, ":")
			if idx >= 0 && idx < len(line)-1 {
				valStr := strings.TrimSpace(line[idx+1:])
				peakLevel, _ = strconv.ParseFloat(valStr, 64)
			}
		}
	}

	// Crest factor в dB — разница между пиком и RMS
	if rmsLevel < 0 && peakLevel < 0 {
		crest := peakLevel - rmsLevel
		return crest * 1.5
	}

	return 0
}

// getVADBasedSNR через silencedetect
func getVADBasedSNR(path string) float64 {
	cmd := exec.Command("ffmpeg",
		"-i", path,
		"-af", "silencedetect=noise=-40dB:d=0.1",
		"-f", "null", "-",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	// Считаем количество silence_start событий
	silenceCount := strings.Count(string(output), "silence_start")

	// Если много пауз — вероятно чистая запись
	// Мало пауз — либо непрерывная речь, либо шум
	if silenceCount > 5 {
		return 25 // Хорошее качество
	} else if silenceCount > 2 {
		return 18
	}
	return 12
}

// combineSNRAll комбинирует все методы с весами
func combineSNRAll(soxSNR, spectralSNR, vadSNR, wadaSNR float64) float64 {
	values := []float64{}
	weights := []float64{}

	// Sox — самый надёжный для телефонии
	if isValidSNR(soxSNR) {
		values = append(values, soxSNR)
		weights = append(weights, 3.0) // было 1.0
	}

	if isValidSNR(spectralSNR) {
		values = append(values, spectralSNR)
		weights = append(weights, 1.0) // было 1.5
	}

	if isValidSNR(vadSNR) {
		values = append(values, vadSNR)
		weights = append(weights, 0.5)
	}

	if isValidSNR(wadaSNR) {
		values = append(values, wadaSNR)
		weights = append(weights, 1.5) // было 2.5
	}

	if len(values) == 0 {
		return 0
	}

	var sum, weightSum float64
	for i, v := range values {
		sum += v * weights[i]
		weightSum += weights[i]
	}

	result := sum / weightSum

	if math.IsNaN(result) || math.IsInf(result, 0) {
		return 0
	}

	return math.Round(result*10) / 10
}

// isValidSNR проверяет что SNR значение валидное
func isValidSNR(snr float64) bool {
	if math.IsNaN(snr) || math.IsInf(snr, 0) {
		return false
	}
	if snr <= 0 || snr > 100 {
		return false
	}
	return true
}

// classifyNoise классифицирует уровень шума
func classifyNoise(snr float64) string {
	if snr >= 25 {
		return "low" // Чистая запись
	} else if snr >= 18 {
		return "medium" // Небольшой шум
	} else if snr >= 10 {
		return "high" // Заметный шум
	}
	return "very_high" // Сильный шум
}

// Quality анализ качества аудио
func (s *AudioStats) Quality() AudioQuality {
	q := AudioQuality{}

	q.IsTooQuiet = s.RMSLevDB < -40
	q.IsTooLoud = s.RMSLevDB > -10
	q.IsClipping = s.FlatFactor > 0
	q.HasDCOffset = s.DCOffset > 0.01 || s.DCOffset < -0.01

	// Расчёт score (0-100)
	score := 100

	// Штраф за тихий сигнал
	if s.RMSLevDB < -40 {
		score -= 30
	} else if s.RMSLevDB < -35 {
		score -= 15
	}

	// Штраф за громкий сигнал
	if s.RMSLevDB > -10 {
		score -= 20
	}

	// Штраф за клиппинг
	if s.FlatFactor > 0 {
		score -= int(s.FlatFactor * 10)
	}

	// Штраф за DC offset
	if q.HasDCOffset {
		score -= 10
	}

	// Бонус/штраф за SNR
	if s.SNREstimate >= 30 {
		score += 10
	} else if s.SNREstimate < 15 {
		score -= 20
	}

	// Оптимальный RMS для речи: -25 to -18 dB
	if s.RMSLevDB >= -25 && s.RMSLevDB <= -18 {
		score += 10
	}

	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	q.Score = score

	if score >= 80 {
		q.Level = "good"
	} else if score >= 50 {
		q.Level = "medium"
	} else {
		q.Level = "poor"
	}

	return q
}

// ToJSON для сохранения в БД
func (s *AudioStats) ToJSON() string {
	// Создаём копию с заменой Inf/NaN на 0
	safe := *s
	if math.IsInf(safe.RMSTrDB, 0) || math.IsNaN(safe.RMSTrDB) {
		safe.RMSTrDB = 0
	}
	if math.IsInf(safe.RMSPkDB, 0) || math.IsNaN(safe.RMSPkDB) {
		safe.RMSPkDB = 0
	}
	if math.IsInf(safe.PkLevDB, 0) || math.IsNaN(safe.PkLevDB) {
		safe.PkLevDB = 0
	}
	if math.IsInf(safe.RMSLevDB, 0) || math.IsNaN(safe.RMSLevDB) {
		safe.RMSLevDB = 0
	}
	if math.IsInf(safe.SNRSox, 0) || math.IsNaN(safe.SNRSox) {
		safe.SNRSox = 0
	}
	if math.IsInf(safe.SNRSpectral, 0) || math.IsNaN(safe.SNRSpectral) {
		safe.SNRSpectral = 0
	}
	if math.IsInf(safe.SNREstimate, 0) || math.IsNaN(safe.SNREstimate) {
		safe.SNREstimate = 0
	}

	b, err := json.Marshal(safe)
	if err != nil {
		return "{}"
	}
	return string(b)
}
