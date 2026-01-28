package audio

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
)

// WADA-SNR: Waveform Amplitude Distribution Analysis
func WADASNR(path string) (float64, error) {
	samples, err := readWavSamples(path)
	if err != nil {
		return 0, err
	}

	if len(samples) < 1000 {
		return 0, errors.New("audio too short")
	}

	// Вычисляем адаптивный порог (1% от RMS)
	var sumSq float64
	for _, s := range samples {
		sumSq += s * s
	}
	rmsAll := math.Sqrt(sumSq / float64(len(samples)))
	threshold := rmsAll * 0.1 // 10% от RMS
	if threshold < 0.001 {
		threshold = 0.001
	}
	if threshold > 0.01 {
		threshold = 0.01
	}

	// Убираем тишину
	var filtered []float64
	for _, s := range samples {
		if math.Abs(s) > threshold {
			filtered = append(filtered, s)
		}
	}

	if len(filtered) < 500 {
		return 0, errors.New("not enough non-silent samples")
	}

	// Вычисляем mean absolute и RMS
	var sumAbs, sumSqF float64
	for _, s := range filtered {
		sumAbs += math.Abs(s)
		sumSqF += s * s
	}

	n := float64(len(filtered))
	meanAbs := sumAbs / n
	rms := math.Sqrt(sumSqF / n)

	if rms < 1e-10 {
		return 0, errors.New("signal too quiet")
	}

	gamma := meanAbs / rms

	diff := gamma - 0.707
	if diff < 0.001 {
		diff = 0.001
	}

	snr := -10 * math.Log10(diff/0.091)

	if snr < 0 {
		snr = 0
	}
	if snr > 50 {
		snr = 50
	}

	return math.Round(snr*10) / 10, nil
}

// readWavSamples читает WAV и возвращает нормализованные сэмплы [-1, 1]
func readWavSamples(path string) ([]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Читаем RIFF заголовок (12 байт)
	riffHeader := make([]byte, 12)
	if _, err := io.ReadFull(f, riffHeader); err != nil {
		return nil, err
	}

	if string(riffHeader[0:4]) != "RIFF" || string(riffHeader[8:12]) != "WAVE" {
		return nil, errors.New("not a valid WAV file")
	}

	var numChannels uint16 = 1
	var bitsPerSample uint16 = 16

	// Читаем chunks
	for {
		chunkHeader := make([]byte, 8)
		if _, err := io.ReadFull(f, chunkHeader); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		chunkID := string(chunkHeader[0:4])
		chunkSize := binary.LittleEndian.Uint32(chunkHeader[4:8])

		switch chunkID {
		case "fmt ":
			fmtData := make([]byte, chunkSize)
			if _, err := io.ReadFull(f, fmtData); err != nil {
				return nil, err
			}
			audioFormat := binary.LittleEndian.Uint16(fmtData[0:2])
			if audioFormat != 1 {
				return nil, errors.New("only PCM format supported")
			}
			numChannels = binary.LittleEndian.Uint16(fmtData[2:4])
			bitsPerSample = binary.LittleEndian.Uint16(fmtData[14:16])

		case "data":
			data := make([]byte, chunkSize)
			if _, err := io.ReadFull(f, data); err != nil {
				return nil, err
			}

			bytesPerSample := int(bitsPerSample / 8)
			numSamples := len(data) / bytesPerSample / int(numChannels)
			samples := make([]float64, numSamples)

			for i := 0; i < numSamples; i++ {
				offset := i * bytesPerSample * int(numChannels)

				var sample float64
				switch bitsPerSample {
				case 8:
					sample = (float64(data[offset]) - 128) / 128
				case 16:
					val := int16(binary.LittleEndian.Uint16(data[offset : offset+2]))
					sample = float64(val) / 32768
				case 24:
					b := data[offset : offset+3]
					val := int32(b[0]) | int32(b[1])<<8 | int32(b[2])<<16
					if val&0x800000 != 0 {
						val |= ^0xFFFFFF
					}
					sample = float64(val) / 8388608
				case 32:
					val := int32(binary.LittleEndian.Uint32(data[offset : offset+4]))
					sample = float64(val) / 2147483648
				default:
					return nil, errors.New("unsupported bit depth")
				}

				samples[i] = sample
			}

			return samples, nil

		default:
			// Пропускаем неизвестные chunks (LIST, INFO и т.д.)
			skipSize := int64(chunkSize)
			if chunkSize%2 != 0 {
				skipSize++ // WAV chunks выравнены на 2 байта
			}
			if _, err := f.Seek(skipSize, io.SeekCurrent); err != nil {
				return nil, err
			}
		}
	}

	return nil, errors.New("data chunk not found")
}
