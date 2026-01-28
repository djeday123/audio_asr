package asr

import (
	"audio-labeler/internal/audio"
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type KaldiDecoder struct {
	kaldiRoot  string
	modelPath  string
	graphDir   string
	wordsTxt   string
	hclgFst    string
	onlineConf string
	lmScale    float64
}

type DecodeResult struct {
	Text           string
	Duration       float64
	ProcessingTime float64
	RTF            float64
	Success        bool
	Error          string
}

func NewKaldiDecoder(modelDir string) (*KaldiDecoder, error) {
	kaldiRoot := "/opt/kaldi"

	d := &KaldiDecoder{
		kaldiRoot:  kaldiRoot,
		modelPath:  filepath.Join(modelDir, "model/final.mdl"),
		graphDir:   filepath.Join(modelDir, "graph"),
		wordsTxt:   filepath.Join(modelDir, "graph/words.txt"),
		hclgFst:    filepath.Join(modelDir, "graph/HCLG.fst"),
		onlineConf: filepath.Join(modelDir, "conf/online.conf"),
		lmScale:    1.0,
	}

	files := []string{d.modelPath, d.wordsTxt, d.hclgFst, d.onlineConf}
	for _, f := range files {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			return nil, fmt.Errorf("missing file: %s", f)
		}
	}

	return d, nil
}

func NewKaldiDecoderNoLM(modelDir string) (*KaldiDecoder, error) {
	d, err := NewKaldiDecoder(modelDir)
	if err != nil {
		return nil, err
	}
	d.lmScale = 0.0
	return d, nil
}

func (d *KaldiDecoder) SetLMScale(scale float64) {
	d.lmScale = scale
}

func (d *KaldiDecoder) GetLMScale() float64 {
	return d.lmScale
}

// ============================================================
// Single file decoding
// ============================================================

func (d *KaldiDecoder) Decode(wavPath string) (*DecodeResult, error) {
	if _, err := os.Stat(wavPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("audio file not found: %s", wavPath)
	}

	duration, err := audio.GetAudioDuration(wavPath)
	if err != nil {
		return nil, fmt.Errorf("get duration: %w", err)
	}

	uttID := fmt.Sprintf("utt_%d", time.Now().UnixNano())

	// NoLM режим — используем lattice rescoring
	if d.lmScale == 0 {
		return d.decodeWithRescoring(wavPath, uttID, duration)
	}

	// Обычный режим — прямое декодирование
	return d.decodeDirect(wavPath, uttID, duration)
}

// decodeDirect — обычное декодирование с LM
func (d *KaldiDecoder) decodeDirect(wavPath, uttID string, duration float64) (*DecodeResult, error) {
	start := time.Now()

	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		fmt.Sprintf("ark:echo %s %s |", uttID, uttID),
		fmt.Sprintf("scp:echo %s %s |", uttID, wavPath),
		"ark:/dev/null",
	)

	output, err := cmd.CombinedOutput()
	elapsed := time.Since(start).Seconds()

	if err != nil {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("decode error: %v, output: %s", err, string(output)),
		}, nil
	}

	text := d.parseOutput(string(output), uttID)

	rtf := 0.0
	if duration > 0 {
		rtf = elapsed / duration
	}

	return &DecodeResult{
		Text:           text,
		Duration:       duration,
		ProcessingTime: elapsed,
		RTF:            rtf,
		Success:        true,
	}, nil
}

// decodeWithRescoring — декодирование с lattice rescoring (lm-scale=0)
func (d *KaldiDecoder) decodeWithRescoring(wavPath, uttID string, duration float64) (*DecodeResult, error) {
	start := time.Now()

	tmpDir, err := os.MkdirTemp("", "kaldi_nolm_")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	latticePath := filepath.Join(tmpDir, "lat.ark")

	// Шаг 1: Декодируем в lattice
	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd1 := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		fmt.Sprintf("ark:echo %s %s |", uttID, uttID),
		fmt.Sprintf("scp:echo %s %s |", uttID, wavPath),
		"ark:"+latticePath,
	)

	if output, err := cmd1.CombinedOutput(); err != nil {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("decode step failed: %v, output: %s", err, string(output)),
		}, nil
	}

	// Проверяем что lattice создан
	if fi, err := os.Stat(latticePath); err != nil || fi.Size() == 0 {
		return &DecodeResult{
			Success: false,
			Error:   "lattice file empty or not created",
		}, nil
	}

	// Шаг 2: Rescoring + Best Path + Convert to words
	rescoreBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-scale")
	bestPathBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-best-path")
	int2symPl := filepath.Join(d.kaldiRoot, "egs/wsj/s5/utils/int2sym.pl")

	// Проверяем наличие int2sym.pl
	if _, err := os.Stat(int2symPl); os.IsNotExist(err) {
		// Пробуем альтернативный путь
		int2symPl = filepath.Join(d.kaldiRoot, "egs/work_3/s5/utils/int2sym.pl")
	}

	cmd2 := exec.Command("bash", "-c", fmt.Sprintf(
		"%s --lm-scale=0.0 --acoustic-scale=1.0 'ark:%s' ark:- | %s ark:- ark,t:- | %s -f 2- %s",
		rescoreBin, latticePath, bestPathBin, int2symPl, d.wordsTxt,
	))

	output, err := cmd2.CombinedOutput()
	elapsed := time.Since(start).Seconds()

	if err != nil {
		return &DecodeResult{
			Success: false,
			Error:   fmt.Sprintf("rescore step failed: %v, output: %s", err, string(output)),
		}, nil
	}

	text := d.parseOutput(string(output), uttID)

	rtf := 0.0
	if duration > 0 {
		rtf = elapsed / duration
	}

	return &DecodeResult{
		Text:           text,
		Duration:       duration,
		ProcessingTime: elapsed,
		RTF:            rtf,
		Success:        true,
	}, nil
}

// ============================================================
// Batch decoding
// ============================================================

func (d *KaldiDecoder) DecodeBatch(wavPaths []string) (map[string]*DecodeResult, error) {
	if len(wavPaths) == 0 {
		return nil, nil
	}

	// NoLM режим — используем batch lattice rescoring
	if d.lmScale == 0 {
		return d.decodeBatchWithRescoring(wavPaths)
	}

	// Обычный режим — прямое batch декодирование
	return d.decodeBatchDirect(wavPaths)
}

// decodeBatchDirect — batch декодирование с LM
func (d *KaldiDecoder) decodeBatchDirect(wavPaths []string) (map[string]*DecodeResult, error) {
	results := make(map[string]*DecodeResult)

	tmpDir, err := os.MkdirTemp("", "kaldi_batch_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	wavScp := filepath.Join(tmpDir, "wav.scp")
	spk2utt := filepath.Join(tmpDir, "spk2utt")

	// Создаём wav.scp
	wavScpFile, err := os.Create(wavScp)
	if err != nil {
		return nil, err
	}

	uttIDs := make(map[string]string) // uttID -> wavPath
	var uttList []string

	for i, path := range wavPaths {
		uttID := fmt.Sprintf("utt%06d", i)
		uttIDs[uttID] = path
		uttList = append(uttList, uttID)
		fmt.Fprintf(wavScpFile, "%s %s\n", uttID, path)
	}
	wavScpFile.Close()

	// Создаём spk2utt
	spk2uttFile, err := os.Create(spk2utt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(spk2uttFile, "global %s\n", strings.Join(uttList, " "))
	spk2uttFile.Close()

	start := time.Now()

	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		"ark:"+spk2utt,
		"scp:"+wavScp,
		"ark:/dev/null",
	)

	output, err := cmd.CombinedOutput()
	totalElapsed := time.Since(start).Seconds()

	if err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch decode error: %v", err),
			}
		}
		return results, nil
	}

	// Парсим результаты
	transcriptions := d.parseOutputBatch(string(output), uttIDs)

	// Формируем результаты
	for uttID, path := range uttIDs {
		duration, _ := audio.GetAudioDuration(path)
		text := transcriptions[uttID]

		rtf := 0.0
		avgTime := totalElapsed / float64(len(wavPaths))
		if duration > 0 {
			rtf = avgTime / duration
		}

		results[path] = &DecodeResult{
			Text:           text,
			Duration:       duration,
			ProcessingTime: avgTime,
			RTF:            rtf,
			Success:        true,
		}
	}

	return results, nil
}

// decodeBatchWithRescoring — batch декодирование с lattice rescoring (NoLM)
func (d *KaldiDecoder) decodeBatchWithRescoring(wavPaths []string) (map[string]*DecodeResult, error) {
	results := make(map[string]*DecodeResult)

	tmpDir, err := os.MkdirTemp("", "kaldi_batch_nolm_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	wavScp := filepath.Join(tmpDir, "wav.scp")
	spk2utt := filepath.Join(tmpDir, "spk2utt")
	latticePath := filepath.Join(tmpDir, "lat.ark")

	// Создаём wav.scp
	wavScpFile, err := os.Create(wavScp)
	if err != nil {
		return nil, err
	}

	uttIDs := make(map[string]string) // uttID -> wavPath
	var uttList []string

	for i, path := range wavPaths {
		uttID := fmt.Sprintf("utt%06d", i)
		uttIDs[uttID] = path
		uttList = append(uttList, uttID)
		fmt.Fprintf(wavScpFile, "%s %s\n", uttID, path)
	}
	wavScpFile.Close()

	// Создаём spk2utt
	spk2uttFile, err := os.Create(spk2utt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(spk2uttFile, "global %s\n", strings.Join(uttList, " "))
	spk2uttFile.Close()

	start := time.Now()

	// Шаг 1: Batch декодирование в lattice
	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd1 := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		"ark:"+spk2utt,
		"scp:"+wavScp,
		"ark:"+latticePath,
	)

	if output, err := cmd1.CombinedOutput(); err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch decode step failed: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Проверяем lattice
	if fi, err := os.Stat(latticePath); err != nil || fi.Size() == 0 {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   "batch lattice file empty or not created",
			}
		}
		return results, nil
	}

	// Шаг 2: Batch rescoring + best path
	rescoreBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-scale")
	bestPathBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-best-path")

	int2symPl := filepath.Join(d.kaldiRoot, "egs/wsj/s5/utils/int2sym.pl")
	if _, err := os.Stat(int2symPl); os.IsNotExist(err) {
		int2symPl = filepath.Join(d.kaldiRoot, "egs/work_3/s5/utils/int2sym.pl")
	}

	cmd2 := exec.Command("bash", "-c", fmt.Sprintf(
		"%s --lm-scale=0.0 --acoustic-scale=1.0 'ark:%s' ark:- | %s ark:- ark,t:- | %s -f 2- %s",
		rescoreBin, latticePath, bestPathBin, int2symPl, d.wordsTxt,
	))

	output, err := cmd2.CombinedOutput()
	totalElapsed := time.Since(start).Seconds()

	if err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch rescore step failed: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Парсим результаты
	transcriptions := d.parseOutputBatch(string(output), uttIDs)

	// Формируем результаты
	for uttID, path := range uttIDs {
		duration, _ := audio.GetAudioDuration(path)
		text := transcriptions[uttID]

		rtf := 0.0
		avgTime := totalElapsed / float64(len(wavPaths))
		if duration > 0 {
			rtf = avgTime / duration
		}

		results[path] = &DecodeResult{
			Text:           text,
			Duration:       duration,
			ProcessingTime: avgTime,
			RTF:            rtf,
			Success:        true,
		}
	}

	return results, nil
}

// ============================================================
// Helper functions
// ============================================================

// parseOutput извлекает текст для одного utterance
func (d *KaldiDecoder) parseOutput(output, uttID string) string {
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, uttID+" ") {
			parts := strings.SplitN(line, " ", 2)
			if len(parts) > 1 {
				return parts[1]
			}
			return ""
		}
	}
	return ""
}

// parseOutputBatch извлекает тексты для множества utterances
func (d *KaldiDecoder) parseOutputBatch(output string, uttIDs map[string]string) map[string]string {
	transcriptions := make(map[string]string)

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		for uttID := range uttIDs {
			if strings.HasPrefix(line, uttID+" ") {
				parts := strings.SplitN(line, " ", 2)
				if len(parts) > 1 {
					transcriptions[uttID] = parts[1]
				} else {
					transcriptions[uttID] = ""
				}
				break
			}
		}
	}

	return transcriptions
}

func (d *KaldiDecoder) Health() error {
	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")
	if _, err := os.Stat(decoderBin); os.IsNotExist(err) {
		return fmt.Errorf("decoder not found: %s", decoderBin)
	}

	// Проверяем lattice tools для NoLM режима
	rescoreBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-scale")
	if _, err := os.Stat(rescoreBin); os.IsNotExist(err) {
		return fmt.Errorf("lattice-scale not found: %s", rescoreBin)
	}

	bestPathBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-best-path")
	if _, err := os.Stat(bestPathBin); os.IsNotExist(err) {
		return fmt.Errorf("lattice-best-path not found: %s", bestPathBin)
	}

	return nil
}

// ============================================================
// GPU Batch decoding (через batched-wav-nnet3-cuda)
// ============================================================

// DecodeBatchGPU — batch декодирование на GPU
func (d *KaldiDecoder) DecodeBatchGPU(wavPaths []string) (map[string]*DecodeResult, error) {
	if len(wavPaths) == 0 {
		return nil, nil
	}

	// NoLM режим — GPU + lattice rescoring
	if d.lmScale == 0 {
		return d.decodeBatchGPUWithRescoring(wavPaths)
	}

	// Обычный режим — GPU batch
	return d.decodeBatchGPUDirect(wavPaths)
}

// decodeBatchGPUDirect — GPU batch декодирование с LM
func (d *KaldiDecoder) decodeBatchGPUDirect(wavPaths []string) (map[string]*DecodeResult, error) {
	results := make(map[string]*DecodeResult)

	tmpDir, err := os.MkdirTemp("", "kaldi_gpu_batch_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	wavScp := filepath.Join(tmpDir, "wav.scp")
	spk2utt := filepath.Join(tmpDir, "spk2utt")

	// Создаём wav.scp
	wavScpFile, err := os.Create(wavScp)
	if err != nil {
		return nil, err
	}

	uttIDs := make(map[string]string)
	var uttList []string

	for i, path := range wavPaths {
		uttID := fmt.Sprintf("utt%06d", i)
		uttIDs[uttID] = path
		uttList = append(uttList, uttID)
		fmt.Fprintf(wavScpFile, "%s %s\n", uttID, path)
	}
	wavScpFile.Close()

	// Создаём spk2utt
	spk2uttFile, err := os.Create(spk2utt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(spk2uttFile, "global %s\n", strings.Join(uttList, " "))
	spk2uttFile.Close()

	start := time.Now()

	// GPU batch decoder
	gpuDecoder := filepath.Join(d.kaldiRoot, "src/cudadecoderbin/batched-wav-nnet3-cuda")

	cmd := exec.Command(gpuDecoder,
		"--config="+d.onlineConf,
		"--cuda-decoder-copy-threads=2",
		"--cuda-worker-threads=4",
		"--max-batch-size="+fmt.Sprintf("%d", len(wavPaths)),
		"--num-channels="+fmt.Sprintf("%d", len(wavPaths)),
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		"ark:"+spk2utt,
		"scp:"+wavScp,
		"ark,t:-",
	)

	output, err := cmd.CombinedOutput()
	totalElapsed := time.Since(start).Seconds()

	if err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("GPU batch decode error: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Парсим результаты
	transcriptions := d.parseOutputBatch(string(output), uttIDs)

	for uttID, path := range uttIDs {
		duration, _ := audio.GetAudioDuration(path)
		text := transcriptions[uttID]

		rtf := 0.0
		avgTime := totalElapsed / float64(len(wavPaths))
		if duration > 0 {
			rtf = avgTime / duration
		}

		results[path] = &DecodeResult{
			Text:           text,
			Duration:       duration,
			ProcessingTime: avgTime,
			RTF:            rtf,
			Success:        true,
		}
	}

	return results, nil
}

// decodeBatchGPUWithRescoring — GPU batch + lattice rescoring (NoLM)
func (d *KaldiDecoder) decodeBatchGPUWithRescoring(wavPaths []string) (map[string]*DecodeResult, error) {
	results := make(map[string]*DecodeResult)

	tmpDir, err := os.MkdirTemp("", "kaldi_gpu_batch_nolm_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	wavScp := filepath.Join(tmpDir, "wav.scp")
	spk2utt := filepath.Join(tmpDir, "spk2utt")
	latticePath := filepath.Join(tmpDir, "lat.ark")

	// Создаём wav.scp
	wavScpFile, err := os.Create(wavScp)
	if err != nil {
		return nil, err
	}

	uttIDs := make(map[string]string)
	var uttList []string

	for i, path := range wavPaths {
		uttID := fmt.Sprintf("utt%06d", i)
		uttIDs[uttID] = path
		uttList = append(uttList, uttID)
		fmt.Fprintf(wavScpFile, "%s %s\n", uttID, path)
	}
	wavScpFile.Close()

	// Создаём spk2utt
	spk2uttFile, err := os.Create(spk2utt)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(spk2uttFile, "global %s\n", strings.Join(uttList, " "))
	spk2uttFile.Close()

	start := time.Now()

	// Шаг 1: GPU batch декодирование в lattice
	// Используем CPU декодер для lattice output (GPU decoder не поддерживает lattice output напрямую)
	decoderBin := filepath.Join(d.kaldiRoot, "src/online2bin/online2-wav-nnet3-latgen-faster")

	cmd1 := exec.Command(decoderBin,
		"--config="+d.onlineConf,
		"--frame-subsampling-factor=3",
		"--max-active=7000",
		"--beam=15.0",
		"--lattice-beam=8.0",
		"--acoustic-scale=1.0",
		"--word-symbol-table="+d.wordsTxt,
		d.modelPath,
		d.hclgFst,
		"ark:"+spk2utt,
		"scp:"+wavScp,
		"ark:"+latticePath,
	)

	if output, err := cmd1.CombinedOutput(); err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch decode step failed: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Шаг 2: Batch rescoring + best path
	rescoreBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-scale")
	bestPathBin := filepath.Join(d.kaldiRoot, "src/latbin/lattice-best-path")
	int2symPl := filepath.Join(d.kaldiRoot, "egs/work_3/s5/utils/int2sym.pl")

	cmd2 := exec.Command("bash", "-c", fmt.Sprintf(
		"%s --lm-scale=0.0 --acoustic-scale=1.0 'ark:%s' ark:- | %s ark:- ark,t:- | %s -f 2- %s",
		rescoreBin, latticePath, bestPathBin, int2symPl, d.wordsTxt,
	))

	output, err := cmd2.CombinedOutput()
	totalElapsed := time.Since(start).Seconds()

	if err != nil {
		for _, path := range wavPaths {
			results[path] = &DecodeResult{
				Success: false,
				Error:   fmt.Sprintf("batch rescore step failed: %v, output: %s", err, string(output)),
			}
		}
		return results, nil
	}

	// Парсим результаты
	transcriptions := d.parseOutputBatch(string(output), uttIDs)

	for uttID, path := range uttIDs {
		duration, _ := audio.GetAudioDuration(path)
		text := transcriptions[uttID]

		rtf := 0.0
		avgTime := totalElapsed / float64(len(wavPaths))
		if duration > 0 {
			rtf = avgTime / duration
		}

		results[path] = &DecodeResult{
			Text:           text,
			Duration:       duration,
			ProcessingTime: avgTime,
			RTF:            rtf,
			Success:        true,
		}
	}

	return results, nil
}
