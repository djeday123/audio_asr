package api

import (
	"log"
	"net/http"
	"time"

	"audio-labeler/internal/config"
	"audio-labeler/internal/db"
	"audio-labeler/internal/segment"
	"audio-labeler/internal/service"
)

type Router struct {
	mux      *http.ServeMux
	handlers *Handlers
}

func NewRouter(cfg *config.Config, database *db.DB) *Router {
	// Scanner
	scanner := service.NewScanner(database, cfg.Data.Dir, cfg.Workers.Scan)
	log.Printf("✓ Scanner: %s (workers=%d)", cfg.Data.Dir, cfg.Workers.Scan)

	// Kaldi ASR
	var asrService *service.ASRService
	if cfg.Kaldi.ModelDir != "" {
		var err error
		asrService, err = service.NewASRService(database, cfg.Kaldi.ModelDir, cfg.Workers.ASR)
		if err != nil {
			log.Printf("⚠ Kaldi ASR error: %v", err)
		} else {
			log.Printf("✓ Kaldi ASR: %s (workers=%d)", cfg.Kaldi.ModelDir, cfg.Workers.ASR)
		}
	}

	// Kaldi ASR NoLM (без LM, lm-scale=0)
	var asrNoLMService *service.ASRNoLMService
	if cfg.Kaldi.ModelDir != "" {
		var err error
		asrNoLMService, err = service.NewASRNoLMService(database, cfg.Kaldi.ModelDir, cfg.Workers.ASR)
		if err != nil {
			log.Printf("⚠ Kaldi ASR NoLM error: %v", err)
		} else {
			log.Printf("✓ Kaldi ASR NoLM: %s (lm-scale=0)", cfg.Kaldi.ModelDir)
		}
	}

	// Kaldi ASR GPU
	var asrGPUService *service.ASRGPUService
	if cfg.Kaldi.ModelDir != "" {
		var err error
		asrGPUService, err = service.NewASRGPUService(database, cfg.Kaldi.ModelDir, 32)
		if err != nil {
			log.Printf("⚠ Kaldi ASR GPU error: %v", err)
		} else {
			log.Printf("✓ Kaldi ASR GPU: %s (batch_size=32)", cfg.Kaldi.ModelDir)
		}
	}

	// Kaldi ASR GPU NoLM
	var asrGPUNoLMService *service.ASRGPUNoLMService
	if cfg.Kaldi.ModelDir != "" {
		var err error
		asrGPUNoLMService, err = service.NewASRGPUNoLMService(database, cfg.Kaldi.ModelDir, 32)
		if err != nil {
			log.Printf("⚠ Kaldi ASR GPU NoLM error: %v", err)
		} else {
			log.Printf("✓ Kaldi ASR GPU NoLM: %s (batch_size=32, lm-scale=0)", cfg.Kaldi.ModelDir)
		}
	}

	// Whisper Local
	var whisperLocal *service.WhisperLocalService
	if cfg.Whisper.LocalURL != "" {
		whisperLocal = service.NewWhisperLocalService(database, cfg.Whisper.LocalURL, cfg.Whisper.Lang, 3)
		log.Printf("✓ Whisper Local: %s", cfg.Whisper.LocalURL)
	}

	// Whisper OpenAI
	var whisperOpenAI *service.WhisperOpenAIService
	if cfg.Whisper.OpenAIKey != "" {
		whisperOpenAI = service.NewWhisperOpenAIService(database, cfg.Whisper.OpenAIKey, cfg.Whisper.OpenAIModel, cfg.Whisper.Lang, 3)
		log.Println("✓ Whisper OpenAI configured")
	}

	// Merge Service
	mergeOutputDir := "/data/processed_labeler/merged" // или cfg.Data.Dir + "/merged"
	mergeService := service.NewMergeService(database, mergeOutputDir)
	log.Printf("✓ Merge Service: output to %s", mergeOutputDir)

	r := &Router{
		mux:      http.NewServeMux(),
		handlers: NewHandlers(database, scanner, asrService, asrNoLMService, asrGPUService, asrGPUNoLMService, whisperLocal, whisperOpenAI, mergeService),
	}

	// Pyannote Segment Service
	var segmentHandlers *SegmentHandlers
	pyannoteURL := "http://127.0.0.1:8087"
	segmentRepo := segment.NewRepository(database.DB())
	segmentClient := segment.NewClient(pyannoteURL)

	if err := segmentRepo.CreateTable(); err != nil {
		log.Printf("⚠ Segment table error: %v", err)
	} else {
		segmentHandlers = NewSegmentHandlers(segmentRepo, segmentClient)
		log.Printf("✓ Pyannote Segments: %s", pyannoteURL)
	}

	r.handlers.segmentHandlers = segmentHandlers

	r.setupRoutes()
	return r
}

func (r *Router) setupRoutes() {

	r.mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))
	// Health
	r.mux.HandleFunc("GET /api/health", r.handlers.Health)

	// Stats
	r.mux.HandleFunc("GET /api/stats", r.handlers.Stats)
	r.mux.HandleFunc("GET /api/test/audio-stats", r.handlers.TestAudioStats)
	r.mux.HandleFunc("GET /api/speakers", r.handlers.SpeakersList)

	// Scan
	r.mux.HandleFunc("POST /api/scan/start", r.handlers.ScanStart)
	r.mux.HandleFunc("GET /api/scan/status", r.handlers.ScanStatus)
	r.mux.HandleFunc("POST /api/scan/stop", r.handlers.ScanStop)

	// ASR
	r.mux.HandleFunc("POST /api/asr/start", r.handlers.ASRStart)
	r.mux.HandleFunc("GET /api/asr/status", r.handlers.ASRStatus)
	r.mux.HandleFunc("POST /api/asr/stop", r.handlers.ASRStop)

	// ASR NoLM (Kaldi без LM)
	r.mux.HandleFunc("POST /api/asr-nolm/start", r.handlers.ASRNoLMStart)
	r.mux.HandleFunc("GET /api/asr-nolm/status", r.handlers.ASRNoLMStatus)
	r.mux.HandleFunc("POST /api/asr-nolm/stop", r.handlers.ASRNoLMStop)

	// ASR GPU
	r.mux.HandleFunc("POST /api/asr-gpu/start", r.handlers.ASRGPUStart)
	r.mux.HandleFunc("GET /api/asr-gpu/status", r.handlers.ASRGPUStatus)
	r.mux.HandleFunc("POST /api/asr-gpu/stop", r.handlers.ASRGPUStop)

	// ASR GPU NoLM
	r.mux.HandleFunc("POST /api/asr-gpu-nolm/start", r.handlers.ASRGPUNoLMStart)
	r.mux.HandleFunc("GET /api/asr-gpu-nolm/status", r.handlers.ASRGPUNoLMStatus)
	r.mux.HandleFunc("POST /api/asr-gpu-nolm/stop", r.handlers.ASRGPUNoLMStop)

	// Whisper Local
	r.mux.HandleFunc("POST /api/whisper-local/start", r.handlers.WhisperLocalStart)
	r.mux.HandleFunc("GET /api/whisper-local/status", r.handlers.WhisperLocalStatus)
	r.mux.HandleFunc("POST /api/whisper-local/stop", r.handlers.WhisperLocalStop)

	// Whisper OpenAI
	r.mux.HandleFunc("POST /api/whisper-openai/start", r.handlers.WhisperOpenAIStart)
	r.mux.HandleFunc("POST /api/whisper-openai/start-forced", r.handlers.WhisperOpenAIStartForced)
	r.mux.HandleFunc("GET /api/whisper-openai/status", r.handlers.WhisperOpenAIStatus)
	r.mux.HandleFunc("POST /api/whisper-openai/stop", r.handlers.WhisperOpenAIStop)

	// Data files
	r.mux.HandleFunc("GET /api/files", r.handlers.FilesList)
	r.mux.HandleFunc("GET /api/files/{id}", r.handlers.FilesGet)
	r.mux.HandleFunc("GET /api/audio/{id}", r.handlers.ServeAudio)

	// Edit transcription (редактирование оригинала)
	r.mux.HandleFunc("PUT /api/files/{id}/transcription", r.handlers.UpdateTranscription)

	// Trim audio (редактирование оригинала)
	r.mux.HandleFunc("POST /api/files/{id}/trim", r.handlers.TrimAudio)

	// Verification (верификация оператором)
	r.mux.HandleFunc("POST /api/files/{id}/verify", r.handlers.VerifyFile)
	r.mux.HandleFunc("POST /api/files/{id}/unverify", r.handlers.UnverifyFile)

	// Silence
	r.mux.HandleFunc("GET /api/files/{id}/silence", r.handlers.CheckSilence)
	r.mux.HandleFunc("POST /api/files/{id}/add-silence", r.handlers.AddSilence)
	r.mux.HandleFunc("POST /api/files/{id}/remove-silence", r.handlers.RemoveSilence)
	r.mux.HandleFunc("POST /api/files/{id}/analyze", r.handlers.AnalyzeFile)

	// Process single file
	r.mux.HandleFunc("POST /api/process/{id}", r.handlers.ProcessFile)
	r.mux.HandleFunc("DELETE /api/files/{id}", r.handlers.DeleteFile)

	// Recalc
	r.mux.HandleFunc("POST /api/recalc/{id}", r.handlers.RecalcWER)
	r.mux.HandleFunc("POST /api/recalc-all", r.handlers.RecalcAll)

	// Merge & Merge Queue
	r.mux.HandleFunc("POST /api/merge", r.handlers.MergeFiles)
	r.mux.HandleFunc("GET /api/short-files", r.handlers.GetShortFiles)
	r.mux.HandleFunc("POST /api/merge/queue", r.handlers.AddToMergeQueue)
	r.mux.HandleFunc("POST /api/merge/now", r.handlers.MergeFromString)
	r.mux.HandleFunc("POST /api/merge/queue/start", r.handlers.ProcessMergeQueue)
	r.mux.HandleFunc("GET /api/merge/queue/status", r.handlers.MergeQueueStatus)
	r.mux.HandleFunc("POST /api/merge/queue/stop", r.handlers.StopMergeQueue)
	r.mux.HandleFunc("GET /api/merge/queue", r.handlers.ListMergeQueue)
	r.mux.HandleFunc("POST /api/merge/queue/batch", r.handlers.AddBatchToMergeQueue)

	r.mux.HandleFunc("POST /api/analyze/start", r.handlers.AnalyzeStart)
	r.mux.HandleFunc("GET /api/analyze/status", r.handlers.AnalyzeStatus)

	// Segments (Pyannote)
	if r.handlers.segmentHandlers != nil {
		sh := r.handlers.segmentHandlers
		r.mux.HandleFunc("POST /api/files/{id}/diarize", func(w http.ResponseWriter, req *http.Request) {
			sh.DiarizeFile(w, req, r.handlers)
		})
		r.mux.HandleFunc("GET /api/files/{id}/segments", func(w http.ResponseWriter, req *http.Request) {
			sh.GetSegments(w, req, r.handlers)
		})
		r.mux.HandleFunc("PUT /api/files/{id}/segments/select", func(w http.ResponseWriter, req *http.Request) {
			sh.UpdateSegmentSelection(w, req, r.handlers)
		})
		r.mux.HandleFunc("PUT /api/files/{id}/segments/transcripts", func(w http.ResponseWriter, req *http.Request) {
			sh.UpdateSegmentTranscripts(w, req, r.handlers)
		})
		r.mux.HandleFunc("POST /api/files/{id}/segments/apply", func(w http.ResponseWriter, req *http.Request) {
			sh.ApplySegmentTranscripts(w, req, r.handlers)
		})
		r.mux.HandleFunc("GET /api/files/{id}/segments/check", func(w http.ResponseWriter, req *http.Request) {
			sh.CheckSegments(w, req, r.handlers)
		})
		r.mux.HandleFunc("GET /api/pyannote/health", func(w http.ResponseWriter, req *http.Request) {
			sh.PyannoteHealth(w, req, r.handlers)
		})
		r.mux.HandleFunc("POST /api/files/{id}/segments/export", func(w http.ResponseWriter, req *http.Request) {
			sh.ExportSegments(w, req, r.handlers)
		})
	}

	// Other
	r.mux.HandleFunc("GET /", r.handlers.ServeWeb)
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if req.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Логируем запрос
	log.Printf("→ %s %s %s", req.Method, req.URL.Path, req.URL.RawQuery)

	r.mux.ServeHTTP(w, req)

	// Логируем время выполнения
	log.Printf("← %s %s [%v]", req.Method, req.URL.Path, time.Since(start))
}
