package main

import (
	"flag"
	"log"
	"net/http"

	"audio-labeler/internal/api"
	"audio-labeler/internal/config"
	"audio-labeler/internal/db"
)

func main() {
	envFile := flag.String("env", ".env", "path to .env file")
	flag.Parse()

	// Load config
	cfg, err := config.Load(*envFile)
	if err != nil {
		log.Fatalf("Config error: %v", err)
	}

	// Database
	database, err := db.New(
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Name,
	)
	if err != nil {
		log.Fatalf("DB error: %v", err)
	}
	defer database.Close()
	log.Println("✓ Connected to MariaDB")

	// Router (создаёт все сервисы внутри)
	router := api.NewRouter(cfg, database)

	// Start
	log.Printf("✓ Server starting on %s", cfg.Server.Addr)
	log.Printf("  Data dir: %s", cfg.Data.Dir)
	printEndpoints()

	if err := http.ListenAndServe(cfg.Server.Addr, router); err != nil {
		log.Fatal(err)
	}
}

func printEndpoints() {
	log.Println("\nEndpoints:")
	log.Println("  POST /api/scan/start")
	log.Println("  POST /api/asr/start")
	log.Println("  POST /api/whisper-local/start")
	log.Println("  POST /api/whisper-openai/start")
	log.Println("  POST /api/whisper-openai/start-forced")
	log.Println("  GET  /api/stats")
	log.Println("  GET  /api/files")
	log.Println("  GET  /")
}
