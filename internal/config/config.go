package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Data     DataConfig
	Kaldi    KaldiConfig
	Whisper  WhisperConfig
	Workers  WorkersConfig
}

type ServerConfig struct {
	Addr string
}

type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Name     string
}

type DataConfig struct {
	Dir string
}

type KaldiConfig struct {
	ModelDir string
	Host     string
	Key      string
}

type WhisperConfig struct {
	LocalURL    string
	Lang        string
	OpenAIKey   string
	OpenAIModel string
}

type WorkersConfig struct {
	Scan int
	ASR  int
}

func Load(envFile string) (*Config, error) {
	godotenv.Load(envFile)

	return &Config{
		Server: ServerConfig{
			Addr: getEnv("SERVER_ADDR", ":8082"),
		},
		Database: DatabaseConfig{
			Host:     getEnv("DB_HOST", "127.0.0.1"),
			Port:     getEnvInt("DB_PORT", 53306),
			User:     getEnv("DB_USER", "root"),
			Password: getEnv("DB_PASSWORD", ""),
			Name:     getEnv("DB_NAME", "label1"),
		},
		Data: DataConfig{
			Dir: getEnv("DATA_DIR", ""),
		},
		Kaldi: KaldiConfig{
			ModelDir: getEnv("KALDI_MODEL_DIR", ""),
			Host:     getEnv("ASR_HOST", ""),
			Key:      getEnv("ASR_KEY", ""),
		},
		Whisper: WhisperConfig{
			LocalURL:    getEnv("WHISPER_LOCAL_URL", ""),
			Lang:        getEnv("WHISPER_LOCAL_LANG", "az"),
			OpenAIKey:   getEnv("WHISPER_OPENAI_KEY", ""),
			OpenAIModel: getEnv("WHISPER_OPENAI_MODEL", "whisper-1"),
		},
		Workers: WorkersConfig{
			Scan: getEnvInt("SCAN_WORKERS", 10),
			ASR:  getEnvInt("ASR_WORKERS", 5),
		},
	}, nil
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}
