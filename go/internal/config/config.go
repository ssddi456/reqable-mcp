package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

const (
	DefaultIngestHost        = "127.0.0.1"
	DefaultIngestPort        = 18765
	DefaultIngestPath        = "/report"
	DefaultWSEventsPath      = "/ws/events"
	DefaultMCPHost           = "127.0.0.1"
	DefaultMCPPort           = 18766
	DefaultMaxBodySize       = 1024 * 100
	DefaultMaxReportSize     = 10 * 1024 * 1024
	DefaultMaxImportFileSize = 100 * 1024 * 1024
	DefaultRetentionDays     = 7
)

var reservedIngestPaths = map[string]struct{}{
	"/health": {},
}

type Config struct {
	DataDir                  string
	DBPath                   string
	IngestHost               string
	IngestPort               int
	IngestPath               string
	WSEventsPath             string
	IngestToken              string
	MCPHost                  string
	MCPPort                  int
	MaxBodySize              int
	MaxReportSize            int
	MaxImportFileSize        int64
	RetentionDays            int
	DefaultListLimit         int
	KeyBodyPreviewLength     int
	SummaryBodyPreviewLength int
}

func readIntEnv(key string, defaultValue, minimum int, maximum *int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("Ignoring invalid %s=%q, fallback=%d", key, raw, defaultValue)
		return defaultValue
	}
	if value < minimum || (maximum != nil && value > *maximum) {
		log.Printf("Ignoring out-of-range %s=%q, fallback=%d", key, raw, defaultValue)
		return defaultValue
	}
	return value
}

func readPathEnv(key string) string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "~/") || raw == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(raw, "~/"))
		}
	}
	return raw
}

func normalizePath(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	if !strings.HasPrefix(value, "/") {
		return "/" + value
	}
	return value
}

func GetDataDir() string {
	if env := readPathEnv("REQABLE_DATA_DIR"); env != "" {
		return env
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "reqable-mcp"
	}
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "reqable-mcp")
	case "windows":
		base := strings.TrimSpace(os.Getenv("APPDATA"))
		if base == "" {
			base = filepath.Join(home, "AppData", "Roaming")
		}
		return filepath.Join(base, "reqable-mcp")
	default:
		return filepath.Join(home, ".local", "share", "reqable-mcp")
	}
}

func GetDBPath() string {
	if env := readPathEnv("REQABLE_DB_PATH"); env != "" {
		return env
	}
	return filepath.Join(GetDataDir(), "requests.db")
}

func Load() (Config, error) {
	ingestPath := normalizePath(os.Getenv("REQABLE_INGEST_PATH"), DefaultIngestPath)
	wsEventsPath := normalizePath(os.Getenv("REQABLE_WS_EVENTS_PATH"), DefaultWSEventsPath)
	if ingestPath == wsEventsPath {
		return Config{}, fmt.Errorf("REQABLE_INGEST_PATH and REQABLE_WS_EVENTS_PATH must be different paths")
	}
	if _, ok := reservedIngestPaths[ingestPath]; ok {
		return Config{}, fmt.Errorf("REQABLE_INGEST_PATH cannot use reserved paths: /health")
	}
	if _, ok := reservedIngestPaths[wsEventsPath]; ok {
		return Config{}, fmt.Errorf("REQABLE_WS_EVENTS_PATH cannot use reserved paths: /health")
	}
	maxPort := 65535
	maxBody := 10 * 1024 * 1024
	maxReport := 100 * 1024 * 1024
	maxImport := 1024 * 1024 * 1024
	maxRetention := 3650
	cfg := Config{
		DataDir:                  GetDataDir(),
		DBPath:                   GetDBPath(),
		IngestHost:               strings.TrimSpace(os.Getenv("REQABLE_INGEST_HOST")),
		IngestPort:               readIntEnv("REQABLE_INGEST_PORT", DefaultIngestPort, 1, &maxPort),
		IngestPath:               ingestPath,
		WSEventsPath:             wsEventsPath,
		IngestToken:              strings.TrimSpace(os.Getenv("REQABLE_INGEST_TOKEN")),
		MCPHost:                  strings.TrimSpace(os.Getenv("REQABLE_MCP_HOST")),
		MCPPort:                  readIntEnv("REQABLE_MCP_PORT", DefaultMCPPort, 1, &maxPort),
		MaxBodySize:              readIntEnv("REQABLE_MAX_BODY_SIZE", DefaultMaxBodySize, 1024, &maxBody),
		MaxReportSize:            readIntEnv("REQABLE_MAX_REPORT_SIZE", DefaultMaxReportSize, 1024, &maxReport),
		MaxImportFileSize:        int64(readIntEnv("REQABLE_MAX_IMPORT_FILE_SIZE", DefaultMaxImportFileSize, 1024, &maxImport)),
		RetentionDays:            readIntEnv("REQABLE_RETENTION_DAYS", DefaultRetentionDays, 1, &maxRetention),
		DefaultListLimit:         20,
		KeyBodyPreviewLength:     500,
		SummaryBodyPreviewLength: 200,
	}
	if cfg.IngestHost == "" {
		cfg.IngestHost = DefaultIngestHost
	}
	if cfg.MCPHost == "" {
		cfg.MCPHost = DefaultMCPHost
	}
	return cfg, nil
}

func (c Config) IngestURL() string {
	return fmt.Sprintf("http://%s:%d%s", c.IngestHost, c.IngestPort, c.IngestPath)
}

func (c Config) WSEventsURL() string {
	return fmt.Sprintf("http://%s:%d%s", c.IngestHost, c.IngestPort, c.WSEventsPath)
}

func (c Config) MCPBaseURL() string {
	return fmt.Sprintf("http://%s:%d", c.MCPHost, c.MCPPort)
}

func (c Config) MCPListenAddr() string {
	return fmt.Sprintf("%s:%d", c.MCPHost, c.MCPPort)
}

func (c Config) IngestListenAddr() string {
	return fmt.Sprintf("%s:%d", c.IngestHost, c.IngestPort)
}
