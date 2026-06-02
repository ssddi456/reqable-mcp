package ingest

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ssddi456/reqable-mcp/internal/config"
	"github.com/ssddi456/reqable-mcp/internal/storage"
)

type Manager struct {
	cfg              config.Config
	storage          *storage.Storage
	server           *http.Server
	mux              *http.ServeMux
	mu               sync.Mutex
	startedAt        *time.Time
	acceptedPayloads int
	failedPayloads   int
	lastError        *string
}

func NewManager(cfg config.Config, store *storage.Storage) *Manager {
	m := &Manager{cfg: cfg, storage: store, mux: http.NewServeMux()}
	m.mux.HandleFunc("/health", m.handleHealth)
	m.mux.HandleFunc(cfg.IngestPath, m.handleReport)
	m.mux.HandleFunc(cfg.WSEventsPath, m.handleWSEvents)
	m.server = &http.Server{Addr: cfg.IngestListenAddr(), Handler: m.mux}
	return m
}

func (m *Manager) Start() error {
	m.mu.Lock()
	if m.startedAt != nil {
		m.mu.Unlock()
		return nil
	}
	now := time.Now().UTC()
	m.startedAt = &now
	m.mu.Unlock()

	m.storage.AddEvent("info", "Ingest server started", map[string]any{"host": m.cfg.IngestHost, "port": m.cfg.IngestPort})
	err := m.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		m.setLastError(err.Error())
		m.storage.AddEvent("error", "Ingest server failed", map[string]any{"error": err.Error()})
		return err
	}
	return nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.storage.AddEvent("info", "Ingest server stopped", nil)
	return m.server.Shutdown(ctx)
}

func (m *Manager) setLastError(message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if strings.TrimSpace(message) == "" {
		m.lastError = nil
		return
	}
	copy := message
	m.lastError = &copy
}

func (m *Manager) bumpAccepted() {
	m.mu.Lock()
	m.acceptedPayloads++
	m.lastError = nil
	m.mu.Unlock()
}

func (m *Manager) bumpFailed(message string) {
	m.mu.Lock()
	m.failedPayloads++
	if strings.TrimSpace(message) != "" {
		copy := message
		m.lastError = &copy
	}
	m.mu.Unlock()
}

func (m *Manager) decodeBody(raw []byte, contentEncoding string) ([]byte, error) {
	data := raw
	encodings := []string{}
	for _, item := range strings.Split(contentEncoding, ",") {
		item = strings.TrimSpace(strings.ToLower(item))
		if item != "" && item != "identity" {
			encodings = append(encodings, item)
		}
	}
	for i := len(encodings) - 1; i >= 0; i-- {
		encoding := encodings[i]
		switch encoding {
		case "gzip", "x-gzip":
			reader, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			decoded, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				return nil, err
			}
			data = decoded
		case "deflate":
			reader, err := zlib.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			decoded, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				return nil, err
			}
			data = decoded
		default:
			return nil, fmt.Errorf("unsupported content-encoding: %s", encoding)
		}
	}
	return data, nil
}

func (m *Manager) requireToken(w http.ResponseWriter, r *http.Request) bool {
	if m.cfg.IngestToken == "" {
		return true
	}
	token := r.Header.Get("X-Reqable-Token")
	if subtle.ConstantTimeCompare([]byte(token), []byte(m.cfg.IngestToken)) == 1 {
		return true
	}
	m.bumpFailed("forbidden")
	writeJSON(w, http.StatusForbidden, map[string]any{"ok": false, "error": "forbidden"})
	return false
}

func (m *Manager) parsePayload(w http.ResponseWriter, r *http.Request) (any, bool) {
	if !m.requireToken(w, r) {
		return nil, false
	}
	if r.ContentLength < 0 {
		m.bumpFailed("content_length_required")
		writeJSON(w, http.StatusLengthRequired, map[string]any{"ok": false, "error": "content_length_required"})
		return nil, false
	}
	if r.ContentLength > int64(m.cfg.MaxReportSize) {
		m.bumpFailed("payload_too_large")
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"ok": false, "error": "payload_too_large", "max_report_size": m.cfg.MaxReportSize})
		return nil, false
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, int64(m.cfg.MaxReportSize)+1))
	if err != nil {
		m.bumpFailed(err.Error())
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid_payload", "detail": err.Error()})
		return nil, false
	}
	if len(raw) > m.cfg.MaxReportSize {
		m.bumpFailed("payload_too_large")
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"ok": false, "error": "payload_too_large", "max_report_size": m.cfg.MaxReportSize})
		return nil, false
	}
	decoded, err := m.decodeBody(raw, r.Header.Get("Content-Encoding"))
	if err != nil {
		m.bumpFailed(err.Error())
		m.storage.AddEvent("error", "Decode report payload failed", map[string]any{"error": err.Error()})
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid_payload", "detail": err.Error()})
		return nil, false
	}
	if len(decoded) > m.cfg.MaxReportSize {
		m.bumpFailed("payload_too_large_after_decode")
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"ok": false, "error": "payload_too_large_after_decode", "max_report_size": m.cfg.MaxReportSize})
		return nil, false
	}
	var payload any
	if err := json.Unmarshal(decoded, &payload); err != nil {
		m.bumpFailed(err.Error())
		m.storage.AddEvent("error", "Decode report payload failed", map[string]any{"error": err.Error()})
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid_payload", "detail": err.Error()})
		return nil, false
	}
	return payload, true
}

func (m *Manager) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "method_not_allowed"})
		return
	}
	writeJSON(w, http.StatusOK, m.PublicHealthStatus())
}

func (m *Manager) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "method_not_allowed"})
		return
	}
	payload, ok := m.parsePayload(w, r)
	if !ok {
		return
	}
	platform := nullableHeader(r.Header.Get("X-Reqable-Platform"))
	reporterHost := nullableHeader(r.Header.Get("X-Reporter-Host"))
	result := m.storage.IngestPayload(payload, "report_server", platform, reporterHost)
	m.bumpAccepted()
	writeJSON(w, http.StatusOK, mergeMaps(map[string]any{"ok": true, "ingest_mode": "report"}, result))
}

func (m *Manager) handleWSEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"ok": false, "error": "method_not_allowed"})
		return
	}
	payload, ok := m.parsePayload(w, r)
	if !ok {
		return
	}
	platform := nullableHeader(r.Header.Get("X-Reqable-Platform"))
	reporterHost := nullableHeader(r.Header.Get("X-Reporter-Host"))
	result := m.storage.IngestWebSocketEvents(payload, "report_server_ws_events", platform, reporterHost)
	m.bumpAccepted()
	writeJSON(w, http.StatusOK, mergeMaps(map[string]any{"ok": true, "ingest_mode": "ws_events"}, result))
}

func mergeMaps(base map[string]any, extra map[string]any) map[string]any {
	result := map[string]any{}
	for k, v := range base {
		result[k] = v
	}
	for k, v := range extra {
		result[k] = v
	}
	return result
}

func nullableHeader(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		log.Printf("json encode error: %v", err)
		body = []byte(`{"ok":false,"error":"encode_failed"}`)
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (m *Manager) Status(includeEvents bool) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	listening := m.startedAt != nil
	var startedAt any
	if m.startedAt != nil {
		startedAt = m.startedAt.Format(time.RFC3339)
	}
	var lastError any
	if m.lastError != nil {
		lastError = *m.lastError
	}
	result := map[string]any{
		"ok":                                    true,
		"mode":                                  "local",
		"listening":                             listening,
		"ingest_transport":                      "http",
		"supports_raw_websocket_listener":       false,
		"supports_websocket_capture":            true,
		"supports_incremental_websocket_events": true,
		"ingest_url":                            m.cfg.IngestURL(),
		"ws_events_url":                         m.cfg.WSEventsURL(),
		"host":                                  m.cfg.IngestHost,
		"port":                                  m.cfg.IngestPort,
		"path":                                  m.cfg.IngestPath,
		"ws_events_path":                        m.cfg.WSEventsPath,
		"started_at":                            startedAt,
		"accepted_payloads":                     m.acceptedPayloads,
		"failed_payloads":                       m.failedPayloads,
		"last_error":                            lastError,
		"db_path":                               m.storage.DBPath(),
		"total_requests":                        m.storage.TotalRequests(),
		"total_websocket_sessions":              m.storage.TotalWebSocketSessions(),
		"total_websocket_messages":              m.storage.TotalWebSocketMessages(),
	}
	if includeEvents {
		result["recent_events"] = m.storage.RecentEvents(10)
	}
	return result
}

func (m *Manager) PublicHealthStatus() map[string]any {
	status := m.Status(false)
	delete(status, "db_path")
	delete(status, "last_error")
	return status
}
