package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	_ "modernc.org/sqlite"

	"github.com/ssddi456/reqable-mcp/internal/normalizer"
)

type DetailLevel string

const (
	DetailSummary DetailLevel = "summary"
	DetailKey     DetailLevel = "key"
	DetailFull    DetailLevel = "full"

	// maxWSQueryLimit is the hard upper bound on WebSocket-related sub-queries.
	maxWSQueryLimit = 5000
)

var (
	validSearchAreas  = map[string]struct{}{"all": {}, "url": {}, "request_body": {}, "response_body": {}, "raw_entry": {}, "raw": {}}
	validWSDirections = map[string]struct{}{"inbound": {}, "outbound": {}, "unknown": {}}
)

type WebSocketMessage struct {
	Seq           int            `json:"seq"`
	Direction     string         `json:"direction"`
	Timestamp     *string        `json:"timestamp,omitempty"`
	Opcode        *int           `json:"opcode,omitempty"`
	MessageType   *string        `json:"message_type,omitempty"`
	Data          *string        `json:"data,omitempty"`
	DataJSON      any            `json:"data_json,omitempty"`
	IsBinary      bool           `json:"is_binary"`
	Encoding      *string        `json:"encoding,omitempty"`
	BodyTruncated bool           `json:"body_truncated"`
	CloseCode     *int           `json:"close_code,omitempty"`
	CloseReason   *string        `json:"close_reason,omitempty"`
	Raw           map[string]any `json:"raw,omitempty"`
}

type RequestSummary struct {
	ID                    string  `json:"id"`
	Method                string  `json:"method"`
	URL                   string  `json:"url"`
	Host                  *string `json:"host,omitempty"`
	Path                  *string `json:"path,omitempty"`
	Status                *int    `json:"status,omitempty"`
	DurationMS            *int    `json:"duration_ms,omitempty"`
	Timestamp             *string `json:"timestamp,omitempty"`
	IsWebSocket           bool    `json:"is_websocket"`
	WebSocketMessageCount int     `json:"websocket_message_count"`
}

type RequestKey struct {
	ID                    string            `json:"id"`
	Method                string            `json:"method"`
	URL                   string            `json:"url"`
	Host                  *string           `json:"host,omitempty"`
	Path                  *string           `json:"path,omitempty"`
	Status                *int              `json:"status,omitempty"`
	DurationMS            *int              `json:"duration_ms,omitempty"`
	Timestamp             *string           `json:"timestamp,omitempty"`
	QueryParams           map[string]string `json:"query_params"`
	ContentType           *string           `json:"content_type,omitempty"`
	RequestBodyPreview    *string           `json:"request_body_preview,omitempty"`
	RequestBodyStructure  any               `json:"request_body_structure,omitempty"`
	ResponseBodyPreview   *string           `json:"response_body_preview,omitempty"`
	ResponseBodyStructure any               `json:"response_body_structure,omitempty"`
	HasAuth               bool              `json:"has_auth"`
	IsJSON                bool              `json:"is_json"`
	BodyTruncated         bool              `json:"body_truncated"`
	IsWebSocket           bool              `json:"is_websocket"`
	WebSocketMessageCount int               `json:"websocket_message_count"`
}

type RequestFull struct {
	ID                    string              `json:"id"`
	Method                string              `json:"method"`
	URL                   string              `json:"url"`
	Protocol              string              `json:"protocol"`
	Host                  *string             `json:"host,omitempty"`
	Port                  *int                `json:"port,omitempty"`
	Path                  *string             `json:"path,omitempty"`
	QueryString           *string             `json:"query_string,omitempty"`
	QueryParams           map[string]string   `json:"query_params"`
	Status                *int                `json:"status,omitempty"`
	StatusText            *string             `json:"status_text,omitempty"`
	DurationMS            *int                `json:"duration_ms,omitempty"`
	Timestamp             *string             `json:"timestamp,omitempty"`
	RequestHeaders        map[string][]string `json:"request_headers"`
	ResponseHeaders       map[string][]string `json:"response_headers"`
	RequestBody           *string             `json:"request_body,omitempty"`
	RequestBodyJSON       any                 `json:"request_body_json,omitempty"`
	ResponseBody          *string             `json:"response_body,omitempty"`
	ResponseBodyJSON      any                 `json:"response_body_json,omitempty"`
	RemoteIP              *string             `json:"remote_ip,omitempty"`
	IsHTTPS               bool                `json:"is_https"`
	BodyTruncated         bool                `json:"body_truncated"`
	Source                *string             `json:"source,omitempty"`
	Platform              *string             `json:"platform,omitempty"`
	ReporterHost          *string             `json:"reporter_host,omitempty"`
	IsWebSocket           bool                `json:"is_websocket"`
	WebSocketMessageCount int                 `json:"websocket_message_count"`
	WebSocketMessages     []WebSocketMessage  `json:"websocket_messages,omitempty"`
	RawEntry              map[string]any      `json:"raw_entry,omitempty"`
}

func (r RequestFull) ToCurl() string {
	parts := []string{fmt.Sprintf("curl -X %s", shellQuote(strings.ToUpper(r.Method))), shellQuote(r.URL)}
	for name, values := range r.RequestHeaders {
		lower := strings.ToLower(name)
		if lower == "host" || lower == "content-length" {
			continue
		}
		for _, value := range values {
			parts = append(parts, fmt.Sprintf("-H %s", shellQuote(fmt.Sprintf("%s: %s", name, value))))
		}
	}
	if r.RequestBody != nil && *r.RequestBody != "" {
		parts = append(parts, fmt.Sprintf("--data-raw %s", shellQuote(*r.RequestBody)))
	}
	return strings.Join(parts, " \\\n  ")
}

func shellQuote(text string) string {
	return "'" + strings.ReplaceAll(text, "'", "'\"'\"'") + "'"
}

type Storage struct {
	db                       *sql.DB
	dbPath                   string
	maxBodySize              int
	summaryBodyPreviewLength int
	keyBodyPreviewLength     int
	retentionDays            int
	ingestCalls              atomic.Int64
}

func New(dbPath string, maxBodySize, summaryBodyPreviewLength, keyBodyPreviewLength, retentionDays int) (*Storage, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	s := &Storage{
		db:                       db,
		dbPath:                   dbPath,
		maxBodySize:              maxBodySize,
		summaryBodyPreviewLength: summaryBodyPreviewLength,
		keyBodyPreviewLength:     keyBodyPreviewLength,
		retentionDays:            retentionDays,
	}
	if err := s.initDB(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	_, _ = s.PruneRetention()
	return s, nil
}

func (s *Storage) Close() error   { return s.db.Close() }
func (s *Storage) DBPath() string { return s.dbPath }

func (s *Storage) initDB(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA synchronous=NORMAL;`,
		`CREATE TABLE IF NOT EXISTS requests (
            id TEXT PRIMARY KEY,
            method TEXT NOT NULL,
            url TEXT NOT NULL,
            host TEXT,
            path TEXT,
            query_string TEXT,
            query_params TEXT NOT NULL DEFAULT '{}',
            status INTEGER,
            status_text TEXT,
            duration_ms INTEGER,
            timestamp TEXT,
            request_headers TEXT NOT NULL DEFAULT '{}',
            response_headers TEXT NOT NULL DEFAULT '{}',
            request_body TEXT,
            response_body TEXT,
            request_body_json TEXT,
            response_body_json TEXT,
            content_type TEXT,
            has_auth INTEGER NOT NULL DEFAULT 0,
            is_https INTEGER NOT NULL DEFAULT 0,
            body_truncated INTEGER NOT NULL DEFAULT 0,
            remote_ip TEXT,
            source TEXT,
            platform TEXT,
            reporter_host TEXT,
            raw_entry_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );`,
		`CREATE INDEX IF NOT EXISTS idx_requests_created_at ON requests(created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_requests_created_at_id ON requests(created_at DESC, id DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_requests_host ON requests(host);`,
		`CREATE INDEX IF NOT EXISTS idx_requests_method ON requests(method);`,
		`CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status);`,
		`CREATE TABLE IF NOT EXISTS websocket_messages (
            request_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            direction TEXT NOT NULL,
            timestamp TEXT,
            opcode INTEGER,
            message_type TEXT,
            data TEXT,
            data_json TEXT,
            is_binary INTEGER NOT NULL DEFAULT 0,
            encoding TEXT,
            body_truncated INTEGER NOT NULL DEFAULT 0,
            raw_message_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (request_id, seq)
        );`,
		`CREATE INDEX IF NOT EXISTS idx_websocket_messages_request_id ON websocket_messages(request_id, seq);`,
		`CREATE INDEX IF NOT EXISTS idx_websocket_messages_direction ON websocket_messages(direction);`,
		`CREATE INDEX IF NOT EXISTS idx_websocket_messages_created_at ON websocket_messages(created_at DESC, request_id, seq);`,
		`CREATE TABLE IF NOT EXISTS ingest_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );`,
	}
	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if err := s.ensureColumn(ctx, "requests", "raw_entry_json", "TEXT"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "websocket_messages", "raw_message_json", "TEXT"); err != nil {
		return err
	}
	return nil
}

func (s *Storage) ensureColumn(ctx context.Context, table, column, definition string) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition))
	return err
}

func safeInt(value any) *int {
	switch v := value.(type) {
	case nil:
		return nil
	case int:
		x := v
		return &x
	case int64:
		x := int(v)
		return &x
	case int32:
		x := int(v)
		return &x
	case float64:
		x := int(v)
		return &x
	case json.Number:
		if i, err := v.Int64(); err == nil {
			x := int(i)
			return &x
		}
		if f, err := v.Float64(); err == nil {
			x := int(f)
			return &x
		}
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		if i, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			x := i
			return &x
		}
	}
	return nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func jsonDumps(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(data)
}

func jsonLoads(value string, defaultValue any) any {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	var result any
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		return defaultValue
	}
	return result
}

func jsonLoadsMapStringList(value string) map[string][]string {
	result := map[string][]string{}
	if strings.TrimSpace(value) == "" {
		return result
	}
	_ = json.Unmarshal([]byte(value), &result)
	return result
}

func jsonLoadsMapString(value string) map[string]string {
	result := map[string]string{}
	if strings.TrimSpace(value) == "" {
		return result
	}
	_ = json.Unmarshal([]byte(value), &result)
	return result
}

func anyString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case *string:
		if v == nil {
			return ""
		}
		return *v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func anyNullableString(value any) *string {
	text := strings.TrimSpace(anyString(value))
	if text == "" {
		return nil
	}
	return &text
}

func anyBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	case string:
		if v == "" {
			return false
		}
		if i, err := strconv.Atoi(v); err == nil {
			return i != 0
		}
		b, _ := strconv.ParseBool(v)
		return b
	default:
		return false
	}
}

func eventID(value any) string { return strings.TrimSpace(anyString(value)) }

func eventType(event map[string]any) string {
	return strings.ToLower(strings.TrimSpace(anyString(firstPresent(event, "event_type", "type"))))
}

func extractWSEvents(payload any) []map[string]any {
	switch v := payload.(type) {
	case map[string]any:
		if events, ok := v["events"].([]any); ok {
			return filterMaps(events)
		}
		if event, ok := v["event"].(map[string]any); ok {
			return []map[string]any{event}
		}
		for _, key := range []string{"session_id", "request_id", "event_type", "type"} {
			if _, ok := v[key]; ok {
				return []map[string]any{v}
			}
		}
	case []any:
		return filterMaps(v)
	}
	return nil
}

func wsEventDefaults(payload any) map[string]any {
	src, ok := payload.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	defaults := map[string]any{}
	for _, key := range []string{"session_id", "request_id", "id", "source", "platform", "reporter_host", "url", "method", "status", "status_text", "session_started_at"} {
		if value, ok := src[key]; ok {
			defaults[key] = value
		}
	}
	for _, key := range []string{"session", "request", "response", "raw_session"} {
		if value, ok := src[key].(map[string]any); ok {
			defaults[key] = value
		}
	}
	return defaults
}

func mergeEventWithDefaults(event, defaults map[string]any) map[string]any {
	if len(defaults) == 0 {
		return event
	}
	merged := map[string]any{}
	for key, value := range defaults {
		merged[key] = value
	}
	for key, value := range event {
		merged[key] = value
	}
	for _, key := range []string{"session", "request", "response", "raw_session"} {
		base, baseOK := defaults[key].(map[string]any)
		current, currentOK := event[key].(map[string]any)
		if baseOK && currentOK {
			nested := map[string]any{}
			for k, v := range base {
				nested[k] = v
			}
			for k, v := range current {
				nested[k] = v
			}
			merged[key] = nested
		}
	}
	return merged
}

func normalizeHeaders(value any) map[string][]string {
	headers := map[string][]string{}
	switch v := value.(type) {
	case map[string]any:
		for key, raw := range v {
			name := strings.TrimSpace(key)
			if name == "" {
				continue
			}
			switch item := raw.(type) {
			case []any:
				vals := make([]string, 0, len(item))
				for _, child := range item {
					if child != nil {
						vals = append(vals, anyString(child))
					}
				}
				headers[name] = vals
			case nil:
				headers[name] = []string{""}
			default:
				headers[name] = []string{anyString(raw)}
			}
		}
	case []any:
		for _, item := range v {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			name := strings.TrimSpace(anyString(row["name"]))
			if name == "" {
				continue
			}
			headers[name] = append(headers[name], anyString(row["value"]))
		}
	}
	return headers
}

func queryParamsFromURL(rawURL string) (*string, map[string]string) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, map[string]string{}
	}
	var queryString *string
	if parsed.RawQuery != "" {
		queryString = &parsed.RawQuery
	}
	queryParams := map[string]string{}
	for key, values := range parsed.Query() {
		if len(values) == 1 {
			queryParams[key] = values[0]
		} else {
			queryParams[key] = jsonDumps(values)
		}
	}
	return queryString, queryParams
}

func filterMaps(items []any) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if row, ok := item.(map[string]any); ok {
			result = append(result, row)
		}
	}
	return result
}

func firstPresent(m map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			return value
		}
	}
	return nil
}

func extractJSONStructure(data any, maxDepth, currentDepth int) any {
	if currentDepth >= maxDepth {
		return "..."
	}
	switch v := data.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "int"
	case float32, float64:
		return "float"
	case string:
		return "string"
	case []any:
		if len(v) == 0 {
			return []any{}
		}
		return []any{extractJSONStructure(v[0], maxDepth, currentDepth+1)}
	case map[string]any:
		result := map[string]any{}
		count := 0
		for key, value := range v {
			result[key] = extractJSONStructure(value, maxDepth, currentDepth+1)
			count++
			if count >= 10 {
				break
			}
		}
		return result
	default:
		return fmt.Sprintf("%T", data)
	}
}

func previewPtr(text *string, maxLen int) *string {
	if text == nil || *text == "" {
		return nil
	}
	preview := *text
	if len(preview) > maxLen {
		preview = preview[:maxLen]
	}
	return &preview
}

func wsCloseDetails(message WebSocketMessage) (*int, *string) {
	candidates := []any{}
	if message.Raw != nil {
		candidates = append(candidates, message.Raw["payload"], message.Raw)
	}
	candidates = append(candidates, message.DataJSON)
	for _, candidate := range candidates {
		row, ok := candidate.(map[string]any)
		if !ok {
			continue
		}
		code := safeInt(row["code"])
		var reason *string
		if value, ok := row["reason"]; ok && value != nil {
			text := strings.TrimSpace(anyString(value))
			if text != "" {
				reason = &text
			}
		}
		if code != nil || reason != nil {
			return code, reason
		}
	}
	return nil, nil
}

func (s *Storage) AddEvent(level, message string, details map[string]any) {
	_, _ = s.db.Exec(`INSERT INTO ingest_events(level, message, details) VALUES (?, ?, ?)`, level, message, nullableJSON(details))
}

func nullableJSON(value any) any {
	if value == nil {
		return nil
	}
	return jsonDumps(value)
}

func (s *Storage) RecentEvents(limit int) []map[string]any {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.queryRows(`SELECT level, message, details, created_at FROM ingest_events ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil
	}
	result := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		result = append(result, map[string]any{
			"level":      anyString(row["level"]),
			"message":    anyString(row["message"]),
			"details":    jsonLoads(anyString(row["details"]), nil),
			"created_at": nullableStringValue(anyNullableString(row["created_at"])),
		})
	}
	return result
}

func (s *Storage) PruneRetention() (int, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`DELETE FROM requests WHERE datetime(created_at) < datetime('now', ?)`, fmt.Sprintf("-%d days", s.retentionDays))
	if err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`DELETE FROM websocket_messages WHERE request_id NOT IN (SELECT id FROM requests)`); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	deleted, _ := res.RowsAffected()
	return int(deleted), nil
}

func (s *Storage) replaceWebSocketMessages(tx *sql.Tx, requestID string, messages []map[string]any) error {
	if _, err := tx.Exec(`DELETE FROM websocket_messages WHERE request_id = ?`, requestID); err != nil {
		return err
	}
	for _, message := range messages {
		if _, err := tx.Exec(`INSERT INTO websocket_messages(
            request_id, seq, direction, timestamp, opcode, message_type,
            data, data_json, is_binary, encoding, body_truncated, raw_message_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			requestID,
			intValue(message["seq"]),
			anyString(message["direction"]),
			ptrValue(message["timestamp"]),
			ptrIntValue(message["opcode"]),
			ptrValue(message["message_type"]),
			ptrValue(message["data"]),
			jsonDumps(message["data_json"]),
			boolInt(anyBool(message["is_binary"])),
			ptrValue(message["encoding"]),
			boolInt(anyBool(message["body_truncated"])),
			jsonDumps(message["raw"]),
		); err != nil {
			return err
		}
	}
	return nil
}

func ptrValue(value any) any {
	switch v := value.(type) {
	case *string:
		if v == nil {
			return nil
		}
		return *v
	case string:
		if v == "" {
			return nil
		}
		return v
	case nil:
		return nil
	default:
		text := anyString(value)
		if strings.TrimSpace(text) == "" {
			return nil
		}
		return text
	}
}

func ptrIntValue(value any) any {
	switch v := value.(type) {
	case *int:
		if v == nil {
			return nil
		}
		return *v
	case int:
		return v
	case int64:
		return v
	case nil:
		return nil
	default:
		if result := safeInt(value); result != nil {
			return *result
		}
		return nil
	}
}

func intValue(value any) int {
	if result := safeInt(value); result != nil {
		return *result
	}
	return 0
}

func (s *Storage) upsertRequestRecord(tx *sql.Tx, record map[string]any) (bool, error) {
	row := tx.QueryRow(`SELECT 1 FROM requests WHERE id = ?`, anyString(record["id"]))
	var existing int
	err := row.Scan(&existing)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	_, err = tx.Exec(`INSERT INTO requests(
        id, method, url, host, path, query_string, query_params,
        status, status_text, duration_ms, timestamp,
        request_headers, response_headers,
        request_body, response_body,
        request_body_json, response_body_json,
        content_type, has_auth, is_https, body_truncated,
        remote_ip, source, platform, reporter_host, raw_entry_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        method=excluded.method,
        url=excluded.url,
        host=excluded.host,
        path=excluded.path,
        query_string=excluded.query_string,
        query_params=excluded.query_params,
        status=excluded.status,
        status_text=excluded.status_text,
        duration_ms=excluded.duration_ms,
        timestamp=excluded.timestamp,
        request_headers=excluded.request_headers,
        response_headers=excluded.response_headers,
        request_body=excluded.request_body,
        response_body=excluded.response_body,
        request_body_json=excluded.request_body_json,
        response_body_json=excluded.response_body_json,
        content_type=excluded.content_type,
        has_auth=excluded.has_auth,
        is_https=excluded.is_https,
        body_truncated=excluded.body_truncated,
        remote_ip=excluded.remote_ip,
        source=excluded.source,
        platform=excluded.platform,
        reporter_host=excluded.reporter_host,
        raw_entry_json=excluded.raw_entry_json`,
		anyString(record["id"]), anyString(record["method"]), anyString(record["url"]), ptrValue(record["host"]), ptrValue(record["path"]), ptrValue(record["query_string"]), jsonDumps(record["query_params"]),
		ptrIntValue(record["status"]), ptrValue(record["status_text"]), ptrIntValue(record["duration_ms"]), ptrValue(record["timestamp"]),
		jsonDumps(record["request_headers"]), jsonDumps(record["response_headers"]),
		ptrValue(record["request_body"]), ptrValue(record["response_body"]),
		jsonDumps(record["request_body_json"]), jsonDumps(record["response_body_json"]),
		ptrValue(record["content_type"]), boolInt(anyBool(record["has_auth"])), boolInt(anyBool(record["is_https"])), boolInt(anyBool(record["body_truncated"])),
		ptrValue(record["remote_ip"]), ptrValue(record["source"]), ptrValue(record["platform"]), ptrValue(record["reporter_host"]), jsonDumps(record["raw_entry"]),
	)
	return exists, err
}

func (s *Storage) IngestPayload(payload any, source string, platform, reporterHost *string) map[string]any {
	entries := normalizer.ExtractEntries(payload)
	if len(entries) == 0 {
		s.AddEvent("warning", "No valid HAR entries found in payload", nil)
		return map[string]any{"received": 0, "inserted": 0, "updated": 0}
	}
	tx, err := s.db.Begin()
	if err != nil {
		s.AddEvent("error", "Ingest transaction failed", map[string]any{"error": err.Error()})
		return map[string]any{"received": len(entries), "inserted": 0, "updated": 0}
	}
	defer tx.Rollback()
	inserted, updated, websocketSessions, websocketMessages := 0, 0, 0, 0
	for _, entry := range entries {
		record := normalizer.NormalizeEntry(entry, s.maxBodySize, source, platform, reporterHost)
		existing, err := s.upsertRequestRecord(tx, record)
		if err != nil {
			s.AddEvent("error", "Upsert request failed", map[string]any{"error": err.Error()})
			return map[string]any{"received": len(entries), "inserted": inserted, "updated": updated}
		}
		messages, _ := record["websocket_messages"].([]map[string]any)
		if err := s.replaceWebSocketMessages(tx, anyString(record["id"]), messages); err != nil {
			s.AddEvent("error", "Replace websocket messages failed", map[string]any{"error": err.Error()})
			return map[string]any{"received": len(entries), "inserted": inserted, "updated": updated}
		}
		if anyBool(record["is_websocket"]) {
			websocketSessions++
			websocketMessages += len(messages)
		}
		if existing {
			updated++
		} else {
			inserted++
		}
	}
	if err := tx.Commit(); err != nil {
		s.AddEvent("error", "Commit payload failed", map[string]any{"error": err.Error()})
		return map[string]any{"received": len(entries), "inserted": inserted, "updated": updated}
	}
	s.AddEvent("info", "Payload ingested", map[string]any{
		"received":           len(entries),
		"inserted":           inserted,
		"updated":            updated,
		"websocket_sessions": websocketSessions,
		"websocket_messages": websocketMessages,
	})
	if s.ingestCalls.Add(1)%500 == 0 {
		_, _ = s.PruneRetention()
	}
	return map[string]any{
		"received":           len(entries),
		"inserted":           inserted,
		"updated":            updated,
		"websocket_sessions": websocketSessions,
		"websocket_messages": websocketMessages,
	}
}

func (s *Storage) buildWSSessionRecord(event map[string]any, requestID, source string, platform, reporterHost *string, existingRow map[string]any) map[string]any {
	sessionData, _ := event["session"].(map[string]any)
	requestObj, _ := event["request"].(map[string]any)
	if requestObj == nil {
		requestObj, _ = sessionData["request"].(map[string]any)
	}
	if requestObj == nil {
		requestObj = map[string]any{}
	}
	responseObj, _ := event["response"].(map[string]any)
	if responseObj == nil {
		responseObj, _ = sessionData["response"].(map[string]any)
	}
	if responseObj == nil {
		responseObj = map[string]any{}
	}

	existingRequestHeaders := jsonLoadsMapStringList(anyString(existingRow["request_headers"]))
	existingResponseHeaders := jsonLoadsMapStringList(anyString(existingRow["response_headers"]))

	method := strings.ToUpper(strings.TrimSpace(anyString(firstPresent(requestObj, "method"))))
	if method == "" {
		method = strings.ToUpper(strings.TrimSpace(anyString(firstPresent(event, "method"))))
	}
	if method == "" {
		method = strings.ToUpper(strings.TrimSpace(anyString(firstPresent(sessionData, "method"))))
	}
	if method == "" {
		method = strings.ToUpper(strings.TrimSpace(anyString(firstPresent(existingRow, "method"))))
	}
	if method == "" {
		method = "GET"
	}

	rawURL := strings.TrimSpace(anyString(firstPresent(requestObj, "url")))
	if rawURL == "" {
		rawURL = strings.TrimSpace(anyString(firstPresent(event, "url")))
	}
	if rawURL == "" {
		rawURL = strings.TrimSpace(anyString(firstPresent(sessionData, "url")))
	}
	if rawURL == "" {
		rawURL = strings.TrimSpace(anyString(firstPresent(existingRow, "url")))
	}
	if rawURL == "" {
		rawURL = fmt.Sprintf("ws://unknown.local/%s", requestID)
	}

	parsed, _ := url.Parse(rawURL)
	queryString, queryParams := queryParamsFromURL(rawURL)
	requestHeaders := normalizeHeaders(firstPresent(requestObj, "headers"))
	if len(requestHeaders) == 0 {
		requestHeaders = normalizeHeaders(firstPresent(event, "request_headers"))
	}
	if len(requestHeaders) == 0 {
		requestHeaders = normalizeHeaders(firstPresent(sessionData, "request_headers"))
	}
	if len(requestHeaders) == 0 {
		requestHeaders = existingRequestHeaders
	}
	responseHeaders := normalizeHeaders(firstPresent(responseObj, "headers"))
	if len(responseHeaders) == 0 {
		responseHeaders = normalizeHeaders(firstPresent(event, "response_headers"))
	}
	if len(responseHeaders) == 0 {
		responseHeaders = normalizeHeaders(firstPresent(sessionData, "response_headers"))
	}
	if len(responseHeaders) == 0 {
		responseHeaders = existingResponseHeaders
	}
	status := safeInt(firstPresent(responseObj, "status"))
	if status == nil {
		status = safeInt(firstPresent(event, "status"))
	}
	if status == nil {
		status = safeInt(firstPresent(existingRow, "status"))
	}

	statusText := anyNullableString(firstPresent(responseObj, "statusText"))
	if statusText == nil {
		statusText = anyNullableString(firstPresent(event, "status_text"))
	}
	if statusText == nil {
		statusText = anyNullableString(firstPresent(sessionData, "status_text"))
	}
	if statusText == nil {
		statusText = anyNullableString(firstPresent(existingRow, "status_text"))
	}

	durationMS := safeInt(firstPresent(event, "duration_ms"))
	if durationMS == nil {
		durationMS = safeInt(firstPresent(existingRow, "duration_ms"))
	}

	timestamp := anyNullableString(firstPresent(sessionData, "startedDateTime"))
	if timestamp == nil {
		timestamp = anyNullableString(firstPresent(event, "session_started_at", "timestamp", "time"))
	}
	if timestamp == nil {
		timestamp = anyNullableString(firstPresent(existingRow, "timestamp"))
	}

	remoteIP := anyNullableString(firstPresent(event, "remote_ip"))
	if remoteIP == nil {
		remoteIP = anyNullableString(firstPresent(sessionData, "remote_ip"))
	}
	if remoteIP == nil {
		remoteIP = anyNullableString(firstPresent(existingRow, "remote_ip"))
	}

	var contentType *string
	for key, values := range requestHeaders {
		if strings.EqualFold(key, "Content-Type") && len(values) > 0 {
			value := values[0]
			contentType = &value
			break
		}
	}
	if contentType == nil {
		contentType = anyNullableString(firstPresent(existingRow, "content_type"))
	}

	hasAuth := false
	for key := range requestHeaders {
		if strings.EqualFold(key, "authorization") {
			hasAuth = true
			break
		}
	}
	if len(requestHeaders) == 0 {
		hasAuth = anyBool(firstPresent(existingRow, "has_auth"))
	}

	rawEntry, _ := event["raw_session"].(map[string]any)
	if rawEntry == nil {
		rawEntry = sessionData
	}
	if rawEntry == nil {
		if existing := jsonLoads(anyString(existingRow["raw_entry_json"]), nil); existing != nil {
			rawEntry, _ = existing.(map[string]any)
		}
	}
	if rawEntry == nil {
		rawEntry = map[string]any{"session_id": requestID}
	}

	normalizedQueryParams := queryParams
	if len(normalizedQueryParams) == 0 {
		normalizedQueryParams = jsonLoadsMapString(anyString(existingRow["query_params"]))
	}

	var host, path *string
	isHTTPS := false
	if parsed != nil {
		if parsed.Hostname() != "" {
			value := parsed.Hostname()
			host = &value
		}
		if parsed.Path != "" {
			value := parsed.Path
			path = &value
		} else {
			value := "/"
			path = &value
		}
		switch strings.ToLower(parsed.Scheme) {
		case "https", "wss":
			isHTTPS = true
		case "http", "ws":
			isHTTPS = false
		default:
			isHTTPS = anyBool(firstPresent(existingRow, "is_https"))
		}
	}
	if host == nil {
		host = anyNullableString(firstPresent(existingRow, "host"))
	}
	if path == nil {
		path = anyNullableString(firstPresent(existingRow, "path"))
		if path == nil {
			value := "/"
			path = &value
		}
	}
	if queryString == nil {
		queryString = anyNullableString(firstPresent(existingRow, "query_string"))
	}

	requestBody := anyNullableString(firstPresent(existingRow, "request_body"))
	responseBody := anyNullableString(firstPresent(existingRow, "response_body"))
	requestBodyJSON := jsonLoads(anyString(existingRow["request_body_json"]), nil)
	responseBodyJSON := jsonLoads(anyString(existingRow["response_body_json"]), nil)
	bodyTruncated := anyBool(firstPresent(existingRow, "body_truncated"))
	existingPlatform := anyNullableString(firstPresent(existingRow, "platform"))
	existingReporterHost := anyNullableString(firstPresent(existingRow, "reporter_host"))

	finalPlatform := platform
	if finalPlatform == nil {
		finalPlatform = existingPlatform
	}
	finalReporterHost := reporterHost
	if finalReporterHost == nil {
		finalReporterHost = existingReporterHost
	}

	return map[string]any{
		"id":                 requestID,
		"method":             method,
		"url":                rawURL,
		"host":               host,
		"path":               path,
		"query_string":       queryString,
		"query_params":       normalizedQueryParams,
		"status":             status,
		"status_text":        statusText,
		"duration_ms":        durationMS,
		"timestamp":          timestamp,
		"request_headers":    requestHeaders,
		"response_headers":   responseHeaders,
		"request_body":       requestBody,
		"response_body":      responseBody,
		"request_body_json":  requestBodyJSON,
		"response_body_json": responseBodyJSON,
		"content_type":       contentType,
		"has_auth":           hasAuth,
		"is_https":           isHTTPS,
		"body_truncated":     bodyTruncated,
		"remote_ip":          remoteIP,
		"source":             source,
		"platform":           finalPlatform,
		"reporter_host":      finalReporterHost,
		"raw_entry":          rawEntry,
	}
}

func (s *Storage) extractWSEventFrames(event map[string]any) []map[string]any {
	frames := []map[string]any{}
	if rawFrames, ok := event["frames"].([]any); ok {
		frames = append(frames, filterMaps(rawFrames)...)
	}
	if frame, ok := event["frame"].(map[string]any); ok {
		frames = append(frames, frame)
	}
	if message, ok := event["message"].(map[string]any); ok {
		frames = append(frames, message)
	}
	if len(frames) > 0 {
		return frames
	}
	kind := eventType(event)
	switch kind {
	case "message", "frame", "ping", "pong", "close":
	default:
		return nil
	}
	generated := map[string]any{}
	for _, key := range []string{"direction", "fromClient", "outgoing", "flow", "opcode", "messageType", "type", "encoding", "binary"} {
		if value, ok := event[key]; ok {
			generated[key] = value
		}
	}
	if value, ok := event["timestamp"]; ok {
		generated["timestamp"] = value
	} else if value, ok := event["time"]; ok {
		generated["time"] = value
	}
	if value, ok := event["data"]; ok {
		generated["data"] = value
	} else if value, ok := event["payload_text"]; ok {
		generated["data"] = value
	} else if value, ok := event["payload"]; ok {
		generated["payload"] = value
	}
	closeCode := safeInt(event["close_code"])
	closeReason := strings.TrimSpace(anyString(event["close_reason"]))
	if kind == "close" {
		if _, ok := generated["opcode"]; !ok {
			generated["opcode"] = 8
		}
		payload, _ := generated["payload"].(map[string]any)
		if payload == nil {
			payload = map[string]any{}
		}
		if _, ok := payload["type"]; !ok {
			payload["type"] = 6
		}
		if closeCode != nil {
			payload["code"] = *closeCode
		}
		if closeReason != "" {
			payload["reason"] = closeReason
		}
		if len(payload) > 0 {
			generated["payload"] = payload
		}
	}
	if len(generated) == 0 {
		return nil
	}
	return []map[string]any{generated}
}

func (s *Storage) nextWSSeq(tx *sql.Tx, requestID string) int {
	row := tx.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM websocket_messages WHERE request_id = ?`, requestID)
	var seq int
	_ = row.Scan(&seq)
	return seq + 1
}

func (s *Storage) findDuplicateWSSeqByContent(tx *sql.Tx, requestID string, message map[string]any) *int {
	rawJSON := jsonDumps(message["raw"])
	if rawJSON != "null" && rawJSON != "" {
		row := tx.QueryRow(`SELECT seq FROM websocket_messages WHERE request_id = ? AND raw_message_json = ? ORDER BY seq DESC LIMIT 1`, requestID, rawJSON)
		var seq int
		if err := row.Scan(&seq); err == nil {
			return &seq
		}
	}
	row := tx.QueryRow(`SELECT seq FROM websocket_messages
        WHERE request_id = ?
          AND COALESCE(timestamp, '') = COALESCE(?, '')
          AND direction = ?
          AND COALESCE(opcode, -1) = COALESCE(?, -1)
          AND COALESCE(message_type, '') = COALESCE(?, '')
          AND COALESCE(data, '') = COALESCE(?, '')
        ORDER BY seq DESC LIMIT 1`,
		requestID,
		ptrValue(message["timestamp"]),
		anyString(message["direction"]),
		ptrIntValue(message["opcode"]),
		ptrValue(message["message_type"]),
		ptrValue(message["data"]),
	)
	var seq int
	if err := row.Scan(&seq); err == nil {
		return &seq
	}
	return nil
}

func (s *Storage) appendWebSocketMessage(tx *sql.Tx, requestID string, message map[string]any) (string, error) {
	rowMap, _ := s.queryRowsTx(tx, `SELECT direction, timestamp, opcode, message_type, data, data_json,
        is_binary, encoding, body_truncated, raw_message_json
        FROM websocket_messages WHERE request_id = ? AND seq = ?`, requestID, intValue(message["seq"]))
	var existing map[string]any
	if len(rowMap) > 0 {
		existing = rowMap[0]
	}
	if existing == nil {
		_, err := tx.Exec(`INSERT INTO websocket_messages(
            request_id, seq, direction, timestamp, opcode, message_type,
            data, data_json, is_binary, encoding, body_truncated, raw_message_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			requestID, intValue(message["seq"]), anyString(message["direction"]), ptrValue(message["timestamp"]), ptrIntValue(message["opcode"]), ptrValue(message["message_type"]), ptrValue(message["data"]), jsonDumps(message["data_json"]), boolInt(anyBool(message["is_binary"])), ptrValue(message["encoding"]), boolInt(anyBool(message["body_truncated"])), jsonDumps(message["raw"]),
		)
		if err == nil {
			return "inserted", nil
		}
		rowMap, _ = s.queryRowsTx(tx, `SELECT direction, timestamp, opcode, message_type, data, data_json,
            is_binary, encoding, body_truncated, raw_message_json
            FROM websocket_messages WHERE request_id = ? AND seq = ?`, requestID, intValue(message["seq"]))
		if len(rowMap) == 0 {
			return "", err
		}
		existing = rowMap[0]
	}

	mergedDirection := anyString(existing["direction"])
	if mergedDirection == "unknown" && anyString(message["direction"]) != "unknown" {
		mergedDirection = anyString(message["direction"])
	}
	mergedTimestamp := anyNullableString(existing["timestamp"])
	if mergedTimestamp == nil {
		mergedTimestamp = anyNullableString(message["timestamp"])
	}
	mergedOpcode := safeInt(existing["opcode"])
	if mergedOpcode == nil {
		mergedOpcode = safeInt(message["opcode"])
	}
	mergedMessageType := anyNullableString(existing["message_type"])
	if mergedMessageType == nil {
		mergedMessageType = anyNullableString(message["message_type"])
	}
	mergedData := anyNullableString(existing["data"])
	if mergedData == nil {
		mergedData = anyNullableString(message["data"])
	}
	incomingDataJSON := jsonDumps(message["data_json"])
	mergedDataJSON := anyString(existing["data_json"])
	if mergedDataJSON == "" || mergedDataJSON == "null" {
		mergedDataJSON = incomingDataJSON
	}
	mergedIsBinary := anyBool(existing["is_binary"]) || anyBool(message["is_binary"])
	mergedEncoding := anyNullableString(existing["encoding"])
	if mergedEncoding == nil {
		mergedEncoding = anyNullableString(message["encoding"])
	}
	mergedBodyTruncated := anyBool(existing["body_truncated"]) || anyBool(message["body_truncated"])
	incomingRaw := jsonDumps(message["raw"])
	mergedRaw := anyString(existing["raw_message_json"])
	if mergedRaw == "" || mergedRaw == "null" {
		mergedRaw = incomingRaw
	}
	changed := mergedDirection != anyString(existing["direction"]) ||
		nullableStringText(mergedTimestamp) != anyString(existing["timestamp"]) ||
		nullableIntValue(mergedOpcode) != nullableIntValue(safeInt(existing["opcode"])) ||
		nullableStringText(mergedMessageType) != anyString(existing["message_type"]) ||
		nullableStringText(mergedData) != anyString(existing["data"]) ||
		mergedDataJSON != anyString(existing["data_json"]) ||
		mergedIsBinary != anyBool(existing["is_binary"]) ||
		nullableStringText(mergedEncoding) != anyString(existing["encoding"]) ||
		mergedBodyTruncated != anyBool(existing["body_truncated"]) ||
		mergedRaw != anyString(existing["raw_message_json"])
	if !changed {
		return "duplicate", nil
	}
	_, err := tx.Exec(`UPDATE websocket_messages SET direction = ?, timestamp = ?, opcode = ?, message_type = ?,
        data = ?, data_json = ?, is_binary = ?, encoding = ?, body_truncated = ?, raw_message_json = ?
        WHERE request_id = ? AND seq = ?`,
		mergedDirection, nullableStringValue(mergedTimestamp), nullableIntAny(mergedOpcode), nullableStringValue(mergedMessageType), nullableStringValue(mergedData), mergedDataJSON, boolInt(mergedIsBinary), nullableStringValue(mergedEncoding), boolInt(mergedBodyTruncated), mergedRaw, requestID, intValue(message["seq"]),
	)
	if err != nil {
		return "", err
	}
	return "updated", nil
}

func nullableStringValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
func nullableStringText(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func nullableIntValue(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}
func nullableIntAny(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func (s *Storage) IngestWebSocketEvents(payload any, source string, platform, reporterHost *string) map[string]any {
	events := extractWSEvents(payload)
	defaults := wsEventDefaults(payload)
	if len(events) == 0 {
		s.AddEvent("warning", "No valid WebSocket events found in payload", nil)
		return map[string]any{"received_events": 0, "accepted_events": 0, "rejected_events": 0, "inserted_sessions": 0, "updated_sessions": 0, "inserted_messages": 0, "updated_messages": 0, "duplicate_messages": 0}
	}
	tx, err := s.db.Begin()
	if err != nil {
		s.AddEvent("error", "WebSocket ingest transaction failed", map[string]any{"error": err.Error()})
		return map[string]any{"received_events": len(events), "accepted_events": 0, "rejected_events": len(events)}
	}
	defer tx.Rollback()
	acceptedEvents, rejectedEvents, insertedSessions, updatedSessions := 0, 0, 0, 0
	insertedMessages, updatedMessages, duplicateMessages := 0, 0, 0
	sessionUpsertFields := map[string]struct{}{"session": {}, "request": {}, "response": {}, "raw_session": {}, "url": {}, "method": {}, "status": {}, "status_text": {}, "session_started_at": {}}
	sessionSeen := map[string]struct{}{}
	for _, event := range events {
		mergedEvent := mergeEventWithDefaults(event, defaults)
		sessionObj, _ := mergedEvent["session"].(map[string]any)
		requestID := eventID(firstPresent(mergedEvent, "session_id", "request_id", "id"))
		if requestID == "" && sessionObj != nil {
			requestID = eventID(sessionObj["id"])
		}
		if requestID == "" {
			rejectedEvents++
			continue
		}
		_, alreadySeen := sessionSeen[requestID]
		shouldUpsert := !alreadySeen
		if !shouldUpsert {
			for key := range sessionUpsertFields {
				if _, ok := event[key]; ok {
					shouldUpsert = true
					break
				}
			}
		}
		if shouldUpsert {
			existingRows, err := s.queryRowsTx(tx, `SELECT * FROM requests WHERE id = ?`, requestID)
			if err != nil {
				rejectedEvents++
				continue
			}
			var existingRow map[string]any
			if len(existingRows) > 0 {
				existingRow = existingRows[0]
			}
			record := s.buildWSSessionRecord(mergedEvent, requestID, source, platform, reporterHost, existingRow)
			wasExisting, err := s.upsertRequestRecord(tx, record)
			if err != nil {
				rejectedEvents++
				continue
			}
			if wasExisting {
				updatedSessions++
			} else {
				insertedSessions++
			}
			sessionSeen[requestID] = struct{}{}
		}
		rawFrames := s.extractWSEventFrames(mergedEvent)
		eventSeq := safeInt(mergedEvent["seq"])
		if len(rawFrames) == 0 {
			acceptedEvents++
			continue
		}
		nextSeq := s.nextWSSeq(tx, requestID)
		for index, rawFrame := range rawFrames {
			seq := safeInt(rawFrame["seq"])
			isAutoSeq := false
			if seq == nil && eventSeq != nil {
				value := *eventSeq + index
				seq = &value
			}
			if seq == nil || *seq <= 0 {
				value := nextSeq
				seq = &value
				nextSeq++
				isAutoSeq = true
			}
			normalized := normalizer.NormalizeWebSocketMessage(rawFrame, s.maxBodySize, *seq)
			if isAutoSeq {
				if duplicate := s.findDuplicateWSSeqByContent(tx, requestID, normalized); duplicate != nil {
					normalized["seq"] = *duplicate
				}
			}
			action, err := s.appendWebSocketMessage(tx, requestID, normalized)
			if err != nil {
				continue
			}
			switch action {
			case "inserted":
				insertedMessages++
			case "updated":
				updatedMessages++
			default:
				duplicateMessages++
			}
		}
		acceptedEvents++
	}
	if err := tx.Commit(); err != nil {
		s.AddEvent("error", "WebSocket events commit failed", map[string]any{"error": err.Error()})
	}
	result := map[string]any{"received_events": len(events), "accepted_events": acceptedEvents, "rejected_events": rejectedEvents, "inserted_sessions": insertedSessions, "updated_sessions": updatedSessions, "inserted_messages": insertedMessages, "updated_messages": updatedMessages, "duplicate_messages": duplicateMessages}
	s.AddEvent("info", "WebSocket events ingested", result)
	return result
}

func (s *Storage) ImportHARFile(path string, maxFileSizeBytes int64) (map[string]any, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if maxFileSizeBytes > 0 && info.Size() > maxFileSizeBytes {
		return nil, fmt.Errorf("HAR file too large: %d bytes > %d bytes", info.Size(), maxFileSizeBytes)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var payload any
	if err := json.Unmarshal(data, &payload); err == nil {
		return s.IngestPayload(payload, "har_import", nil, nil), nil
	}
	entries := []any{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, ","))
		if line == "" {
			continue
		}
		var item any
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			continue
		}
		if _, ok := item.(map[string]any); ok {
			entries = append(entries, item)
		}
	}
	return s.IngestPayload(entries, "har_import", nil, nil), nil
}

func (s *Storage) withWSProjection(columns string) string {
	return fmt.Sprintf(`%s,
        EXISTS(SELECT 1 FROM websocket_messages wm WHERE wm.request_id = requests.id) AS is_websocket,
        (SELECT COUNT(*) FROM websocket_messages wm WHERE wm.request_id = requests.id) AS websocket_message_count`, columns)
}

func (s *Storage) queryRows(query string, args ...any) ([]map[string]any, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func (s *Storage) queryRowsTx(tx *sql.Tx, query string, args ...any) ([]map[string]any, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func scanRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	results := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, col := range cols {
			switch v := values[i].(type) {
			case []byte:
				row[col] = string(v)
			default:
				row[col] = v
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func (s *Storage) queryRowsWithFilters(limit int, domain, method *string, statusCode *int, columns string, websocketOnly bool) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}
	whereParts := []string{}
	args := []any{}
	if domain != nil && strings.TrimSpace(*domain) != "" {
		whereParts = append(whereParts, "(host LIKE ? OR url LIKE ?)")
		needle := "%" + strings.TrimSpace(*domain) + "%"
		args = append(args, needle, needle)
	}
	if method != nil && strings.TrimSpace(*method) != "" {
		whereParts = append(whereParts, "UPPER(method) = ?")
		args = append(args, strings.ToUpper(strings.TrimSpace(*method)))
	}
	if statusCode != nil {
		whereParts = append(whereParts, "status = ?")
		args = append(args, *statusCode)
	}
	if websocketOnly {
		whereParts = append(whereParts, "EXISTS(SELECT 1 FROM websocket_messages wm WHERE wm.request_id = requests.id)")
	}
	whereClause := ""
	if len(whereParts) > 0 {
		whereClause = "WHERE " + strings.Join(whereParts, " AND ")
	}
	query := fmt.Sprintf(`SELECT %s FROM requests %s ORDER BY created_at DESC, id DESC LIMIT ?`, columns, whereClause)
	args = append(args, limit)
	return s.queryRows(query, args...)
}

func (s *Storage) rowToSummary(row map[string]any) RequestSummary {
	return RequestSummary{ID: anyString(row["id"]), Method: anyString(row["method"]), URL: anyString(row["url"]), Host: anyNullableString(row["host"]), Path: anyNullableString(row["path"]), Status: safeInt(row["status"]), DurationMS: safeInt(row["duration_ms"]), Timestamp: anyNullableString(row["timestamp"]), IsWebSocket: anyBool(row["is_websocket"]), WebSocketMessageCount: intValue(row["websocket_message_count"])}
}

func (s *Storage) rowToKey(row map[string]any) RequestKey {
	requestBody := anyNullableString(row["request_body"])
	responseBody := anyNullableString(row["response_body"])
	requestBodyJSON := jsonLoads(anyString(row["request_body_json"]), nil)
	responseBodyJSON := jsonLoads(anyString(row["response_body_json"]), nil)
	return RequestKey{ID: anyString(row["id"]), Method: anyString(row["method"]), URL: anyString(row["url"]), Host: anyNullableString(row["host"]), Path: anyNullableString(row["path"]), Status: safeInt(row["status"]), DurationMS: safeInt(row["duration_ms"]), Timestamp: anyNullableString(row["timestamp"]), QueryParams: jsonLoadsMapString(anyString(row["query_params"])), ContentType: anyNullableString(row["content_type"]), RequestBodyPreview: previewPtr(requestBody, s.keyBodyPreviewLength), RequestBodyStructure: structureOrNil(requestBodyJSON), ResponseBodyPreview: previewPtr(responseBody, s.keyBodyPreviewLength), ResponseBodyStructure: structureOrNil(responseBodyJSON), HasAuth: anyBool(row["has_auth"]), IsJSON: requestBodyJSON != nil || responseBodyJSON != nil, BodyTruncated: anyBool(row["body_truncated"]), IsWebSocket: anyBool(row["is_websocket"]), WebSocketMessageCount: intValue(row["websocket_message_count"])}
}

func structureOrNil(value any) any {
	if value == nil {
		return nil
	}
	return extractJSONStructure(value, 3, 0)
}

func (s *Storage) rowToWebSocketMessage(row map[string]any) WebSocketMessage {
	rawAny := jsonLoads(anyString(row["raw_message_json"]), nil)
	raw, _ := rawAny.(map[string]any)
	var derived map[string]any
	if raw != nil {
		derived = normalizer.NormalizeWebSocketMessage(raw, s.maxBodySize, intValue(row["seq"]))
	}
	direction := anyString(row["direction"])
	if direction == "unknown" && derived != nil {
		direction = anyString(derived["direction"])
	}
	timestamp := anyNullableString(row["timestamp"])
	if timestamp == nil && derived != nil {
		timestamp = anyNullableString(derived["timestamp"])
	}
	opcode := safeInt(row["opcode"])
	if opcode == nil && derived != nil {
		opcode = safeInt(derived["opcode"])
	}
	messageType := anyNullableString(row["message_type"])
	if messageType == nil && derived != nil {
		messageType = anyNullableString(derived["message_type"])
	}
	data := anyNullableString(row["data"])
	if data == nil && derived != nil {
		data = anyNullableString(derived["data"])
	}
	dataJSON := jsonLoads(anyString(row["data_json"]), nil)
	if dataJSON == nil && derived != nil {
		dataJSON = derived["data_json"]
	}
	isBinary := anyBool(row["is_binary"])
	if !isBinary && derived != nil {
		isBinary = anyBool(derived["is_binary"])
	}
	encoding := anyNullableString(row["encoding"])
	if encoding == nil && derived != nil {
		encoding = anyNullableString(derived["encoding"])
	}
	bodyTruncated := anyBool(row["body_truncated"])
	if !bodyTruncated && derived != nil {
		bodyTruncated = anyBool(derived["body_truncated"])
	}
	message := WebSocketMessage{Seq: intValue(row["seq"]), Direction: direction, Timestamp: timestamp, Opcode: opcode, MessageType: messageType, Data: data, DataJSON: dataJSON, IsBinary: isBinary, Encoding: encoding, BodyTruncated: bodyTruncated, Raw: raw}
	message.CloseCode, message.CloseReason = wsCloseDetails(message)
	return message
}

func (s *Storage) GetWebSocketMessages(requestID string, limit int) []WebSocketMessage {
	if limit <= 0 {
		limit = maxWSQueryLimit
	}
	if limit > 10000 {
		limit = 10000
	}
	rows, err := s.queryRows(`SELECT seq, direction, timestamp, opcode, message_type,
        data, data_json, is_binary, encoding, body_truncated, raw_message_json
        FROM websocket_messages WHERE request_id = ? ORDER BY seq ASC LIMIT ?`, requestID, limit)
	if err != nil {
		return nil
	}
	result := make([]WebSocketMessage, 0, len(rows))
	for _, row := range rows {
		result = append(result, s.rowToWebSocketMessage(row))
	}
	return result
}

func (s *Storage) rowToFull(row map[string]any) RequestFull {
	rawURL := anyString(row["url"])
	var port *int
	if strings.HasPrefix(rawURL, "https://") || strings.HasPrefix(rawURL, "wss://") {
		value := 443
		port = &value
	} else if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "ws://") {
		value := 80
		port = &value
	}
	return RequestFull{ID: anyString(row["id"]), Method: anyString(row["method"]), URL: rawURL, Protocol: "HTTP/1.1", Host: anyNullableString(row["host"]), Port: port, Path: anyNullableString(row["path"]), QueryString: anyNullableString(row["query_string"]), QueryParams: jsonLoadsMapString(anyString(row["query_params"])), Status: safeInt(row["status"]), StatusText: anyNullableString(row["status_text"]), DurationMS: safeInt(row["duration_ms"]), Timestamp: anyNullableString(row["timestamp"]), RequestHeaders: jsonLoadsMapStringList(anyString(row["request_headers"])), ResponseHeaders: jsonLoadsMapStringList(anyString(row["response_headers"])), RequestBody: anyNullableString(row["request_body"]), RequestBodyJSON: jsonLoads(anyString(row["request_body_json"]), nil), ResponseBody: anyNullableString(row["response_body"]), ResponseBodyJSON: jsonLoads(anyString(row["response_body_json"]), nil), RemoteIP: anyNullableString(row["remote_ip"]), IsHTTPS: anyBool(row["is_https"]), BodyTruncated: anyBool(row["body_truncated"]), Source: anyNullableString(row["source"]), Platform: anyNullableString(row["platform"]), ReporterHost: anyNullableString(row["reporter_host"]), IsWebSocket: anyBool(row["is_websocket"]), WebSocketMessageCount: intValue(row["websocket_message_count"]), RawEntry: mapOrNil(jsonLoads(anyString(row["raw_entry_json"]), nil))}
}

func mapOrNil(value any) map[string]any {
	if value == nil {
		return nil
	}
	result, _ := value.(map[string]any)
	return result
}

func normalizeDetailLevel(detailLevel string) DetailLevel {
	switch DetailLevel(strings.ToLower(strings.TrimSpace(detailLevel))) {
	case DetailSummary, DetailKey, DetailFull:
		return DetailLevel(strings.ToLower(strings.TrimSpace(detailLevel)))
	default:
		return DetailSummary
	}
}

func (s *Storage) GetRequests(limit int, detailLevel string, domain, method *string, statusCode *int, websocketOnly bool) []any {
	dl := normalizeDetailLevel(detailLevel)
	var rows []map[string]any
	var err error
	switch dl {
	case DetailSummary:
		rows, err = s.queryRowsWithFilters(limit, domain, method, statusCode, s.withWSProjection("id, method, url, host, path, status, duration_ms, timestamp"), websocketOnly)
	case DetailKey:
		rows, err = s.queryRowsWithFilters(limit, domain, method, statusCode, s.withWSProjection(`id, method, url, host, path, status, duration_ms, timestamp,
            query_params, content_type, request_body, request_body_json,
            response_body, response_body_json, has_auth, body_truncated`), websocketOnly)
	default:
		rows, err = s.queryRowsWithFilters(limit, domain, method, statusCode, s.withWSProjection("requests.*"), websocketOnly)
	}
	if err != nil {
		return nil
	}
	result := make([]any, 0, len(rows))
	switch dl {
	case DetailSummary:
		for _, row := range rows {
			result = append(result, s.rowToSummary(row))
		}
	case DetailKey:
		for _, row := range rows {
			result = append(result, s.rowToKey(row))
		}
	default:
		for _, row := range rows {
			item := s.rowToFull(row)
			if item.IsWebSocket {
				item.WebSocketMessages = s.GetWebSocketMessages(item.ID, maxWSQueryLimit)
			}
			result = append(result, item)
		}
	}
	return result
}

func (s *Storage) GetRequestByID(id, detailLevel string) any {
	rows, err := s.queryRows(fmt.Sprintf(`SELECT %s FROM requests WHERE id = ?`, s.withWSProjection("requests.*")), id)
	if err != nil || len(rows) == 0 {
		return nil
	}
	row := rows[0]
	switch normalizeDetailLevel(detailLevel) {
	case DetailSummary:
		return s.rowToSummary(row)
	case DetailKey:
		return s.rowToKey(row)
	default:
		item := s.rowToFull(row)
		if item.IsWebSocket {
			item.WebSocketMessages = s.GetWebSocketMessages(item.ID, maxWSQueryLimit)
		}
		return item
	}
}

func (s *Storage) Search(keyword, searchIn string, limit int) []map[string]any {
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}
	area := strings.ToLower(strings.TrimSpace(searchIn))
	if _, ok := validSearchAreas[area]; !ok {
		area = "all"
	}
	needle := strings.ToLower(keyword)
	if strings.TrimSpace(needle) == "" {
		return nil
	}
	var where string
	var args []any
	like := "%" + needle + "%"
	switch area {
	case "url":
		where = "LOWER(url) LIKE ?"
		args = []any{like}
	case "request_body":
		where = "LOWER(COALESCE(request_body, '')) LIKE ?"
		args = []any{like}
	case "response_body":
		where = "LOWER(COALESCE(response_body, '')) LIKE ?"
		args = []any{like}
	case "raw_entry", "raw":
		where = "LOWER(COALESCE(raw_entry_json, '')) LIKE ?"
		args = []any{like}
	default:
		where = `LOWER(url) LIKE ? OR LOWER(COALESCE(request_body, '')) LIKE ? OR LOWER(COALESCE(response_body, '')) LIKE ? OR LOWER(COALESCE(raw_entry_json, '')) LIKE ?`
		args = []any{like, like, like, like}
	}
	args = append(args, limit)
	rows, err := s.queryRows(fmt.Sprintf(`SELECT %s FROM requests WHERE %s ORDER BY created_at DESC, id DESC LIMIT ?`, s.withWSProjection("id, method, url, host, path, status, timestamp, request_body, response_body, raw_entry_json"), where), args...)
	if err != nil {
		return nil
	}
	result := []map[string]any{}
	for _, row := range rows {
		hitAreas := []string{}
		urlText := strings.ToLower(anyString(row["url"]))
		reqBody := strings.ToLower(anyString(row["request_body"]))
		respBody := strings.ToLower(anyString(row["response_body"]))
		rawEntry := strings.ToLower(anyString(row["raw_entry_json"]))
		if (area == "all" || area == "url") && strings.Contains(urlText, needle) {
			hitAreas = append(hitAreas, "url")
		}
		if (area == "all" || area == "request_body") && strings.Contains(reqBody, needle) {
			hitAreas = append(hitAreas, "request_body")
		}
		if (area == "all" || area == "response_body") && strings.Contains(respBody, needle) {
			hitAreas = append(hitAreas, "response_body")
		}
		if (area == "all" || area == "raw_entry" || area == "raw") && strings.Contains(rawEntry, needle) {
			hitAreas = append(hitAreas, "raw_entry")
		}
		if len(hitAreas) == 0 {
			continue
		}
		result = append(result, map[string]any{"id": anyString(row["id"]), "method": anyString(row["method"]), "url": anyString(row["url"]), "host": anyNullableString(row["host"]), "path": anyNullableString(row["path"]), "status": safeInt(row["status"]), "timestamp": anyNullableString(row["timestamp"]), "is_websocket": anyBool(row["is_websocket"]), "websocket_message_count": intValue(row["websocket_message_count"]), "_matches": hitAreas, "request_body_preview": previewRaw(anyNullableString(row["request_body"]), s.summaryBodyPreviewLength), "response_body_preview": previewRaw(anyNullableString(row["response_body"]), s.summaryBodyPreviewLength), "raw_entry_preview": previewRaw(anyNullableString(row["raw_entry_json"]), s.summaryBodyPreviewLength)})
		if len(result) >= limit {
			break
		}
	}
	return result
}

func previewRaw(text *string, maxLen int) any {
	if text == nil || *text == "" {
		return nil
	}
	value := *text
	if len(value) > maxLen {
		value = value[:maxLen]
	}
	return value
}

func (s *Storage) SearchWebSocketMessages(keyword string, direction, messageType *string, opcode, closeCode *int, requestID, domain *string, hasJSON *bool, limit int) []map[string]any {
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}
	normalizedKeyword := strings.ToLower(strings.TrimSpace(keyword))
	normalizedDirection := normalizeDirection(direction)
	normalizedMessageType := normalizeWSMessageType(messageType)
	normalizedRequestID := normalizeStringPtr(requestID)
	normalizedDomain := normalizeLowerStringPtr(domain)
	whereParts := []string{}
	args := []any{}
	if normalizedKeyword != "" {
		like := "%" + normalizedKeyword + "%"
		whereParts = append(whereParts, `(LOWER(COALESCE(wm.data, '')) LIKE ? OR LOWER(COALESCE(wm.data_json, '')) LIKE ? OR LOWER(COALESCE(wm.raw_message_json, '')) LIKE ?)`)
		args = append(args, like, like, like)
	}
	if normalizedRequestID != nil {
		whereParts = append(whereParts, "wm.request_id = ?")
		args = append(args, *normalizedRequestID)
	}
	if normalizedDomain != nil {
		whereParts = append(whereParts, "LOWER(COALESCE(r.host, '')) = ?")
		args = append(args, *normalizedDomain)
	}
	whereClause := ""
	if len(whereParts) > 0 {
		whereClause = "WHERE " + strings.Join(whereParts, " AND ")
	}
	needsPostFilter := normalizedDirection != nil || normalizedMessageType != nil || opcode != nil || closeCode != nil || hasJSON != nil
	queryLimit := limit
	if normalizedKeyword == "" {
		queryLimit = maxWSQueryLimit
	} else if needsPostFilter {
		queryLimit = limit * 20
		if queryLimit < 200 {
			queryLimit = 200
		}
		if queryLimit > maxWSQueryLimit {
			queryLimit = maxWSQueryLimit
		}
	}
	args = append(args, queryLimit)
	rows, err := s.queryRows(fmt.Sprintf(`SELECT wm.request_id, wm.seq, wm.direction, wm.timestamp, wm.opcode,
        wm.message_type, wm.data, wm.data_json, wm.is_binary,
        wm.encoding, wm.body_truncated, wm.raw_message_json,
        r.method, r.url, r.host, r.path, r.status
        FROM websocket_messages wm
        JOIN requests r ON r.id = wm.request_id
        %s ORDER BY r.created_at DESC, wm.seq DESC LIMIT ?`, whereClause), args...)
	if err != nil {
		return nil
	}
	result := []map[string]any{}
	for _, row := range rows {
		message := s.rowToWebSocketMessage(row)
		if normalizedDirection != nil && message.Direction != *normalizedDirection {
			continue
		}
		if normalizedMessageType != nil {
			if message.MessageType == nil || strings.ToLower(*message.MessageType) != *normalizedMessageType {
				continue
			}
		}
		if opcode != nil {
			if message.Opcode == nil || *message.Opcode != *opcode {
				continue
			}
		}
		if closeCode != nil {
			if message.CloseCode == nil || *message.CloseCode != *closeCode {
				continue
			}
		}
		if hasJSON != nil && (message.DataJSON != nil) != *hasJSON {
			continue
		}
		hitAreas := []string{}
		if normalizedKeyword != "" {
			if message.Data != nil && strings.Contains(strings.ToLower(*message.Data), normalizedKeyword) {
				hitAreas = append(hitAreas, "data")
			}
			if message.DataJSON != nil && strings.Contains(strings.ToLower(jsonDumps(message.DataJSON)), normalizedKeyword) {
				hitAreas = append(hitAreas, "data_json")
			}
			if strings.Contains(strings.ToLower(anyString(row["raw_message_json"])), normalizedKeyword) {
				hitAreas = append(hitAreas, "raw_message")
			}
		}
		result = append(result, map[string]any{"request_id": anyString(row["request_id"]), "seq": message.Seq, "direction": message.Direction, "timestamp": message.Timestamp, "opcode": message.Opcode, "message_type": message.MessageType, "data": message.Data, "data_json": message.DataJSON, "has_json": message.DataJSON != nil, "is_binary": message.IsBinary, "encoding": message.Encoding, "body_truncated": message.BodyTruncated, "close_code": message.CloseCode, "close_reason": message.CloseReason, "raw": message.Raw, "_matches": hitAreas, "method": anyString(row["method"]), "url": anyString(row["url"]), "host": anyNullableString(row["host"]), "path": anyNullableString(row["path"]), "status": safeInt(row["status"])})
		if len(result) >= limit {
			break
		}
	}
	return result
}

func normalizeDirection(value *string) *string {
	if value == nil {
		return nil
	}
	text := strings.TrimSpace(*value)
	if _, ok := validWSDirections[text]; ok {
		return &text
	}
	return nil
}

func normalizeWSMessageType(value *string) *string {
	if value == nil {
		return nil
	}
	text := strings.ToLower(strings.TrimSpace(*value))
	switch text {
	case "text", "binary", "close", "ping", "pong":
		return &text
	default:
		return nil
	}
}

func normalizeStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	text := strings.TrimSpace(*value)
	if text == "" {
		return nil
	}
	return &text
}
func normalizeLowerStringPtr(value *string) *string {
	if v := normalizeStringPtr(value); v != nil {
		lower := strings.ToLower(*v)
		return &lower
	}
	return nil
}

func (s *Storage) TailWebSocketMessages(requestID string, afterSeq *int, direction, messageType *string, includeRaw bool, limit int) map[string]any {
	normalizedRequestID := strings.TrimSpace(requestID)
	if normalizedRequestID == "" {
		return map[string]any{"request_id": requestID, "after_seq": afterSeq, "returned": 0, "next_after_seq": afterSeq, "messages": []any{}}
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}
	normalizedDirection := normalizeDirection(direction)
	normalizedMessageType := normalizeWSMessageType(messageType)
	queryLimit := limit
	if normalizedDirection != nil || normalizedMessageType != nil {
		queryLimit = limit * 20
		if queryLimit < 200 {
			queryLimit = 200
		}
		if queryLimit > maxWSQueryLimit {
			queryLimit = maxWSQueryLimit
		}
	}
	whereParts := []string{"request_id = ?"}
	args := []any{normalizedRequestID}
	orderClause := "ORDER BY seq DESC"
	if afterSeq != nil {
		whereParts = append(whereParts, "seq > ?")
		args = append(args, *afterSeq)
		orderClause = "ORDER BY seq ASC"
	}
	args = append(args, queryLimit)
	rows, err := s.queryRows(fmt.Sprintf(`SELECT request_id, seq, direction, timestamp, opcode, message_type,
        data, data_json, is_binary, encoding, body_truncated, raw_message_json
        FROM websocket_messages WHERE %s %s LIMIT ?`, strings.Join(whereParts, " AND "), orderClause), args...)
	if err != nil {
		return map[string]any{"request_id": normalizedRequestID, "after_seq": afterSeq, "returned": 0, "messages": []any{}}
	}
	orderedRows := rows
	if afterSeq == nil {
		reverseRows(orderedRows)
	}
	scannedUntilSeq := afterSeq
	if len(orderedRows) > 0 {
		value := intValue(orderedRows[len(orderedRows)-1]["seq"])
		scannedUntilSeq = &value
	}
	messages := []map[string]any{}
	for _, row := range orderedRows {
		message := s.rowToWebSocketMessage(row)
		if normalizedDirection != nil && message.Direction != *normalizedDirection {
			continue
		}
		if normalizedMessageType != nil {
			if message.MessageType == nil || strings.ToLower(*message.MessageType) != *normalizedMessageType {
				continue
			}
		}
		item := map[string]any{"request_id": normalizedRequestID, "seq": message.Seq, "direction": message.Direction, "timestamp": message.Timestamp, "opcode": message.Opcode, "message_type": message.MessageType, "data": message.Data, "data_json": message.DataJSON, "has_json": message.DataJSON != nil, "is_binary": message.IsBinary, "encoding": message.Encoding, "body_truncated": message.BodyTruncated, "close_code": message.CloseCode, "close_reason": message.CloseReason}
		if includeRaw {
			item["raw"] = message.Raw
		}
		messages = append(messages, item)
		if len(messages) >= limit {
			break
		}
	}
	nextAfterSeq := scannedUntilSeq
	if len(messages) > 0 {
		value := messages[len(messages)-1]["seq"].(int)
		nextAfterSeq = &value
	}
	return map[string]any{"request_id": normalizedRequestID, "after_seq": afterSeq, "returned": len(messages), "next_after_seq": nextAfterSeq, "scanned_until_seq": scannedUntilSeq, "messages": messages}
}

func reverseRows(rows []map[string]any) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

func (s *Storage) ListActiveWebSocketSessions(limit int, domain *string, activeWithinSeconds int, includeClosing bool) []map[string]any {
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if activeWithinSeconds <= 0 {
		activeWithinSeconds = 300
	}
	if activeWithinSeconds > 86400 {
		activeWithinSeconds = 86400
	}
	normalizedDomain := normalizeLowerStringPtr(domain)
	whereParts := []string{"datetime(agg.last_seen_at) >= datetime('now', ?)"}
	args := []any{fmt.Sprintf("-%d seconds", activeWithinSeconds)}
	if normalizedDomain != nil {
		whereParts = append(whereParts, "LOWER(COALESCE(r.host, '')) = ?")
		args = append(args, *normalizedDomain)
	}
	if !includeClosing {
		whereParts = append(whereParts, "agg.has_close_frame = 0")
	}
	args = append(args, limit)
	query := fmt.Sprintf(`WITH agg AS (
        SELECT request_id, MAX(created_at) AS last_seen_at, MAX(seq) AS last_seq,
               COUNT(*) AS message_count,
               MAX(CASE WHEN opcode = 8 OR LOWER(COALESCE(message_type, '')) = 'close' THEN 1 ELSE 0 END) AS has_close_frame
        FROM websocket_messages GROUP BY request_id
    )
    SELECT r.id, r.method, r.url, r.host, r.path, r.status, r.duration_ms, r.timestamp,
           agg.last_seen_at, agg.last_seq, agg.message_count, agg.has_close_frame,
           wm.direction AS last_direction, wm.message_type AS last_message_type, wm.opcode AS last_opcode
    FROM agg
    JOIN requests r ON r.id = agg.request_id
    LEFT JOIN websocket_messages wm ON wm.request_id = agg.request_id AND wm.seq = agg.last_seq
    WHERE %s ORDER BY agg.last_seen_at DESC, r.id DESC LIMIT ?`, strings.Join(whereParts, " AND "))
	rows, err := s.queryRows(query, args...)
	if err != nil {
		return nil
	}
	result := []map[string]any{}
	for _, row := range rows {
		result = append(result, map[string]any{"id": anyString(row["id"]), "method": anyString(row["method"]), "url": anyString(row["url"]), "host": anyNullableString(row["host"]), "path": anyNullableString(row["path"]), "status": safeInt(row["status"]), "duration_ms": safeInt(row["duration_ms"]), "timestamp": anyNullableString(row["timestamp"]), "is_websocket": true, "websocket_message_count": intValue(row["message_count"]), "last_message_at": anyNullableString(row["last_seen_at"]), "last_message_seq": intValue(row["last_seq"]), "last_message_direction": anyNullableString(row["last_direction"]), "last_message_type": anyNullableString(row["last_message_type"]), "last_message_opcode": safeInt(row["last_opcode"]), "has_close_frame": anyBool(row["has_close_frame"])})
	}
	return result
}

func (s *Storage) GetDomains(limit int) []map[string]any {
	if limit <= 0 {
		limit = 500
	}
	if limit > 2000 {
		limit = 2000
	}
	rows, err := s.queryRows(`SELECT host AS domain, COUNT(*) AS count, GROUP_CONCAT(DISTINCT UPPER(method)) AS methods
        FROM requests WHERE host IS NOT NULL AND host != '' GROUP BY host ORDER BY count DESC, domain ASC LIMIT ?`, limit)
	if err != nil {
		return nil
	}
	result := []map[string]any{}
	for _, row := range rows {
		methods := []string{}
		for _, item := range strings.Split(anyString(row["methods"]), ",") {
			if item != "" {
				methods = append(methods, item)
			}
		}
		sort.Strings(methods)
		result = append(result, map[string]any{"domain": anyString(row["domain"]), "count": intValue(row["count"]), "methods": methods})
	}
	return result
}

func (s *Storage) TotalRequests() int {
	row := s.db.QueryRow(`SELECT COUNT(*) FROM requests`)
	var count int
	_ = row.Scan(&count)
	return count
}
func (s *Storage) TotalWebSocketSessions() int {
	row := s.db.QueryRow(`SELECT COUNT(DISTINCT request_id) FROM websocket_messages`)
	var count int
	_ = row.Scan(&count)
	return count
}
func (s *Storage) TotalWebSocketMessages() int {
	row := s.db.QueryRow(`SELECT COUNT(*) FROM websocket_messages`)
	var count int
	_ = row.Scan(&count)
	return count
}

func (s *Storage) WebSocketHealthReport(sampleLimit int) map[string]any {
	if sampleLimit <= 0 {
		sampleLimit = 20
	}
	if sampleLimit > 50 {
		sampleLimit = 50
	}
	countWhere := func(where string, args ...any) int {
		row := s.db.QueryRow(`SELECT COUNT(*) FROM websocket_messages WHERE `+where, args...)
		var count int
		_ = row.Scan(&count)
		return count
	}
	samples := map[string]any{}
	if rows, err := s.queryRows(`SELECT request_id, seq, direction, opcode, message_type FROM websocket_messages WHERE raw_message_json IS NULL OR raw_message_json = '' ORDER BY created_at DESC LIMIT ?`, sampleLimit); err == nil {
		samples["messages_missing_raw"] = rows
	}
	if rows, err := s.queryRows(`SELECT request_id, seq, opcode, message_type FROM websocket_messages WHERE direction = 'unknown' ORDER BY created_at DESC LIMIT ?`, sampleLimit); err == nil {
		samples["messages_direction_unknown"] = rows
	}
	if rows, err := s.queryRows(`SELECT r.id AS request_id, r.url, r.created_at FROM requests r WHERE (r.raw_entry_json IS NULL OR r.raw_entry_json = '') AND EXISTS (SELECT 1 FROM websocket_messages wm WHERE wm.request_id = r.id) ORDER BY r.created_at DESC LIMIT ?`, sampleLimit); err == nil {
		samples["sessions_missing_raw_entry"] = rows
	}
	row := s.db.QueryRow(`SELECT COUNT(*) FROM requests r WHERE (r.raw_entry_json IS NULL OR r.raw_entry_json = '') AND EXISTS (SELECT 1 FROM websocket_messages wm WHERE wm.request_id = r.id)`)
	var sessionsMissingRaw int
	_ = row.Scan(&sessionsMissingRaw)
	return map[string]any{"total_requests": s.TotalRequests(), "total_websocket_sessions": s.TotalWebSocketSessions(), "total_websocket_messages": s.TotalWebSocketMessages(), "websocket_sessions_missing_raw_entry": sessionsMissingRaw, "websocket_messages_missing_raw": countWhere(`raw_message_json IS NULL OR raw_message_json = ''`), "websocket_messages_direction_unknown": countWhere(`direction = 'unknown'`), "websocket_messages_missing_opcode": countWhere(`opcode IS NULL`), "websocket_messages_missing_message_type": countWhere(`message_type IS NULL`), "websocket_messages_missing_data": countWhere(`data IS NULL OR data = ''`), "websocket_messages_missing_data_json": countWhere(`data_json IS NULL OR data_json = ''`), "samples": samples}
}

func (s *Storage) RepairWebSocketMessages(maxRows int, dryRun bool) map[string]any {
	if maxRows <= 0 {
		maxRows = 2000
	}
	if maxRows > maxWSQueryLimit {
		maxRows = maxWSQueryLimit
	}
	updatedFields := map[string]int{"direction": 0, "timestamp": 0, "opcode": 0, "message_type": 0, "data": 0, "data_json": 0, "is_binary": 0, "encoding": 0, "body_truncated": 0}
	rows, err := s.queryRows(`SELECT request_id, seq, direction, timestamp, opcode, message_type,
        data, data_json, is_binary, encoding, body_truncated, raw_message_json
        FROM websocket_messages
        WHERE raw_message_json IS NOT NULL AND raw_message_json != ''
          AND (direction = 'unknown' OR opcode IS NULL OR message_type IS NULL OR data IS NULL OR data_json IS NULL)
        ORDER BY created_at DESC LIMIT ?`, maxRows)
	if err != nil {
		return map[string]any{"ok": false, "error": err.Error()}
	}
	tx, err := s.db.Begin()
	if err != nil {
		return map[string]any{"ok": false, "error": err.Error()}
	}
	defer tx.Rollback()
	scanned, repaired := 0, 0
	for _, row := range rows {
		scanned++
		raw := mapOrNil(jsonLoads(anyString(row["raw_message_json"]), nil))
		if raw == nil {
			continue
		}
		derived := normalizer.NormalizeWebSocketMessage(raw, s.maxBodySize, intValue(row["seq"]))
		updates := map[string]any{}
		if anyString(row["direction"]) == "unknown" && anyString(derived["direction"]) != "unknown" {
			updates["direction"] = anyString(derived["direction"])
			updatedFields["direction"]++
		}
		if anyNullableString(row["timestamp"]) == nil && anyNullableString(derived["timestamp"]) != nil {
			updates["timestamp"] = ptrValue(derived["timestamp"])
			updatedFields["timestamp"]++
		}
		if safeInt(row["opcode"]) == nil && safeInt(derived["opcode"]) != nil {
			updates["opcode"] = ptrIntValue(derived["opcode"])
			updatedFields["opcode"]++
		}
		if anyNullableString(row["message_type"]) == nil && anyNullableString(derived["message_type"]) != nil {
			updates["message_type"] = ptrValue(derived["message_type"])
			updatedFields["message_type"]++
		}
		if anyNullableString(row["data"]) == nil && anyNullableString(derived["data"]) != nil {
			updates["data"] = ptrValue(derived["data"])
			updatedFields["data"]++
		}
		if row["data_json"] == nil && derived["data_json"] != nil {
			updates["data_json"] = jsonDumps(derived["data_json"])
			updatedFields["data_json"]++
		}
		if !anyBool(row["is_binary"]) && anyBool(derived["is_binary"]) {
			updates["is_binary"] = 1
			updatedFields["is_binary"]++
		}
		if anyNullableString(row["encoding"]) == nil && anyNullableString(derived["encoding"]) != nil {
			updates["encoding"] = ptrValue(derived["encoding"])
			updatedFields["encoding"]++
		}
		if !anyBool(row["body_truncated"]) && anyBool(derived["body_truncated"]) {
			updates["body_truncated"] = 1
			updatedFields["body_truncated"]++
		}
		if len(updates) == 0 {
			continue
		}
		repaired++
		if dryRun {
			continue
		}
		keys := make([]string, 0, len(updates))
		for key := range updates {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		assignments := make([]string, 0, len(keys))
		args := make([]any, 0, len(keys)+2)
		for _, key := range keys {
			assignments = append(assignments, key+" = ?")
			args = append(args, updates[key])
		}
		args = append(args, anyString(row["request_id"]), intValue(row["seq"]))
		if _, err := tx.Exec(`UPDATE websocket_messages SET `+strings.Join(assignments, ", ")+` WHERE request_id = ? AND seq = ?`, args...); err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
	}
	if !dryRun {
		if err := tx.Commit(); err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
	}
	return map[string]any{"ok": true, "dry_run": dryRun, "scanned": scanned, "repaired": repaired, "updated_fields": updatedFields}
}
