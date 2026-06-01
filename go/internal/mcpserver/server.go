package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/ssddi456/reqable-mcp/internal/config"
	"github.com/ssddi456/reqable-mcp/internal/ingest"
	"github.com/ssddi456/reqable-mcp/internal/storage"
)

type Server struct {
	cfg    config.Config
	store  *storage.Storage
	ingest *ingest.Manager
	mcp    *mcpserver.MCPServer
	sse    *mcpserver.SSEServer
}

func New(cfg config.Config, store *storage.Storage, ingestManager *ingest.Manager) *Server {
	srv := &Server{
		cfg:    cfg,
		store:  store,
		ingest: ingestManager,
	}
	core := mcpserver.NewMCPServer(
		"reqable",
		"1.0.0",
		mcpserver.WithToolCapabilities(false),
		mcpserver.WithRecovery(),
		mcpserver.WithInstructions("Analyze Reqable-captured HTTP and WebSocket traffic via the provided tools."),
	)
	srv.mcp = core
	srv.registerTools()
	srv.sse = mcpserver.NewSSEServer(core, mcpserver.WithBaseURL(cfg.MCPBaseURL()))
	return srv
}

func (s *Server) Start(addr string) error            { return s.sse.Start(addr) }
func (s *Server) Shutdown(ctx context.Context) error { return s.sse.Shutdown(ctx) }

func jsonTextResult(data any) (*mcp.CallToolResult, error) {
	body, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(body)), nil
}

func errorTextResult(message string) (*mcp.CallToolResult, error) {
	return mcp.NewToolResultError(message), nil
}

func normalizeLimit(value, defaultValue, max int) int {
	if value <= 0 {
		return defaultValue
	}
	if value > max {
		return max
	}
	return value
}

func normalizeOptionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func normalizeOptionalLowerString(value string) *string {
	if v := normalizeOptionalString(value); v != nil {
		lower := strings.ToLower(*v)
		return &lower
	}
	return nil
}

func asRequestFull(value any) (storage.RequestFull, bool) {
	switch v := value.(type) {
	case storage.RequestFull:
		return v, true
	case *storage.RequestFull:
		if v == nil {
			return storage.RequestFull{}, false
		}
		return *v, true
	default:
		return storage.RequestFull{}, false
	}
}

func toJSONMap(value any) map[string]any {
	body, _ := json.Marshal(value)
	out := map[string]any{}
	_ = json.Unmarshal(body, &out)
	return out
}

func toJSONArray(value any) []any {
	body, _ := json.Marshal(value)
	out := []any{}
	_ = json.Unmarshal(body, &out)
	return out
}

func requestValueToMap(value any) map[string]any {
	return toJSONMap(value)
}

func wsMessageSummary(message storage.WebSocketMessage) map[string]any {
	summary := map[string]any{
		"seq":            message.Seq,
		"direction":      message.Direction,
		"timestamp":      message.Timestamp,
		"opcode":         message.Opcode,
		"message_type":   message.MessageType,
		"has_json":       message.DataJSON != nil,
		"is_binary":      message.IsBinary,
		"body_truncated": message.BodyTruncated,
		"close_code":     message.CloseCode,
		"close_reason":   message.CloseReason,
		"raw_present":    message.Raw != nil,
	}
	if message.Data != nil && *message.Data != "" {
		preview := *message.Data
		if len(preview) > 160 {
			preview = preview[:160]
		}
		summary["data_preview"] = preview
	}
	if obj, ok := message.DataJSON.(map[string]any); ok {
		keys := make([]string, 0, len(obj))
		for key := range obj {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if len(keys) > 20 {
			keys = keys[:20]
		}
		summary["top_level_keys"] = keys
		summary["json_structure"] = extractJSONStructure(obj, 0, 3)
	}
	return summary
}

func extractJSONStructure(data any, depth, maxDepth int) any {
	if depth >= maxDepth {
		return "..."
	}
	switch v := data.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case string:
		return "string"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "int"
	case float32, float64:
		return "float"
	case []any:
		if len(v) == 0 {
			return []any{}
		}
		return []any{extractJSONStructure(v[0], depth+1, maxDepth)}
	case map[string]any:
		result := map[string]any{}
		count := 0
		for key, value := range v {
			result[key] = extractJSONStructure(value, depth+1, maxDepth)
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

func safeHeaders(req storage.RequestFull) map[string]string {
	headers := map[string]string{}
	for key, values := range req.RequestHeaders {
		if len(values) == 0 {
			continue
		}
		lower := strings.ToLower(key)
		if lower == "host" || lower == "content-length" || lower == "connection" {
			continue
		}
		headers[key] = values[0]
	}
	return headers
}

func genPython(req storage.RequestFull, framework string) string {
	lines := []string{}
	headers := safeHeaders(req)
	method := strings.ToUpper(req.Method)
	if framework == "httpx" {
		lines = append(lines, "import asyncio", "import httpx", "", "async def main():")
	} else {
		lines = append(lines, "import requests", "")
	}
	indent := ""
	if framework == "httpx" {
		indent = "    "
	}
	lines = append(lines,
		fmt.Sprintf("%smethod = %q", indent, method),
		fmt.Sprintf("%surl = %q", indent, req.URL),
	)
	if len(headers) > 0 {
		body, _ := json.Marshal(headers)
		lines = append(lines, fmt.Sprintf("%sheaders = %s", indent, string(body)))
	}
	payloadParam := ""
	if req.RequestBodyJSON != nil {
		body, _ := json.Marshal(req.RequestBodyJSON)
		lines = append(lines, fmt.Sprintf("%spayload = %s", indent, string(body)))
		payloadParam = ", json=payload"
	} else if req.RequestBody != nil {
		lines = append(lines, fmt.Sprintf("%spayload = %q", indent, *req.RequestBody))
		payloadParam = ", data=payload"
	}
	headersParam := ""
	if len(headers) > 0 {
		headersParam = ", headers=headers"
	}
	lines = append(lines, "")
	if framework == "httpx" {
		lines = append(lines,
			indent+"async with httpx.AsyncClient(timeout=30) as client:",
			fmt.Sprintf("%s    response = await client.request(method, url%s%s)", indent, headersParam, payloadParam),
			indent+"    print(response.status_code)",
			indent+"    print(response.text)",
			"",
			"asyncio.run(main())",
		)
	} else {
		lines = append(lines,
			fmt.Sprintf("response = requests.request(method, url%s%s, timeout=30)", headersParam, payloadParam),
			"print(response.status_code)",
			"print(response.text)",
		)
	}
	return strings.Join(lines, "\n")
}

func genJS(req storage.RequestFull, framework string) string {
	lines := []string{}
	headers := safeHeaders(req)
	method := strings.ToUpper(req.Method)
	if framework == "axios" {
		lines = append(lines, "import axios from 'axios';", "")
		config := map[string]any{"method": method, "url": req.URL}
		if len(headers) > 0 {
			config["headers"] = headers
		}
		if req.RequestBodyJSON != nil {
			config["data"] = req.RequestBodyJSON
		} else if req.RequestBody != nil {
			config["data"] = *req.RequestBody
		}
		body, _ := json.MarshalIndent(config, "", "  ")
		lines = append(lines, "const config = "+string(body)+";", "", "axios(config)", "  .then((response) => console.log(response.data))", "  .catch((error) => console.error(error));")
		return strings.Join(lines, "\n")
	}
	lines = append(lines, fmt.Sprintf("const url = %q;", req.URL))
	options := map[string]any{"method": method}
	if len(headers) > 0 {
		options["headers"] = headers
	}
	body, _ := json.MarshalIndent(options, "", "  ")
	lines = append(lines, "const options = "+string(body)+";")
	if req.RequestBodyJSON != nil {
		payload, _ := json.Marshal(req.RequestBodyJSON)
		lines = append(lines, "options.body = JSON.stringify("+string(payload)+");")
	} else if req.RequestBody != nil {
		lines = append(lines, fmt.Sprintf("options.body = %q;", *req.RequestBody))
	}
	lines = append(lines, "", "fetch(url, options)", "  .then((response) => response.text())", "  .then((text) => console.log(text))", "  .catch((error) => console.error('Error:', error));")
	return strings.Join(lines, "\n")
}

func (s *Server) registerTools() {
	s.mcp.AddTool(mcp.NewTool("ingest_status", mcp.WithDescription("Check local ingest server status")), s.handleIngestStatus)
	s.mcp.AddTool(mcp.NewTool("health_report", mcp.WithDescription("Return ingest status plus WebSocket data quality checks"), mcp.WithBoolean("detail", mcp.Description("Include recent events")), mcp.WithNumber("sample_limit", mcp.Description("Max sample rows"))), s.handleHealthReport)
	s.mcp.AddTool(mcp.NewTool("import_har", mcp.WithDescription("Import a HAR file or line-delimited JSON entries"), mcp.WithString("file_path", mcp.Required(), mcp.Description("Path to the HAR or JSON file"))), s.handleImportHAR)
	s.mcp.AddTool(mcp.NewTool("list_requests", mcp.WithDescription("List recent HTTP/WebSocket handshake requests"), mcp.WithNumber("limit", mcp.Description("Maximum items")), mcp.WithString("detail", mcp.Description("summary, key, or full")), mcp.WithString("domain", mcp.Description("Domain filter")), mcp.WithString("method", mcp.Description("HTTP method filter")), mcp.WithNumber("status", mcp.Description("Status code filter"))), s.handleListRequests)
	s.mcp.AddTool(mcp.NewTool("list_websocket_sessions", mcp.WithDescription("List captured WebSocket sessions"), mcp.WithNumber("limit", mcp.Description("Maximum items")), mcp.WithString("detail", mcp.Description("summary, key, or full")), mcp.WithString("domain", mcp.Description("Domain filter"))), s.handleListWebSocketSessions)
	s.mcp.AddTool(mcp.NewTool("list_active_websocket_sessions", mcp.WithDescription("List recently active WebSocket sessions"), mcp.WithNumber("limit", mcp.Description("Maximum items")), mcp.WithString("domain", mcp.Description("Domain filter")), mcp.WithNumber("active_within_seconds", mcp.Description("Recent activity window in seconds")), mcp.WithBoolean("include_closing", mcp.Description("Include sessions with close frames"))), s.handleListActiveWSSessions)
	s.mcp.AddTool(mcp.NewTool("get_request", mcp.WithDescription("Get detailed information for a single request"), mcp.WithString("request_id", mcp.Required(), mcp.Description("Request id")), mcp.WithBoolean("include_body", mcp.Description("Include full request/response body"))), s.handleGetRequest)
	s.mcp.AddTool(mcp.NewTool("get_websocket_session", mcp.WithDescription("Get WebSocket session details and messages by request id"), mcp.WithString("request_id", mcp.Required(), mcp.Description("Request id")), mcp.WithBoolean("include_messages", mcp.Description("Include websocket_messages"))), s.handleGetWebSocketSession)
	s.mcp.AddTool(mcp.NewTool("tail_websocket_messages", mcp.WithDescription("Tail WebSocket messages for one session using seq cursor"), mcp.WithString("request_id", mcp.Required(), mcp.Description("Request id")), mcp.WithNumber("after_seq", mcp.Description("Cursor sequence")), mcp.WithString("direction", mcp.Description("inbound, outbound, unknown")), mcp.WithString("message_type", mcp.Description("text, binary, close, ping, pong")), mcp.WithBoolean("include_raw", mcp.Description("Include raw message payloads")), mcp.WithNumber("limit", mcp.Description("Maximum items"))), s.handleTailWebSocketMessages)
	s.mcp.AddTool(mcp.NewTool("search_requests", mcp.WithDescription("Search requests by keyword in URL, bodies, or raw entry"), mcp.WithString("keyword", mcp.Required(), mcp.Description("Search keyword")), mcp.WithString("search_in", mcp.Description("all, url, request_body, response_body, raw_entry, raw")), mcp.WithNumber("limit", mcp.Description("Maximum items"))), s.handleSearchRequests)
	s.mcp.AddTool(mcp.NewTool("search_websocket_messages", mcp.WithDescription("Search WebSocket messages by keyword and precise frame filters"), mcp.WithString("keyword", mcp.Description("Keyword")), mcp.WithString("direction", mcp.Description("inbound, outbound, unknown")), mcp.WithString("message_type", mcp.Description("text, binary, close, ping, pong")), mcp.WithNumber("opcode", mcp.Description("Opcode filter")), mcp.WithString("request_id", mcp.Description("Request id filter")), mcp.WithString("domain", mcp.Description("Domain filter")), mcp.WithNumber("close_code", mcp.Description("Close code filter")), mcp.WithBoolean("has_json", mcp.Description("Filter by parsed JSON presence")), mcp.WithNumber("limit", mcp.Description("Maximum items"))), s.handleSearchWebSocketMessages)
	s.mcp.AddTool(mcp.NewTool("repair_websocket_messages", mcp.WithDescription("Backfill missing WebSocket message fields from raw frames"), mcp.WithNumber("max_rows", mcp.Description("Maximum rows to inspect")), mcp.WithBoolean("dry_run", mcp.Description("Do not write changes"))), s.handleRepairWebSocketMessages)
	s.mcp.AddTool(mcp.NewTool("analyze_websocket_session", mcp.WithDescription("Analyze a WebSocket session and summarize frames"), mcp.WithString("request_id", mcp.Required(), mcp.Description("Request id")), mcp.WithNumber("sample_limit", mcp.Description("Samples per direction"))), s.handleAnalyzeWebSocketSession)
	s.mcp.AddTool(mcp.NewTool("export_websocket_session_raw", mcp.WithDescription("Export the raw uploaded WebSocket session entry and raw frame list"), mcp.WithString("request_id", mcp.Required(), mcp.Description("Request id")), mcp.WithBoolean("include_normalized", mcp.Description("Include normalized session data"))), s.handleExportWebSocketRaw)
	s.mcp.AddTool(mcp.NewTool("get_domains", mcp.WithDescription("Get all captured domains with request counts")), s.handleGetDomains)
	s.mcp.AddTool(mcp.NewTool("analyze_api", mcp.WithDescription("Analyze API structure for a specific domain"), mcp.WithString("domain", mcp.Required(), mcp.Description("Domain name"))), s.handleAnalyzeAPI)
	s.mcp.AddTool(mcp.NewTool("generate_code", mcp.WithDescription("Generate API call code from a captured request"), mcp.WithString("request_id", mcp.Required(), mcp.Description("Request id")), mcp.WithString("language", mcp.Description("python, javascript, typescript, curl")), mcp.WithString("framework", mcp.Description("requests/httpx/fetch/axios"))), s.handleGenerateCode)
}

func (s *Server) handleIngestStatus(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return jsonTextResult(s.ingest.Status(true))
}

func (s *Server) handleHealthReport(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	detail := request.GetBool("detail", true)
	sampleLimit := normalizeLimit(request.GetInt("sample_limit", 5), 5, 50)
	return jsonTextResult(map[string]any{"ingest": s.ingest.Status(detail), "websocket_health": s.store.WebSocketHealthReport(sampleLimit)})
}

func (s *Server) handleImportHAR(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	filePath, err := request.RequireString("file_path")
	if err != nil {
		return errorTextResult(err.Error())
	}
	result, importErr := s.store.ImportHARFile(filePath, s.cfg.MaxImportFileSize)
	if importErr != nil {
		s.store.AddEvent("error", "import_har failed", map[string]any{"file": filePath, "error": importErr.Error()})
		return jsonTextResult(map[string]any{"ok": false, "error": importErr.Error()})
	}
	out := map[string]any{"ok": true, "file": filePath}
	for k, v := range result {
		out[k] = v
	}
	return jsonTextResult(out)
}

func (s *Server) handleListRequests(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	limit := normalizeLimit(request.GetInt("limit", 20), 20, 100)
	detail := request.GetString("detail", "summary")
	domain := normalizeOptionalString(request.GetString("domain", ""))
	method := normalizeOptionalString(request.GetString("method", ""))
	var status *int
	if raw := request.GetArguments()["status"]; raw != nil {
		val := request.GetInt("status", 0)
		status = &val
	}
	return jsonTextResult(toJSONArray(s.store.GetRequests(limit, detail, domain, method, status, false)))
}

func (s *Server) handleListWebSocketSessions(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	limit := normalizeLimit(request.GetInt("limit", 20), 20, 100)
	detail := request.GetString("detail", "summary")
	domain := normalizeOptionalString(request.GetString("domain", ""))
	return jsonTextResult(toJSONArray(s.store.GetRequests(limit, detail, domain, nil, nil, true)))
}

func (s *Server) handleListActiveWSSessions(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	limit := normalizeLimit(request.GetInt("limit", 20), 20, 200)
	domain := normalizeOptionalString(request.GetString("domain", ""))
	activeWithin := normalizeLimit(request.GetInt("active_within_seconds", 300), 300, 86400)
	includeClosing := request.GetBool("include_closing", false)
	return jsonTextResult(s.store.ListActiveWebSocketSessions(limit, domain, activeWithin, includeClosing))
}

func (s *Server) handleGetRequest(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	requestID, err := request.RequireString("request_id")
	if err != nil {
		return errorTextResult(err.Error())
	}
	detail := "key"
	if request.GetBool("include_body", true) {
		detail = "full"
	}
	result := s.store.GetRequestByID(requestID, detail)
	if result == nil {
		return jsonTextResult(map[string]any{"error": fmt.Sprintf("Request %s not found", requestID)})
	}
	data := requestValueToMap(result)
	if full, ok := asRequestFull(result); ok && !full.IsWebSocket {
		data["curl_command"] = full.ToCurl()
	}
	return jsonTextResult(data)
}

func (s *Server) handleGetWebSocketSession(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	requestID, err := request.RequireString("request_id")
	if err != nil {
		return errorTextResult(err.Error())
	}
	detail := "key"
	includeMessages := request.GetBool("include_messages", true)
	if includeMessages {
		detail = "full"
	}
	result := s.store.GetRequestByID(requestID, detail)
	if result == nil {
		return jsonTextResult(map[string]any{"error": fmt.Sprintf("Request %s not found", requestID)})
	}
	data := requestValueToMap(result)
	if websocket, ok := data["is_websocket"].(bool); !ok || !websocket {
		return jsonTextResult(map[string]any{"error": fmt.Sprintf("Request %s is not a WebSocket session", requestID), "request": data})
	}
	if !includeMessages {
		delete(data, "websocket_messages")
	}
	return jsonTextResult(data)
}

func (s *Server) handleTailWebSocketMessages(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	requestID, err := request.RequireString("request_id")
	if err != nil {
		return errorTextResult(err.Error())
	}
	var afterSeq *int
	if raw := request.GetArguments()["after_seq"]; raw != nil {
		value := request.GetInt("after_seq", 0)
		afterSeq = &value
	}
	direction := normalizeOptionalLowerString(request.GetString("direction", ""))
	messageType := normalizeOptionalLowerString(request.GetString("message_type", ""))
	includeRaw := request.GetBool("include_raw", false)
	limit := normalizeLimit(request.GetInt("limit", 20), 20, 200)
	return jsonTextResult(s.store.TailWebSocketMessages(requestID, afterSeq, direction, messageType, includeRaw, limit))
}

func (s *Server) handleSearchRequests(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	keyword, err := request.RequireString("keyword")
	if err != nil {
		return errorTextResult(err.Error())
	}
	limit := normalizeLimit(request.GetInt("limit", 20), 20, 100)
	return jsonTextResult(s.store.Search(keyword, request.GetString("search_in", "all"), limit))
}

func (s *Server) handleSearchWebSocketMessages(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	keyword := request.GetString("keyword", "")
	direction := normalizeOptionalLowerString(request.GetString("direction", ""))
	messageType := normalizeOptionalLowerString(request.GetString("message_type", ""))
	requestID := normalizeOptionalString(request.GetString("request_id", ""))
	domain := normalizeOptionalString(request.GetString("domain", ""))
	var opcode, closeCode *int
	args := request.GetArguments()
	if args["opcode"] != nil {
		value := request.GetInt("opcode", 0)
		opcode = &value
	}
	if args["close_code"] != nil {
		value := request.GetInt("close_code", 0)
		closeCode = &value
	}
	var hasJSON *bool
	if args["has_json"] != nil {
		value := request.GetBool("has_json", false)
		hasJSON = &value
	}
	limit := normalizeLimit(request.GetInt("limit", 20), 20, 100)
	return jsonTextResult(s.store.SearchWebSocketMessages(keyword, direction, messageType, opcode, closeCode, requestID, domain, hasJSON, limit))
}

func (s *Server) handleRepairWebSocketMessages(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	maxRows := normalizeLimit(request.GetInt("max_rows", 2000), 2000, 5000)
	return jsonTextResult(s.store.RepairWebSocketMessages(maxRows, request.GetBool("dry_run", false)))
}

func (s *Server) handleAnalyzeWebSocketSession(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	requestID, err := request.RequireString("request_id")
	if err != nil {
		return errorTextResult(err.Error())
	}
	sampleLimit := normalizeLimit(request.GetInt("sample_limit", 3), 3, 10)
	result := s.store.GetRequestByID(requestID, "full")
	if result == nil {
		return jsonTextResult(map[string]any{"error": fmt.Sprintf("Request %s not found", requestID)})
	}
	req, ok := asRequestFull(result)
	if !ok || !req.IsWebSocket {
		return jsonTextResult(map[string]any{"error": fmt.Sprintf("Request %s is not a WebSocket session", requestID)})
	}
	directionCounts := map[string]int{}
	messageTypeCounts := map[string]int{}
	opcodeCounts := map[string]int{}
	closeCodeCounts := map[string]int{}
	topKeys := map[string]int{}
	semanticFields := []string{"event", "type", "action", "command", "topic", "method"}
	semanticMarkers := map[string]map[string]int{}
	samples := map[string][]map[string]any{"outbound": {}, "inbound": {}, "unknown": {}}
	closeEvents := []map[string]any{}
	jsonMessageCount, rawMessageCount, binaryCount, truncatedCount := 0, 0, 0, 0
	for _, field := range semanticFields {
		semanticMarkers[field] = map[string]int{}
	}
	for _, message := range req.WebSocketMessages {
		directionCounts[message.Direction]++
		kind := "unknown"
		if message.MessageType != nil && *message.MessageType != "" {
			kind = *message.MessageType
		}
		messageTypeCounts[kind]++
		if message.Opcode != nil {
			opcodeCounts[fmt.Sprintf("%d", *message.Opcode)]++
		} else {
			opcodeCounts["unknown"]++
		}
		if message.Raw != nil {
			rawMessageCount++
		}
		if message.DataJSON != nil {
			jsonMessageCount++
		}
		if message.IsBinary {
			binaryCount++
		}
		if message.BodyTruncated {
			truncatedCount++
		}
		if len(samples[message.Direction]) < sampleLimit {
			samples[message.Direction] = append(samples[message.Direction], wsMessageSummary(message))
		}
		if message.CloseCode != nil {
			closeCodeCounts[fmt.Sprintf("%d", *message.CloseCode)]++
		}
		if message.CloseCode != nil || message.CloseReason != nil || kind == "close" {
			closeEvents = append(closeEvents, map[string]any{"seq": message.Seq, "direction": message.Direction, "timestamp": message.Timestamp, "opcode": message.Opcode, "close_code": message.CloseCode, "close_reason": message.CloseReason, "raw_present": message.Raw != nil})
		}
		if obj, ok := message.DataJSON.(map[string]any); ok {
			for key := range obj {
				topKeys[key]++
			}
			for _, field := range semanticFields {
				switch value := obj[field].(type) {
				case string:
					if strings.TrimSpace(value) != "" {
						semanticMarkers[field][value]++
					}
				case bool, float64, int:
					semanticMarkers[field][fmt.Sprintf("%v", value)]++
				}
			}
		}
	}
	topLevelKeys := make([]map[string]any, 0, len(topKeys))
	for key, count := range topKeys {
		topLevelKeys = append(topLevelKeys, map[string]any{"key": key, "count": count})
	}
	sort.Slice(topLevelKeys, func(i, j int) bool { return topLevelKeys[i]["count"].(int) > topLevelKeys[j]["count"].(int) })
	if len(topLevelKeys) > 15 {
		topLevelKeys = topLevelKeys[:15]
	}
	semanticResult := map[string]any{}
	for _, field := range semanticFields {
		items := []map[string]any{}
		for value, count := range semanticMarkers[field] {
			items = append(items, map[string]any{"value": value, "count": count})
		}
		sort.Slice(items, func(i, j int) bool { return items[i]["count"].(int) > items[j]["count"].(int) })
		if len(items) > 10 {
			items = items[:10]
		}
		if len(items) > 0 {
			semanticResult[field] = items
		}
	}
	sampleOut := map[string]any{}
	for direction, items := range samples {
		if len(items) > 0 {
			sampleOut[direction] = items
		}
	}
	return jsonTextResult(map[string]any{"request_id": req.ID, "url": req.URL, "host": req.Host, "path": req.Path, "status": req.Status, "timestamp": req.Timestamp, "source": req.Source, "platform": req.Platform, "websocket_message_count": req.WebSocketMessageCount, "raw_entry_present": req.RawEntry != nil, "raw_message_count": rawMessageCount, "missing_raw_message_count": max(0, len(req.WebSocketMessages)-rawMessageCount), "json_message_count": jsonMessageCount, "binary_message_count": binaryCount, "body_truncated_message_count": truncatedCount, "directions": directionCounts, "message_types": messageTypeCounts, "opcodes": opcodeCounts, "close_codes": closeCodeCounts, "top_level_keys": topLevelKeys, "semantic_markers": semanticResult, "close_events": closeEvents, "samples": sampleOut})
}


func (s *Server) handleExportWebSocketRaw(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	requestID, err := request.RequireString("request_id")
	if err != nil {
		return errorTextResult(err.Error())
	}
	includeNormalized := request.GetBool("include_normalized", false)
	result := s.store.GetRequestByID(requestID, "full")
	if result == nil {
		return jsonTextResult(map[string]any{"error": fmt.Sprintf("Request %s not found", requestID)})
	}
	req, ok := asRequestFull(result)
	if !ok || !req.IsWebSocket {
		return jsonTextResult(map[string]any{"error": fmt.Sprintf("Request %s is not a WebSocket session", requestID)})
	}
	rawMessages := []map[string]any{}
	rawPresent := 0
	for _, message := range req.WebSocketMessages {
		if message.Raw != nil {
			rawPresent++
		}
		rawMessages = append(rawMessages, map[string]any{"seq": message.Seq, "direction": message.Direction, "timestamp": message.Timestamp, "raw": message.Raw})
	}
	out := map[string]any{"request_id": req.ID, "url": req.URL, "host": req.Host, "path": req.Path, "status": req.Status, "timestamp": req.Timestamp, "raw_entry": req.RawEntry, "raw_messages": rawMessages, "raw_message_count": rawPresent, "missing_raw_message_count": len(rawMessages) - rawPresent}
	if includeNormalized {
		out["normalized_session"] = requestValueToMap(req)
	}
	return jsonTextResult(out)
}

func (s *Server) handleGetDomains(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return jsonTextResult(s.store.GetDomains(1000))
}

var numericPathPattern = regexp.MustCompile(`/\d+`)
var uuidishPathPattern = regexp.MustCompile(`(?i)/[a-f0-9-]{32,}`)

func (s *Server) handleAnalyzeAPI(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	domain, err := request.RequireString("domain")
	if err != nil {
		return errorTextResult(err.Error())
	}
	normalizedDomain := strings.TrimSpace(domain)
	if normalizedDomain == "" {
		return jsonTextResult(map[string]any{"error": "domain is required"})
	}
	requests := s.store.GetRequests(300, "key", &normalizedDomain, nil, nil, false)
	endpoints := map[string]map[string]any{}
	for _, item := range requests {
		data := requestValueToMap(item)
		if websocket, _ := data["is_websocket"].(bool); websocket {
			continue
		}
		path := "/"
		if value, ok := data["path"].(string); ok && value != "" {
			path = value
		}
		normalizedPath := numericPathPattern.ReplaceAllString(path, "/{id}")
		normalizedPath = uuidishPathPattern.ReplaceAllString(normalizedPath, "/{uuid}")
		method, _ := data["method"].(string)
		key := method + " " + normalizedPath
		endpoint := endpoints[key]
		if endpoint == nil {
			endpoint = map[string]any{"method": method, "path": normalizedPath, "count": 0, "statuses": map[int]struct{}{}, "has_auth": false, "request_structure": nil, "response_structure": nil, "sample_url": data["url"]}
			endpoints[key] = endpoint
		}
		endpoint["count"] = endpoint["count"].(int) + 1
		if statusF, ok := data["status"].(float64); ok {
			endpoint["statuses"].(map[int]struct{})[int(statusF)] = struct{}{}
		}
		if statusI, ok := data["status"].(int); ok {
			endpoint["statuses"].(map[int]struct{})[statusI] = struct{}{}
		}
		if hasAuth, ok := data["has_auth"].(bool); ok && hasAuth {
			endpoint["has_auth"] = true
		}
		if endpoint["request_structure"] == nil && data["request_body_structure"] != nil {
			endpoint["request_structure"] = data["request_body_structure"]
		}
		if endpoint["response_structure"] == nil && data["response_body_structure"] != nil {
			endpoint["response_structure"] = data["response_body_structure"]
		}
	}
	endpointList := []map[string]any{}
	for _, endpoint := range endpoints {
		statuses := []int{}
		for status := range endpoint["statuses"].(map[int]struct{}) {
			statuses = append(statuses, status)
		}
		sort.Ints(statuses)
		endpointList = append(endpointList, map[string]any{"method": endpoint["method"], "path": endpoint["path"], "count": endpoint["count"], "statuses": statuses, "has_auth": endpoint["has_auth"], "request_structure": endpoint["request_structure"], "response_structure": endpoint["response_structure"], "sample_url": endpoint["sample_url"]})
	}
	sort.Slice(endpointList, func(i, j int) bool { return endpointList[i]["count"].(int) > endpointList[j]["count"].(int) })
	return jsonTextResult(map[string]any{"domain": normalizedDomain, "total_requests": len(requests), "endpoints_count": len(endpointList), "endpoints": endpointList})
}

func (s *Server) handleGenerateCode(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	requestID, err := request.RequireString("request_id")
	if err != nil {
		return errorTextResult(err.Error())
	}
	language := strings.ToLower(strings.TrimSpace(request.GetString("language", "python")))
	framework := strings.ToLower(strings.TrimSpace(request.GetString("framework", "requests")))
	reqVal := s.store.GetRequestByID(requestID, "full")
	if reqVal == nil {
		return mcp.NewToolResultText(fmt.Sprintf("# Error: Request %s not found", requestID)), nil
	}
	req, ok := asRequestFull(reqVal)
	if !ok {
		return mcp.NewToolResultText(fmt.Sprintf("# Error: Request %s unavailable as full detail", requestID)), nil
	}
	if req.IsWebSocket {
		return mcp.NewToolResultText("# Error: WebSocket sessions are not supported by generate_code yet. Use get_websocket_session/analyze_websocket_session/export_websocket_session_raw instead."), nil
	}
	switch language {
	case "curl":
		return mcp.NewToolResultText(req.ToCurl()), nil
	case "python":
		if framework != "httpx" {
			framework = "requests"
		}
		return mcp.NewToolResultText(genPython(req, framework)), nil
	case "javascript", "typescript":
		if framework != "axios" {
			framework = "fetch"
		}
		return mcp.NewToolResultText(genJS(req, framework)), nil
	default:
		return mcp.NewToolResultText(fmt.Sprintf("# Error: unsupported language %q. Use one of [curl javascript python typescript]", language)), nil
	}
}
