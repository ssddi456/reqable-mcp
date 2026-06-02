package normalizer

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

func asText(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case nil:
		return ""
	case json.Number:
		return v.String()
	case map[string]any, []any:
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", value)
		}
		return string(data)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func sha1Short(text string) string {
	sum := sha1.Sum([]byte(text))
	return hex.EncodeToString(sum[:])[:12]
}

func entryID(entry map[string]any, requestBody, responseBody string, status *int) string {
	req, _ := entry["request"].(map[string]any)
	statusText := ""
	if status != nil {
		statusText = strconv.Itoa(*status)
	}
	seed := strings.Join([]string{
		strings.TrimSpace(asText(entry["_id"])),
		asText(anyMapGet(req, "method", "GET")),
		asText(anyMapGet(req, "url", "")),
		asText(entry["startedDateTime"]),
		asText(entry["time"]),
		statusText,
		sha1Short(requestBody),
		sha1Short(responseBody),
	}, "|")
	sum := sha1.Sum([]byte(seed))
	return "req-" + hex.EncodeToString(sum[:])[:24]
}

func anyMapGet(m map[string]any, key string, fallback any) any {
	if m == nil {
		return fallback
	}
	if v, ok := m[key]; ok {
		return v
	}
	return fallback
}

func buildHeaders(items any) map[string][]string {
	headers := map[string][]string{}
	list, ok := items.([]any)
	if !ok {
		return headers
	}
	for _, item := range list {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := strings.TrimSpace(asText(row["name"]))
		if name == "" {
			continue
		}
		headers[name] = append(headers[name], asText(row["value"]))
	}
	return headers
}

func firstHeader(headers map[string][]string, name string) *string {
	for key, values := range headers {
		if strings.EqualFold(key, name) {
			if len(values) == 0 {
				return nil
			}
			value := values[0]
			return &value
		}
	}
	return nil
}

func headerContains(headers map[string][]string, name, expected string) bool {
	value := firstHeader(headers, name)
	return value != nil && strings.Contains(strings.ToLower(*value), strings.ToLower(expected))
}

func parseQueryParams(parsed *url.URL) map[string]string {
	if parsed == nil || parsed.RawQuery == "" {
		return map[string]string{}
	}
	result := map[string]string{}
	values := parsed.Query()
	for key, items := range values {
		if len(items) == 1 {
			result[key] = items[0]
			continue
		}
		data, _ := json.Marshal(items)
		result[key] = string(data)
	}
	return result
}

func decodeResponseText(content map[string]any) string {
	textValue := asText(content["text"])
	encoding := strings.ToLower(asText(content["encoding"]))
	if encoding != "base64" {
		return textValue
	}
	decoded, err := base64.StdEncoding.DecodeString(textValue)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(textValue)
		if err != nil {
			return textValue
		}
	}
	return string(decoded)
}

func websocketCandidates(entry map[string]any) []map[string]any {
	for _, key := range []string{"_webSocketMessages", "_websocketMessages", "webSocketMessages", "websocketMessages", "websocket_frames", "websocketFrames", "messages", "frames"} {
		if list, ok := entry[key].([]any); ok {
			result := make([]map[string]any, 0, len(list))
			for _, item := range list {
				if msg, ok := item.(map[string]any); ok {
					result = append(result, msg)
				}
			}
			return result
		}
	}
	return nil
}

func payloadDict(message map[string]any) map[string]any {
	payload, _ := message["payload"].(map[string]any)
	return payload
}

func normalizeWSDirection(message map[string]any) string {
	if v, ok := message["fromClient"]; ok {
		if toBool(v) {
			return "outbound"
		}
		return "inbound"
	}
	if v, ok := message["outgoing"]; ok {
		if toBool(v) {
			return "outbound"
		}
		return "inbound"
	}
	if v, ok := message["flow"]; ok {
		if i, ok := toInt(v); ok {
			if i == 0 {
				return "outbound"
			}
			if i == 1 {
				return "inbound"
			}
		}
	}
	for _, field := range []string{"direction", "side", "sender", "type"} {
		raw := strings.ToLower(strings.TrimSpace(asText(message[field])))
		switch raw {
		case "send", "sent", "out", "outbound", "client", "request", "up":
			return "outbound"
		case "receive", "received", "in", "inbound", "server", "response", "down":
			return "inbound"
		}
	}
	return "unknown"
}

func normalizeWSOpcode(message map[string]any) *int {
	if raw, ok := message["opcode"]; ok {
		if i, ok := toInt(raw); ok {
			return &i
		}
	}
	payload := payloadDict(message)
	if payload != nil {
		if t, ok := toInt(payload["type"]); ok {
			if t == 2 {
				value := 1
				return &value
			}
			if t == 6 {
				value := 8
				return &value
			}
		}
	}
	return nil
}

func normalizeWSMessageType(message map[string]any, opcode *int) *string {
	for _, field := range []string{"messageType", "frameType", "frame_kind"} {
		raw := strings.ToLower(strings.TrimSpace(asText(message[field])))
		if raw != "" {
			return &raw
		}
	}
	fallback := strings.ToLower(strings.TrimSpace(asText(message["type"])))
	switch fallback {
	case "text", "binary", "close", "ping", "pong":
		return &fallback
	}
	payload := payloadDict(message)
	if payload != nil {
		if t, ok := toInt(payload["type"]); ok {
			switch t {
			case 2:
				raw := "text"
				return &raw
			case 6:
				raw := "close"
				return &raw
			}
		}
	}
	if opcode != nil {
		switch *opcode {
		case 1:
			raw := "text"
			return &raw
		case 2:
			raw := "binary"
			return &raw
		case 8:
			raw := "close"
			return &raw
		case 9:
			raw := "ping"
			return &raw
		case 10:
			raw := "pong"
			return &raw
		}
	}
	return nil
}

func extractWSCloseDetails(message map[string]any, dataJSON any) (*int, *string) {
	candidates := []any{payloadDict(message), dataJSON, message}
	for _, candidate := range candidates {
		item, ok := candidate.(map[string]any)
		if !ok {
			continue
		}
		var codePtr *int
		if code, ok := toInt(item["code"]); ok {
			codePtr = &code
		}
		var reasonPtr *string
		if value, exists := item["reason"]; exists && value != nil {
			reason := strings.TrimSpace(asText(value))
			if reason != "" {
				reasonPtr = &reason
			}
		}
		if codePtr != nil || reasonPtr != nil {
			return codePtr, reasonPtr
		}
	}
	return nil, nil
}

func extractWSPayload(message map[string]any) string {
	for _, key := range []string{"data", "text", "payload", "payloadData", "body", "content"} {
		value, ok := message[key]
		if !ok || value == nil {
			continue
		}
		if nested, ok := value.(map[string]any); ok {
			for _, nestedKey := range []string{"text", "data", "payload", "payloadData", "body", "content"} {
				if nestedValue, ok := nested[nestedKey]; ok && nestedValue != nil {
					return asText(nestedValue)
				}
			}
		}
		return asText(value)
	}
	return ""
}

func ParseJSONIfPossible(text string) any {
	if text == "" {
		return nil
	}
	var result any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		return nil
	}
	return result
}

func TruncateBody(body string, maxLength int) (string, bool) {
	if len(body) <= maxLength {
		return body, false
	}
	return body[:maxLength] + "...[truncated]", true
}

func toBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		parsed, _ := strconv.ParseBool(v)
		return parsed
	case float64:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	default:
		return false
	}
}

func toInt(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case uint:
		return int(v), true
	case uint32:
		return int(v), true
	case uint64:
		return int(v), true
	case float64:
		return int(v), true
	case float32:
		return int(v), true
	case json.Number:
		i, err := v.Int64()
		if err == nil {
			return int(i), true
		}
		f, err := v.Float64()
		if err == nil {
			return int(f), true
		}
		return 0, false
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, false
		}
		i, err := strconv.Atoi(strings.TrimSpace(v))
		return i, err == nil
	default:
		return 0, false
	}
}

func NormalizeWebSocketMessage(message map[string]any, maxBodySize int, seq int) map[string]any {
	payloadRaw := extractWSPayload(message)
	payload, truncated := TruncateBody(payloadRaw, maxBodySize)
	opcode := normalizeWSOpcode(message)
	messageType := normalizeWSMessageType(message, opcode)
	dataJSON := ParseJSONIfPossible(payload)
	closeCode, closeReason := extractWSCloseDetails(message, dataJSON)
	isBinary := toBool(message["binary"])
	if !isBinary && messageType != nil && *messageType == "binary" {
		isBinary = true
	}
	if !isBinary && opcode != nil && *opcode == 2 {
		isBinary = true
	}
	encoding := strings.TrimSpace(asText(message["encoding"]))
	var encodingPtr *string
	if encoding != "" {
		encodingPtr = &encoding
	}
	timestamp := strings.TrimSpace(asText(firstNonNil(
		message["time"],
		message["timestamp"],
		message["createdDateTime"],
		message["dateTime"],
		message["sentDateTime"],
		message["receivedDateTime"],
	)))
	var timestampPtr *string
	if timestamp != "" {
		timestampPtr = &timestamp
	}
	return map[string]any{
		"seq":            seq,
		"direction":      normalizeWSDirection(message),
		"timestamp":      timestampPtr,
		"opcode":         opcode,
		"message_type":   messageType,
		"data":           nilIfEmpty(payload),
		"data_json":      dataJSON,
		"is_binary":      isBinary,
		"encoding":       encodingPtr,
		"body_truncated": truncated,
		"close_code":     closeCode,
		"close_reason":   closeReason,
		"raw":            message,
	}
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil && asText(value) != "" {
			return value
		}
	}
	return nil
}

func nilIfEmpty(text string) any {
	if text == "" {
		return nil
	}
	return text
}

func normalizeWebSocketMessages(entry map[string]any, maxBodySize int) []map[string]any {
	candidates := websocketCandidates(entry)
	result := make([]map[string]any, 0, len(candidates))
	for idx, message := range candidates {
		result = append(result, NormalizeWebSocketMessage(message, maxBodySize, idx+1))
	}
	return result
}

func ExtractEntries(payload any) []map[string]any {
	switch v := payload.(type) {
	case map[string]any:
		if logValue, ok := v["log"].(map[string]any); ok {
			if entries, ok := logValue["entries"].([]any); ok {
				return filterRequestEntries(entries)
			}
		}
		if entries, ok := v["entries"].([]any); ok {
			return filterRequestEntries(entries)
		}
		if _, ok := v["request"].(map[string]any); ok {
			return []map[string]any{v}
		}
	case []any:
		return filterRequestEntries(v)
	}
	return nil
}

func filterRequestEntries(items []any) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if _, hasReq := entry["request"].(map[string]any); hasReq {
			result = append(result, entry)
		}
	}
	return result
}

func NormalizeEntry(entry map[string]any, maxBodySize int, source string, platform, reporterHost *string) map[string]any {
	req, _ := entry["request"].(map[string]any)
	resp, _ := entry["response"].(map[string]any)
	method := strings.ToUpper(asText(anyMapGet(req, "method", "GET")))
	if method == "" {
		method = "GET"
	}
	rawURL := strings.TrimSpace(asText(anyMapGet(req, "url", "")))
	parsed, _ := url.Parse(rawURL)
	host := ""
	path := "/"
	queryString := any(nil)
	queryParams := map[string]string{}
	isHTTPS := false
	if parsed != nil {
		host = parsed.Hostname()
		if parsed.Path != "" {
			path = parsed.Path
		}
		if parsed.RawQuery != "" {
			queryString = parsed.RawQuery
		}
		queryParams = parseQueryParams(parsed)
		isHTTPS = strings.EqualFold(parsed.Scheme, "https") || strings.EqualFold(parsed.Scheme, "wss")
	}
	var status *int
	if resp != nil {
		if i, ok := toInt(resp["status"]); ok {
			status = &i
		}
	}
	requestHeaders := buildHeaders(anyMapGet(req, "headers", nil))
	responseHeaders := buildHeaders(anyMapGet(resp, "headers", nil))
	postData, _ := anyMapGet(req, "postData", map[string]any{}).(map[string]any)
	requestBodyRaw := asText(anyMapGet(postData, "text", ""))
	responseContent, _ := anyMapGet(resp, "content", map[string]any{}).(map[string]any)
	responseBodyRaw := decodeResponseText(responseContent)
	requestBody, requestTruncated := TruncateBody(requestBodyRaw, maxBodySize)
	responseBody, responseTruncated := TruncateBody(responseBodyRaw, maxBodySize)
	websocketMessages := normalizeWebSocketMessages(entry, maxBodySize)
	websocketTruncated := false
	for _, item := range websocketMessages {
		if b, _ := item["body_truncated"].(bool); b {
			websocketTruncated = true
			break
		}
	}
	bodyTruncated := requestTruncated || responseTruncated || websocketTruncated
	contentType := strings.TrimSpace(asText(anyMapGet(postData, "mimeType", "")))
	if contentType == "" {
		if header := firstHeader(requestHeaders, "Content-Type"); header != nil {
			contentType = *header
		}
	}
	var contentTypePtr *string
	if contentType != "" {
		contentTypePtr = &contentType
	}
	statusText := strings.TrimSpace(asText(anyMapGet(resp, "statusText", "")))
	var statusTextPtr *string
	if statusText != "" {
		statusTextPtr = &statusText
	}
	timestamp := strings.TrimSpace(asText(entry["startedDateTime"]))
	var timestampPtr *string
	if timestamp != "" {
		timestampPtr = &timestamp
	}
	requestBodyJSON := ParseJSONIfPossible(requestBody)
	responseBodyJSON := ParseJSONIfPossible(responseBody)
	var durationMS *int
	if raw := strings.TrimSpace(asText(entry["time"])); raw != "" {
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			value := int(f)
			durationMS = &value
		}
	}
	isWebSocket := len(websocketMessages) > 0
	if !isWebSocket {
		resourceType := strings.ToLower(strings.TrimSpace(asText(firstNonNil(entry["_resourceType"], entry["resourceType"]))))
		isWebSocket = resourceType == "websocket"
	}
	if !isWebSocket && parsed != nil {
		isWebSocket = strings.EqualFold(parsed.Scheme, "ws") || strings.EqualFold(parsed.Scheme, "wss")
	}
	if !isWebSocket && status != nil {
		isWebSocket = *status == 101 && headerContains(requestHeaders, "Upgrade", "websocket") && headerContains(responseHeaders, "Upgrade", "websocket")
	}
	hasAuth := firstHeader(requestHeaders, "Authorization") != nil
	var hostPtr, pathPtr, remoteIPPtr, requestBodyPtr, responseBodyPtr *string
	if host != "" {
		hostPtr = &host
	}
	if path != "" {
		pathPtr = &path
	}
	remoteIP := strings.TrimSpace(asText(entry["serverIPAddress"]))
	if remoteIP != "" {
		remoteIPPtr = &remoteIP
	}
	if requestBody != "" {
		requestBodyPtr = &requestBody
	}
	if responseBody != "" {
		responseBodyPtr = &responseBody
	}
	return map[string]any{
		"id":                 entryID(entry, requestBody, responseBody, status),
		"method":             method,
		"url":                rawURL,
		"host":               hostPtr,
		"path":               pathPtr,
		"query_string":       queryString,
		"query_params":       queryParams,
		"status":             status,
		"status_text":        statusTextPtr,
		"duration_ms":        durationMS,
		"timestamp":          timestampPtr,
		"request_headers":    requestHeaders,
		"response_headers":   responseHeaders,
		"request_body":       requestBodyPtr,
		"response_body":      responseBodyPtr,
		"request_body_json":  requestBodyJSON,
		"response_body_json": responseBodyJSON,
		"content_type":       contentTypePtr,
		"has_auth":           hasAuth,
		"is_https":           isHTTPS,
		"body_truncated":     bodyTruncated,
		"remote_ip":          remoteIPPtr,
		"source":             source,
		"platform":           platform,
		"reporter_host":      reporterHost,
		"is_websocket":       isWebSocket,
		"websocket_messages": websocketMessages,
		"raw_entry":          entry,
	}
}
