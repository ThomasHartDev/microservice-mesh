package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type SpanContext struct {
	TraceID, SpanID, ParentSpanID string
	Sampled                       bool
}

var traceRE = regexp.MustCompile(`(?i)^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$`)

const zeroTrace = "00000000000000000000000000000000"
const zeroSpan = "0000000000000000"

func ParseTraceparent(header string) *SpanContext {
	m := traceRE.FindStringSubmatch(strings.TrimSpace(header))
	if m == nil {
		return nil
	}
	version, traceID, spanID, flags := strings.ToLower(m[1]), strings.ToLower(m[2]), strings.ToLower(m[3]), strings.ToLower(m[4])
	if version == "ff" || traceID == zeroTrace || spanID == zeroSpan {
		return nil
	}
	n, err := strconv.ParseUint(flags, 16, 8)
	if err != nil {
		return nil
	}
	return &SpanContext{TraceID: traceID, SpanID: spanID, Sampled: n&1 == 1}
}

func FormatTraceparent(ctx SpanContext) string {
	flags := "00"
	if ctx.Sampled {
		flags = "01"
	}
	return "00-" + ctx.TraceID + "-" + ctx.SpanID + "-" + flags
}

func randomHex(n int, zero string) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	out := hex.EncodeToString(b)
	if out == zero {
		return out[:len(out)-1] + "1"
	}
	return out
}

func StartSpan(parent *SpanContext) SpanContext {
	spanID := randomHex(8, zeroSpan)
	if parent == nil {
		return SpanContext{TraceID: randomHex(16, zeroTrace), SpanID: spanID, Sampled: true}
	}
	return SpanContext{TraceID: parent.TraceID, SpanID: spanID, ParentSpanID: parent.SpanID, Sampled: parent.Sampled}
}

func ContinueFrom(header string) SpanContext { return StartSpan(ParseTraceparent(header)) }

type logLine struct {
	Ts            string `json:"ts"`
	Level         string `json:"level"`
	Service       string `json:"service"`
	Msg           string `json:"msg"`
	TraceID       string `json:"trace_id,omitempty"`
	SpanID        string `json:"span_id,omitempty"`
	ParentSpanID  string `json:"parent_span_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	MessageID     string `json:"message_id,omitempty"`
}

func writeLog(w io.Writer, span SpanContext, corr, msgID, msg string) {
	if w == nil {
		return
	}
	line, err := json.Marshal(logLine{
		Ts: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Level: "info", Service: "gateway", Msg: msg,
		TraceID: span.TraceID, SpanID: span.SpanID, ParentSpanID: span.ParentSpanID,
		CorrelationID: corr, MessageID: msgID,
	})
	if err != nil {
		return
	}
	_, _ = w.Write(append(line, '\n'))
}
