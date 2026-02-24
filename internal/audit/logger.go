package audit

import (
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Event represents a single audit log entry.
type Event struct {
	Timestamp    time.Time `json:"timestamp"`
	SessionID    string    `json:"session_id"`
	User         string    `json:"user,omitempty"`
	Role         string    `json:"role,omitempty"`
	Tool         string    `json:"tool"`
	Source       string    `json:"source,omitempty"`
	Table        string    `json:"table,omitempty"`
	Params       any       `json:"params,omitempty"`
	RowsReturned int       `json:"rows_returned,omitempty"`
	DurationMs   int64     `json:"duration_ms"`
	SourceIP     string    `json:"source_ip,omitempty"`
	Error        string    `json:"error,omitempty"`
}

// Output defines where audit events are written.
type Output string

const (
	OutputStdout  Output = "stdout"
	OutputSQLite  Output = "sqlite"
	OutputSyslog  Output = "syslog"
	OutputWebhook Output = "webhook"
)

// Config configures the audit logger.
type Config struct {
	Enabled       bool   `yaml:"enabled"`
	Output        Output `yaml:"output"`
	RetentionDays int    `yaml:"retention_days"`
	WebhookURL    string `yaml:"webhook_url,omitempty"`
}

// Logger records audit events.
type Logger struct {
	config Config
	events []Event
	mu     sync.RWMutex
	logger *slog.Logger
}

// NewLogger creates a new audit logger.
func NewLogger(cfg Config, logger *slog.Logger) *Logger {
	return &Logger{
		config: cfg,
		events: make([]Event, 0, 1000),
		logger: logger,
	}
}

// Log records an audit event.
func (l *Logger) Log(event Event) {
	if !l.config.Enabled {
		return
	}

	event.Timestamp = time.Now()

	l.mu.Lock()
	l.events = append(l.events, event)
	// Keep only last 10000 events in memory
	if len(l.events) > 10000 {
		l.events = l.events[len(l.events)-10000:]
	}
	l.mu.Unlock()

	switch l.config.Output {
	case OutputStdout:
		l.writeStdout(event)
	case OutputSQLite:
		// TODO: Write to SQLite
		l.writeStdout(event)
	default:
		l.writeStdout(event)
	}
}

// Recent returns the most recent N audit events.
func (l *Logger) Recent(n int) []Event {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if n > len(l.events) {
		n = len(l.events)
	}
	if n == 0 {
		return nil
	}

	// Return most recent events (last N)
	result := make([]Event, n)
	copy(result, l.events[len(l.events)-n:])

	// Reverse so newest is first
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// Count returns the number of events logged today.
func (l *Logger) CountToday() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	today := time.Now().Truncate(24 * time.Hour)
	count := 0
	for i := len(l.events) - 1; i >= 0; i-- {
		if l.events[i].Timestamp.Before(today) {
			break
		}
		count++
	}
	return count
}

// AvgDurationMs returns the average query duration in the last N events.
func (l *Logger) AvgDurationMs(n int) float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.events) == 0 {
		return 0
	}

	if n > len(l.events) {
		n = len(l.events)
	}

	var total int64
	for i := len(l.events) - n; i < len(l.events); i++ {
		total += l.events[i].DurationMs
	}

	return float64(total) / float64(n)
}

func (l *Logger) writeStdout(event Event) {
	data, err := json.Marshal(event)
	if err != nil {
		l.logger.Error("failed to marshal audit event", "error", err)
		return
	}
	os.Stdout.Write(data)
	os.Stdout.Write([]byte("\n"))
}
