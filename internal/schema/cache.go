// Package schema provides schema introspection models, caching, digest
// generation, and PII detection for the Conduit MCP server.
package schema

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// SchemaProvider is the interface that connectors must satisfy for schema
// introspection. It is intentionally minimal to avoid importing the full
// connector package (breaking import cycles).
type SchemaProvider interface {
	ListTables(ctx context.Context) ([]TableSummary, error)
	DescribeTable(ctx context.Context, tableName string) (*TableDetail, error)
}

// CacheConfig controls the behavior of the schema cache.
type CacheConfig struct {
	// TTL is how long a cached entry stays valid before it is considered stale.
	// Default: 5 minutes.
	TTL time.Duration

	// RefreshInterval is how often the background goroutine proactively
	// refreshes all cached tables. Default: 5 minutes.
	RefreshInterval time.Duration

	// MaxTables caps the number of tables whose details are cached. Zero means
	// unlimited.
	MaxTables int
}

// DefaultCacheConfig returns sensible defaults for the schema cache.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		TTL:             5 * time.Minute,
		RefreshInterval: 5 * time.Minute,
		MaxTables:       0,
	}
}

// cacheEntry holds one table's cached detail plus its freshness timestamp.
type cacheEntry struct {
	detail    *TableDetail
	fetchedAt time.Time
}

// Cache provides an in-memory cache for database schema metadata with TTL-based
// expiry and optional background refresh. It is safe for concurrent use.
type Cache struct {
	mu       sync.RWMutex
	provider SchemaProvider
	cfg      CacheConfig
	logger   *slog.Logger

	// tables stores the table list (lightweight).
	tables   []TableSummary
	tablesFetched time.Time

	// details stores per-table detail keyed by table name.
	details map[string]*cacheEntry

	// Background refresh lifecycle.
	stopCh chan struct{}
	done   chan struct{}
}

// NewCache creates a schema cache backed by the given provider.
func NewCache(provider SchemaProvider, cfg CacheConfig, logger *slog.Logger) *Cache {
	if cfg.TTL == 0 {
		cfg.TTL = 5 * time.Minute
	}
	if cfg.RefreshInterval == 0 {
		cfg.RefreshInterval = 5 * time.Minute
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Cache{
		provider: provider,
		cfg:      cfg,
		logger:   logger,
		details:  make(map[string]*cacheEntry),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start begins background refresh. Call Stop to clean up.
func (c *Cache) Start(ctx context.Context) {
	go c.refreshLoop(ctx)
}

// Stop halts the background refresh goroutine and waits for it to exit.
func (c *Cache) Stop() {
	close(c.stopCh)
	<-c.done
}

// ListTables returns the cached table list, refreshing if stale or empty.
func (c *Cache) ListTables(ctx context.Context) ([]TableSummary, error) {
	c.mu.RLock()
	if c.tables != nil && time.Since(c.tablesFetched) < c.cfg.TTL {
		tables := make([]TableSummary, len(c.tables))
		copy(tables, c.tables)
		c.mu.RUnlock()
		return tables, nil
	}
	c.mu.RUnlock()

	return c.refreshTableList(ctx)
}

// DescribeTable returns cached table detail, refreshing if stale or missing.
func (c *Cache) DescribeTable(ctx context.Context, tableName string) (*TableDetail, error) {
	c.mu.RLock()
	if entry, ok := c.details[tableName]; ok && time.Since(entry.fetchedAt) < c.cfg.TTL {
		detail := *entry.detail // shallow copy
		c.mu.RUnlock()
		return &detail, nil
	}
	c.mu.RUnlock()

	return c.refreshTableDetail(ctx, tableName)
}

// Invalidate removes a specific table from the cache, forcing a fresh fetch
// on the next access.
func (c *Cache) Invalidate(tableName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.details, tableName)
}

// InvalidateAll clears the entire cache.
func (c *Cache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables = nil
	c.tablesFetched = time.Time{}
	c.details = make(map[string]*cacheEntry)
}

// Refresh forces an immediate refresh of the table list and all cached details.
func (c *Cache) Refresh(ctx context.Context) error {
	tables, err := c.refreshTableList(ctx)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if _, err := c.refreshTableDetail(ctx, t.Name); err != nil {
			c.logger.Warn("failed to refresh table detail",
				slog.String("table", t.Name),
				slog.String("error", err.Error()))
		}
	}
	return nil
}

// Stats returns cache statistics for diagnostics.
func (c *Cache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		TableCount:  len(c.tables),
		DetailCount: len(c.details),
		TableListAge: time.Since(c.tablesFetched),
	}
}

// CacheStats holds diagnostic information about the cache state.
type CacheStats struct {
	TableCount   int           `json:"table_count"`
	DetailCount  int           `json:"detail_count"`
	TableListAge time.Duration `json:"table_list_age"`
}

// refreshTableList fetches fresh table list from the provider.
func (c *Cache) refreshTableList(ctx context.Context) ([]TableSummary, error) {
	tables, err := c.provider.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.tables = tables
	c.tablesFetched = time.Now()
	c.mu.Unlock()

	c.logger.Debug("refreshed table list", slog.Int("count", len(tables)))
	return tables, nil
}

// refreshTableDetail fetches fresh detail for one table.
func (c *Cache) refreshTableDetail(ctx context.Context, tableName string) (*TableDetail, error) {
	detail, err := c.provider.DescribeTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.cfg.MaxTables > 0 && len(c.details) >= c.cfg.MaxTables {
		// Evict oldest entry when at capacity.
		c.evictOldestLocked()
	}
	c.details[tableName] = &cacheEntry{
		detail:    detail,
		fetchedAt: time.Now(),
	}
	c.mu.Unlock()

	c.logger.Debug("refreshed table detail",
		slog.String("table", tableName),
		slog.Int("columns", len(detail.Columns)))
	return detail, nil
}

// evictOldestLocked removes the oldest cache entry. Caller must hold c.mu.
func (c *Cache) evictOldestLocked() {
	var oldestKey string
	var oldestTime time.Time
	for k, v := range c.details {
		if oldestKey == "" || v.fetchedAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.fetchedAt
		}
	}
	if oldestKey != "" {
		delete(c.details, oldestKey)
		c.logger.Debug("evicted cache entry", slog.String("table", oldestKey))
	}
}

// refreshLoop runs periodic refresh until stopped.
func (c *Cache) refreshLoop(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(c.cfg.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Refresh(ctx); err != nil {
				c.logger.Warn("background schema refresh failed",
					slog.String("error", err.Error()))
			}
		}
	}
}
