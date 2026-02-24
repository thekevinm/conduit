package connector

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
)

// ConnectorFactory creates a new Connector instance.
type ConnectorFactory func() Connector

var (
	mu       sync.RWMutex
	registry = map[string]ConnectorFactory{}
)

// Register adds a connector factory for the given driver name.
func Register(driver string, factory ConnectorFactory) {
	mu.Lock()
	defer mu.Unlock()
	registry[driver] = factory
}

// New creates a new Connector for the given driver.
func New(driver string) (Connector, error) {
	mu.RLock()
	defer mu.RUnlock()
	factory, ok := registry[driver]
	if !ok {
		return nil, fmt.Errorf("unsupported database driver: %q (supported: %s)",
			driver, strings.Join(SupportedDrivers(), ", "))
	}
	return factory(), nil
}

// SupportedDrivers returns a sorted list of registered driver names.
func SupportedDrivers() []string {
	mu.RLock()
	defer mu.RUnlock()
	drivers := make([]string, 0, len(registry))
	for d := range registry {
		drivers = append(drivers, d)
	}
	sort.Strings(drivers)
	return drivers
}

// ParseDSN extracts the driver name from a DSN URI scheme.
func ParseDSN(dsn string) (driver string, err error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid DSN: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "postgres", "postgresql":
		return "postgres", nil
	case "mysql":
		return "mysql", nil
	case "sqlserver", "mssql":
		return "mssql", nil
	case "sqlite", "sqlite3":
		return "sqlite", nil
	case "oracle":
		return "oracle", nil
	case "snowflake":
		return "snowflake", nil
	default:
		return "", fmt.Errorf("unknown database scheme: %q (supported: postgres, mysql, sqlserver, sqlite, oracle, snowflake)", scheme)
	}
}

// SanitizeDSN masks the password component of a DSN for safe logging.
func SanitizeDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "[invalid DSN]"
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "****")
	}
	return u.String()
}
