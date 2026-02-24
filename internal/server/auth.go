package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

const (
	apiKeyPrefix    = "conduit_sk_live_"
	apiKeyMinLength = 32
)

// GenerateAPIKey creates a new API key with the conduit prefix.
func GenerateAPIKey() (string, error) {
	bytes := make([]byte, 24)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return apiKeyPrefix + hex.EncodeToString(bytes), nil
}

// ValidateAPIKeyFormat checks if an API key meets minimum requirements.
func ValidateAPIKeyFormat(key string) error {
	if len(key) < apiKeyMinLength {
		return fmt.Errorf("API key must be at least %d characters (got %d)", apiKeyMinLength, len(key))
	}
	return nil
}
