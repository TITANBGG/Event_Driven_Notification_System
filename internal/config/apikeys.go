package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// API key scopes. admin implies every other scope; read and write are independent so a
// producer-only key cannot list other callers' notifications.
const (
	ScopeRead  = "read"
	ScopeWrite = "write"
	ScopeAdmin = "admin"
)

const (
	minAPIKeySecretLen = 16
	maxAPIKeySecretLen = 256
)

var apiKeyNamePattern = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

// APIKey is one configured client credential from API_KEYS ("name:secret:scope1|scope2").
// The secret never leaves this struct through String or GoString, so a %v of the config or a
// validation error cannot leak it into logs.
type APIKey struct {
	Name   string
	Secret string
	Scopes []string
}

// String implements fmt.Stringer with the secret redacted.
func (k APIKey) String() string {
	return fmt.Sprintf("APIKey{Name:%s Secret:[redacted] Scopes:%s}", k.Name, strings.Join(k.Scopes, "|"))
}

// GoString implements fmt.GoStringer with the secret redacted (covers %#v).
func (k APIKey) GoString() string { return k.String() }

// parseAPIKeys splits the raw API_KEYS value into entries. Error messages name the entry by
// position and key name only, never by its secret.
func parseAPIKeys(raw string) ([]APIKey, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var (
		keys []APIKey
		errs []error
	)
	for i, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			errs = append(errs, fmt.Errorf("API_KEYS entry %d must have the form name:secret:scope1|scope2", i))
			continue
		}
		var scopes []string
		for _, s := range strings.Split(parts[2], "|") {
			if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
				scopes = append(scopes, s)
			}
		}
		keys = append(keys, APIKey{Name: strings.TrimSpace(parts[0]), Secret: parts[1], Scopes: scopes})
	}
	return keys, errors.Join(errs...)
}

// LoadAPIKeysFile reads and validates an API_KEYS_FILE: the same name:secret:scopes entries as
// API_KEYS, separated by commas or newlines; blank lines and lines starting with # are ignored.
// It is used at startup and again on SIGHUP to rotate keys without a restart.
func LoadAPIKeysFile(path string) ([]APIKey, error) {
	keys, err := readAPIKeysFile(path)
	if err != nil {
		return nil, err
	}
	if err := validateAPIKeys(keys); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("API_KEYS_FILE %s contains no keys", path)
	}
	return keys, nil
}

func readAPIKeysFile(path string) ([]APIKey, error) {
	// #nosec G304 -- the path is operator configuration (API_KEYS_FILE), not request input.
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading API_KEYS_FILE: %w", err)
	}
	var entries []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		entries = append(entries, line)
	}
	return parseAPIKeys(strings.Join(entries, ","))
}

func validateAPIKeys(keys []APIKey) error {
	var errs []error
	seenNames := make(map[string]bool, len(keys))
	seenSecrets := make(map[string]bool, len(keys))
	for i, k := range keys {
		label := fmt.Sprintf("API_KEYS entry %d (%q)", i, k.Name)
		if !apiKeyNamePattern.MatchString(k.Name) {
			errs = append(errs, fmt.Errorf("%s: name must match %s", label, apiKeyNamePattern))
		}
		if seenNames[k.Name] {
			errs = append(errs, fmt.Errorf("%s: duplicate key name", label))
		}
		seenNames[k.Name] = true

		if n := len(k.Secret); n < minAPIKeySecretLen || n > maxAPIKeySecretLen {
			errs = append(errs, fmt.Errorf("%s: secret must be %d..%d characters", label, minAPIKeySecretLen, maxAPIKeySecretLen))
		} else if !isTokenSafe(k.Secret) {
			errs = append(errs, fmt.Errorf("%s: secret may only contain letters, digits and !#$%%&'*+-.^_`|~", label))
		}
		// Two names sharing a secret would make the authenticated principal ambiguous.
		if seenSecrets[k.Secret] {
			errs = append(errs, fmt.Errorf("%s: secret is reused by another key", label))
		}
		seenSecrets[k.Secret] = true

		if len(k.Scopes) == 0 {
			errs = append(errs, fmt.Errorf("%s: at least one scope (read, write, admin) is required", label))
		}
		for _, s := range k.Scopes {
			switch s {
			case ScopeRead, ScopeWrite, ScopeAdmin:
			default:
				errs = append(errs, fmt.Errorf("%s: unknown scope %q (want read, write or admin)", label, s))
			}
		}
	}
	return errors.Join(errs...)
}

// isTokenSafe restricts secrets to RFC 7230 tchar so a secret is valid both in an Authorization
// header and as a WebSocket subprotocol (browsers reject `new WebSocket(url, [p])` when p is not a
// token), and can never contain the ':' and ',' separators of API_KEYS.
func isTokenSafe(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

func apiKeyNames(keys []APIKey) []string {
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = k.Name
	}
	return names
}
