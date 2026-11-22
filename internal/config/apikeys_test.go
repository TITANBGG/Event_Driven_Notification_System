package config_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/config"
)

const goodSecret = "s3cr3t-value-0123456789"

func TestLoadAPIKeys(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(mapLookup(nil))
	require.NoError(t, err)
	assert.Empty(t, cfg.APIKeys, "auth is disabled unless API_KEYS is set")

	cfg, err = config.LoadFrom(mapLookup(map[string]string{
		"API_KEYS": " ops:" + goodSecret + ":admin , shop-1:another-secret-012345:READ|write ,",
	}))
	require.NoError(t, err)
	require.Len(t, cfg.APIKeys, 2)
	assert.Equal(t, config.APIKey{Name: "ops", Secret: goodSecret, Scopes: []string{"admin"}}, cfg.APIKeys[0])
	assert.Equal(t, []string{"read", "write"}, cfg.APIKeys[1].Scopes, "scopes are normalized to lowercase")
}

func TestLoadAPIKeysInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{"missing scope part", "ops:" + goodSecret, "must have the form name:secret:scope1|scope2"},
		{"too many parts", "ops:" + goodSecret + ":admin:extra", "must have the form"},
		{"empty scopes", "ops:" + goodSecret + ":", "at least one scope"},
		{"unknown scope", "ops:" + goodSecret + ":root", `unknown scope "root"`},
		{"bad name", "Ops Team:" + goodSecret + ":read", "name must match"},
		{"short secret", "ops:short:read", "secret must be 16..256 characters"},
		{"secret with space", "ops:has space 0123456789:read", "secret may only contain"},
		{"secret with slash", "ops:has/slash/0123456789:read", "secret may only contain"},
		{"duplicate name", "ops:" + goodSecret + ":read,ops:other-secret-0123456:read", "duplicate key name"},
		{"reused secret", "a:" + goodSecret + ":read,b:" + goodSecret + ":write", "secret is reused"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.LoadFrom(mapLookup(map[string]string{"API_KEYS": tc.raw}))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.NotContains(t, err.Error(), goodSecret, "config errors must never print a secret")
		})
	}
}

func TestAPIKeySecretNeverFormatted(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(mapLookup(map[string]string{"API_KEYS": "ops:" + goodSecret + ":admin"}))
	require.NoError(t, err)

	for _, s := range []string{
		fmt.Sprintf("%v", cfg.APIKeys),
		fmt.Sprintf("%+v", cfg.APIKeys[0]),
		fmt.Sprintf("%#v", cfg.APIKeys[0]),
		cfg.APIKeys[0].String(),
	} {
		assert.NotContains(t, s, goodSecret)
		assert.Contains(t, s, "ops")
	}

	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("config loaded", slog.Any("config", cfg))
	assert.NotContains(t, buf.String(), goodSecret)
	assert.Contains(t, buf.String(), `"api_key_names":["ops"]`)
	assert.Contains(t, buf.String(), `"api_auth_enabled":true`)
}

func TestAPIKeysFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.WriteFile(path, []byte("# rotated 2026-11-22\nops:"+goodSecret+":admin\n\nshop:another-secret-012345:write|read\n"), 0o600))

	cfg, err := config.LoadFrom(mapLookup(map[string]string{"API_KEYS_FILE": path}))
	require.NoError(t, err)
	require.Len(t, cfg.APIKeys, 2)
	assert.Equal(t, "shop", cfg.APIKeys[1].Name)

	keys, err := config.LoadAPIKeysFile(path)
	require.NoError(t, err)
	assert.Len(t, keys, 2)

	_, err = config.LoadFrom(mapLookup(map[string]string{"API_KEYS_FILE": path, "API_KEYS": "x:" + goodSecret + ":read"}))
	assert.ErrorContains(t, err, "not both")

	empty := filepath.Join(t.TempDir(), "empty")
	require.NoError(t, os.WriteFile(empty, []byte("# nothing yet\n"), 0o600))
	_, err = config.LoadAPIKeysFile(empty)
	assert.ErrorContains(t, err, "contains no keys")

	bad := filepath.Join(t.TempDir(), "bad")
	require.NoError(t, os.WriteFile(bad, []byte("ops:short:admin\n"), 0o600))
	_, err = config.LoadAPIKeysFile(bad)
	assert.ErrorContains(t, err, "secret must be")
	assert.NotContains(t, err.Error(), "short")

	_, err = config.LoadFrom(mapLookup(map[string]string{"API_KEYS_FILE": filepath.Join(t.TempDir(), "missing")}))
	assert.ErrorContains(t, err, "reading API_KEYS_FILE")
}
