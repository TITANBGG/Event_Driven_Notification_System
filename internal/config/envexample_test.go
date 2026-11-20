package config_test

import (
	"bufio"
	"os"
	"regexp"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/config"
)

// envExampleKey matches "KEY=value" and commented alternatives such as "# KEY=value".
var envExampleKey = regexp.MustCompile(`^#?\s*([A-Z][A-Z0-9_]*)=`)

// Every variable the services read must be documented in .env.example (D-3: zero-config, but every
// knob discoverable). The keys come from LoadFrom itself via a recording lookup, so a new setting
// cannot be added to config.go without this test failing until .env.example is updated.
func TestEnvExampleDocumentsEveryConfigKey(t *testing.T) {
	t.Parallel()

	read := make(map[string]bool)
	_, err := config.LoadFrom(func(key string) (string, bool) {
		read[key] = true
		return "", false
	})
	require.NoError(t, err)
	require.NotEmpty(t, read)

	f, err := os.Open("../../.env.example")
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	documented := make(map[string]bool)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if m := envExampleKey.FindStringSubmatch(sc.Text()); m != nil {
			documented[m[1]] = true
		}
	}
	require.NoError(t, sc.Err())

	var missing []string
	for key := range read {
		if !documented[key] {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	assert.Empty(t, missing, "config keys missing from .env.example")
}
