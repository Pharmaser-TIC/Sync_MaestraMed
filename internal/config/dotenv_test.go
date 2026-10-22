package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeEnvFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "settings.env")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDotEnvParsesAndRealEnvironmentWins(t *testing.T) {
	t.Setenv("NT_TEST_KEEP", "from-environment")
	os.Unsetenv("NT_TEST_A")
	os.Unsetenv("NT_TEST_B")
	os.Unsetenv("NT_TEST_C")
	os.Unsetenv("NT_TEST_D")
	os.Unsetenv("NT_TEST_E")
	t.Cleanup(func() {
		for _, k := range []string{"NT_TEST_A", "NT_TEST_B", "NT_TEST_C", "NT_TEST_D", "NT_TEST_E"} {
			os.Unsetenv(k)
		}
	})
	p := writeEnvFile(t, "# a comment\n\nNT_TEST_A=plain\nexport NT_TEST_B = spaced value  \nNT_TEST_C=\"quoted # not a comment\"\nNT_TEST_D='single'\nNT_TEST_E=value # trailing comment\nNT_TEST_KEEP=from-file\n")

	loaded, err := LoadDotEnv(p)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded {
		t.Fatal("expected the file to be reported as loaded")
	}
	for k, want := range map[string]string{
		"NT_TEST_A":    "plain",
		"NT_TEST_B":    "spaced value",
		"NT_TEST_C":    "quoted # not a comment",
		"NT_TEST_D":    "single",
		"NT_TEST_E":    "value",
		"NT_TEST_KEEP": "from-environment",
	} {
		if got := os.Getenv(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
}

func TestLoadDotEnvMissingFileIsNotAnError(t *testing.T) {
	loaded, err := LoadDotEnv(filepath.Join(t.TempDir(), "absent.env"))
	if err != nil || loaded {
		t.Fatalf("loaded=%v err=%v", loaded, err)
	}
}

func TestLoadDotEnvRejectsMalformedLineWithoutEchoingItsValue(t *testing.T) {
	p := writeEnvFile(t, "NT_API_KEY=super-secret-value\nthis line has no equals sign\n")
	os.Unsetenv("NT_API_KEY")
	t.Cleanup(func() { os.Unsetenv("NT_API_KEY") })
	_, err := LoadDotEnv(p)
	if err == nil {
		t.Fatal("expected an error for a line without '='")
	}
	if msg := err.Error(); !containsAll(msg, "line 2") || containsAll(msg, "super-secret-value") {
		t.Fatalf("error must name the line but never a value: %v", err)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}
