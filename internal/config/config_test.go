package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{"NT_API_URL": "https://h/api.php", "NT_API_KEY": "k", "REDIS_ADDR": "r:6379"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.PollInterval != 30*time.Second || c.Redis.Prefix != "nt" || c.Redis.Keep != 2 || c.SQLitePath != "data/nt.db" {
		t.Errorf("unexpected defaults: %+v", c)
	}
	if len(c.IndexFields) != 5 || c.IndexFields[0] != "cum_facturar" {
		t.Errorf("index fields = %v", c.IndexFields)
	}
}

func TestLoadReportsEveryProblem(t *testing.T) {
	_, err := Load(env(map[string]string{"POLL_INTERVAL": "soon", "REDIS_KEEP_VERSIONS": "0"}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"NT_API_URL", "NT_API_KEY", "REDIS_ADDR", "POLL_INTERVAL", "REDIS_KEEP_VERSIONS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}
