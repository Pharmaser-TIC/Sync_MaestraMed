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

func TestSearchFieldsAreOffByDefaultAndParsed(t *testing.T) {
	base := map[string]string{"NT_API_URL": "https://h/api.php", "NT_API_KEY": "k", "REDIS_ADDR": "r:6379"}
	c, err := Load(env(base))
	if err != nil || len(c.SearchFields) != 0 {
		t.Fatalf("default: fields=%v err=%v", c.SearchFields, err)
	}
	base["NT_SEARCH_FIELDS"] = " nombre, descripcion_2 ,,"
	c, err = Load(env(base))
	if err != nil || len(c.SearchFields) != 2 || c.SearchFields[0] != "nombre" || c.SearchFields[1] != "descripcion_2" {
		t.Fatalf("parsed: fields=%v err=%v", c.SearchFields, err)
	}
}

func TestSearchFieldsRejectNamesThatAreNotPlainKeys(t *testing.T) {
	_, err := Load(env(map[string]string{"NT_API_URL": "https://h/api.php", "NT_API_KEY": "k", "REDIS_ADDR": "r:6379", "NT_SEARCH_FIELDS": "nombre,a.b"}))
	if err == nil || !strings.Contains(err.Error(), "NT_SEARCH_FIELDS") {
		t.Fatalf("expected an NT_SEARCH_FIELDS error, got %v", err)
	}
}

func TestChangesMaxLenDefaultsParsesAndRejectsBadValues(t *testing.T) {
	base := map[string]string{"NT_API_URL": "https://h/api.php", "NT_API_KEY": "k", "REDIS_ADDR": "r:6379"}
	c, err := Load(env(base))
	if err != nil || c.Redis.ChangesMaxLen != 100000 {
		t.Fatalf("default: %d, %v", c.Redis.ChangesMaxLen, err)
	}
	base["NT_CHANGES_MAXLEN"] = "0"
	if c, err = Load(env(base)); err != nil || c.Redis.ChangesMaxLen != 0 {
		t.Fatalf("zero: %d, %v", c.Redis.ChangesMaxLen, err)
	}
	for _, bad := range []string{"-1", "many"} {
		base["NT_CHANGES_MAXLEN"] = bad
		if _, err := Load(env(base)); err == nil || !strings.Contains(err.Error(), "NT_CHANGES_MAXLEN") {
			t.Errorf("%q: expected an NT_CHANGES_MAXLEN error, got %v", bad, err)
		}
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
