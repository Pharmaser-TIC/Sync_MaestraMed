package httpapi

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConsumers(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "consumers.json")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConsumersReadsNamesHashesAndPricePermission(t *testing.T) {
	h1, h2 := hashOf("a"), strings.ToUpper(hashOf("b"))
	got, err := LoadConsumers(writeConsumers(t, `{"consumers":[
		{"name":"facturacion","key_sha256":"`+h1+`","precios":true},
		{"name":"inventario","key_sha256":"`+h2+`","precios":false}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "facturacion" || !got[0].Precios || got[1].Precios {
		t.Fatalf("consumers = %+v", got)
	}
	if got[1].KeySHA256 != strings.ToLower(h2) {
		t.Errorf("hashes are compared in lower case, got %q", got[1].KeySHA256)
	}
}

func TestLoadConsumersRejectsAnythingAmbiguous(t *testing.T) {
	h := hashOf("a")
	h2 := hashOf("b")
	for name, tc := range map[string]struct{ json, want string }{
		"price decision missing": {`{"consumers":[{"name":"x","key_sha256":"` + h + `"}]}`, "precios"},
		"typo in a field":        {`{"consumers":[{"name":"x","key_sha256":"` + h + `","precio":false}]}`, "precio"},
		"empty name":             {`{"consumers":[{"name":" ","key_sha256":"` + h + `","precios":true}]}`, "name"},
		"hash too short":         {`{"consumers":[{"name":"x","key_sha256":"abc","precios":true}]}`, "key_sha256"},
		"hash not hex":           {`{"consumers":[{"name":"x","key_sha256":"` + strings.Repeat("z", 64) + `","precios":true}]}`, "key_sha256"},
		"duplicate name":         {`{"consumers":[{"name":"x","key_sha256":"` + h + `","precios":true},{"name":"x","key_sha256":"` + h2 + `","precios":true}]}`, "duplicate"},
		"duplicate key":          {`{"consumers":[{"name":"x","key_sha256":"` + h + `","precios":true},{"name":"y","key_sha256":"` + h + `","precios":false}]}`, "duplicate"},
		"no consumers":           {`{"consumers":[]}`, "at least one"},
		"not json":               {`nope`, "consumers"},
		"trailing data":          {`{"consumers":[{"name":"x","key_sha256":"` + h + `","precios":true}]} {"consumers":[]}`, "trailing"},
		"a plain key by mistake": {`{"consumers":[{"name":"x","key":"secret","key_sha256":"` + h + `","precios":true}]}`, "key"},
	} {
		_, err := LoadConsumers(writeConsumers(t, tc.json))
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.want) {
			t.Errorf("%s: want an error mentioning %q, got %v", name, tc.want, err)
		}
	}
	if _, err := LoadConsumers(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("a missing file must be an error: a server without consumers must not start")
	}
}
