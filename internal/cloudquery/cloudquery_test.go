package cloudquery

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestParsePluginRefs(t *testing.T) {
	pin := strings.Repeat("a", 64)
	refs, err := ParsePluginRefs("cloudquery/xkcd@v1.6.1, hub:cloudquery/hackernews, gh=github:cloudquery/test@4.1.0, " +
		"community=github:acme/my-source@v0.2.0#sha256:" + pin + ", srv=grpc:127.0.0.1:7777, local=/opt/cq-source-foo-bar")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][3]string{}
	for _, r := range refs {
		got[r.Schema] = [3]string{r.Registry, r.Display(), r.Name}
	}
	want := map[string][3]string{
		"xkcd":       {"cloudquery", "cloudquery/xkcd@v1.6.1", "xkcd"},
		"hackernews": {"cloudquery", "cloudquery/hackernews", "hackernews"},
		"gh":         {"github", "github:cloudquery/test@v4.1.0", "test"},
		"community":  {"github", "github:acme/my-source@v0.2.0#sha256:" + pin, "my-source"},
		"srv":        {"grpc", "grpc:127.0.0.1:7777", "p_127_0_0_1"},
		"local":      {"local", "/opt/cq-source-foo-bar", "foo_bar"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refs:\n got %v\nwant %v", got, want)
	}
	for _, bad := range []string{"main=xkcd", "xkcd,xkcd", "Bad Name=xkcd", "xkcd#sha256:abc", "github:cloudquery/test", "x@notaversion",
		"docker:foo", "a/b/c", "grpc:nohost"} {
		if _, err := ParsePluginRefs(bad); err == nil {
			t.Errorf("ParsePluginRefs(%q) should fail", bad)
		}
	}
}

func TestSpecMerge(t *testing.T) {
	attach, err := parseAttachSpec("aws:\n  regions: [us-east-1]\n  accounts:\n    - id: '1'\n  nested: {a: 1, b: 2}\n")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := parseObject(`{"nested": {"b": 3}, "token": "x"}`, "secret")
	if err != nil {
		t.Fatal(err)
	}
	got, err := specJSON(deepMerge(attach["aws"], secret))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"accounts":[{"id":"1"}],"nested":{"a":1,"b":3},"regions":["us-east-1"],"token":"x"}`
	if string(got) != want {
		t.Fatalf("merged spec\n got %s\nwant %s", got, want)
	}
	for _, bad := range []string{"[1,2]", "{bad json", "x: 1"} {
		if _, err := parseAttachSpec(bad); err == nil {
			t.Errorf("parseAttachSpec(%q) should fail", bad)
		}
	}
}

// cqField is a field typed the way an Arrow IPC reader that does not know
// CloudQuery's extension types sees it: storage type + extension name.
func cqField(name string, storage arrow.DataType, ext string) arrow.Field {
	return arrow.Field{Name: name, Type: storage, Nullable: true,
		Metadata: arrow.NewMetadata([]string{mdExtensionName}, []string{ext})}
}

func TestConvertColumns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	ub := array.NewFixedSizeBinaryBuilder(mem, &arrow.FixedSizeBinaryType{ByteWidth: 16})
	ub.Append(bytes.Repeat([]byte{0xab}, 16))
	ub.AppendNull()
	uuids := ub.NewArray()
	ub.Release()
	defer uuids.Release()

	bb := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	bb.AppendValues([][]byte{[]byte(`{"a":1}`), nil}, []bool{true, false})
	jsons := bb.NewArray()
	bb.AppendValues([][]byte{{2, 0, 0, 0, 0, 1}, nil}, []bool{true, false})
	macs := bb.NewArray()
	bb.Release()
	defer jsons.Release()
	defer macs.Release()

	for _, tc := range []struct {
		field     arrow.Field
		arr       arrow.Array
		wantType  string
		wantFirst string
		sameBuf   bool
	}{
		{cqField("u", uuids.DataType(), "uuid"), uuids, "UUID", "abababab-abab-abab-abab-abababababab", true},
		{cqField("j", jsons.DataType(), "json"), jsons, "JSON", `{"a":1}`, true},
		{cqField("m", macs.DataType(), "mac"), macs, "VARCHAR", "02:00:00:00:00:01", false},
	} {
		out, err := convertColumn(mem, tc.arr, tc.field)
		if err != nil {
			t.Fatal(err)
		}
		if got := duckdbTypeName(out.DataType()); got != tc.wantType {
			t.Errorf("%s: type %s, want %s", tc.field.Name, got, tc.wantType)
		}
		if !arrow.TypeEqual(out.DataType(), mapType(tc.field.Type, tc.field.Metadata)) {
			t.Errorf("%s: array type %s differs from the mapped type", tc.field.Name, out.DataType())
		}
		if got := out.ValueStr(0); got != tc.wantFirst {
			t.Errorf("%s: value %q, want %q", tc.field.Name, got, tc.wantFirst)
		}
		if !out.IsNull(1) {
			t.Errorf("%s: NULL lost", tc.field.Name)
		}
		last := len(tc.arr.Data().Buffers()) - 1
		same := out.Data().Buffers()[last] == tc.arr.Data().Buffers()[last]
		if same != tc.sameBuf {
			t.Errorf("%s: shares buffers = %v, want %v", tc.field.Name, same, tc.sameBuf)
		}
		out.Release()
	}

	// list<uuid>: the extension sits on the element field.
	elem := cqField("item", uuids.DataType(), "uuid")
	lt := arrow.ListOfField(elem)
	lb := array.NewListBuilder(mem, uuids.DataType())
	lb.Append(true)
	lb.ValueBuilder().(*array.FixedSizeBinaryBuilder).Append(bytes.Repeat([]byte{1}, 16))
	list := lb.NewArray()
	lb.Release()
	defer list.Release()
	data := array.NewData(lt, list.Len(), list.Data().Buffers(), list.Data().Children(), list.NullN(), 0)
	typed := array.MakeFromData(data)
	data.Release()
	defer typed.Release()
	out, err := convertColumn(mem, typed, arrow.Field{Name: "l", Type: lt, Nullable: true})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got := duckdbTypeName(out.DataType()); got != "UUID[]" {
		t.Fatalf("list<uuid> maps to %s", got)
	}

	// A plain column is passed through untouched.
	ib := array.NewInt64Builder(mem)
	ib.Append(7)
	ints := ib.NewArray()
	ib.Release()
	defer ints.Release()
	same, err := convertColumn(mem, ints, arrow.Field{Name: "i", Type: arrow.PrimitiveTypes.Int64})
	if err != nil {
		t.Fatal(err)
	}
	if same != ints {
		t.Fatal("an int64 column was copied")
	}
	same.Release()
}

// zipOf returns a zip holding one file.
func zipOf(t *testing.T, name string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func hexSHA(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestFetchGitHubVerifiesAndCaches(t *testing.T) {
	plat := runtime.GOOS + "_" + runtime.GOARCH
	good := zipOf(t, "plugins/source/demo"+exeSuffix(), []byte("#!/bin/sh\necho demo\n"))
	var downloads atomic.Int32
	sums := map[string]string{"demo": hexSHA(good), "evil": strings.Repeat("0", 64)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for name, sum := range sums {
			base := fmt.Sprintf("/cloudquery/cloudquery/releases/download/plugins-source-%s-v1.0.0/", name)
			switch r.URL.Path {
			case base + "checksums.txt":
				fmt.Fprintf(w, "%s  %s_%s.zip\n", sum, name, plat)
				return
			case base + name + "_" + plat + ".zip":
				downloads.Add(1)
				_, _ = w.Write(good)
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	cfg := fetchConfig{CacheDir: t.TempDir(), GitHubURL: srv.URL}
	ctx := context.Background()

	ref, _ := ParsePluginRef("github:cloudquery/demo@v1.0.0")
	fp, err := FetchPlugin(ctx, ref, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if fp.FromCache || fp.ArchiveSHA256 != hexSHA(good) {
		t.Fatalf("first fetch: %+v", fp)
	}
	if b, _ := os.ReadFile(fp.Binary); string(b) != "#!/bin/sh\necho demo\n" {
		t.Fatalf("unpacked binary: %q", b)
	}
	fp, err = FetchPlugin(ctx, ref, cfg)
	if err != nil || !fp.FromCache || downloads.Load() != 1 {
		t.Fatalf("second fetch should come from the cache: %+v %v (downloads %d)", fp, err, downloads.Load())
	}

	// A cached binary that changed on disk is not trusted: it is re-fetched.
	if err := os.WriteFile(fp.Binary, []byte("tampered"), 0o755); err != nil {
		t.Fatal(err)
	}
	fp, err = FetchPlugin(ctx, ref, cfg)
	if err != nil || fp.FromCache || downloads.Load() != 2 {
		t.Fatalf("tampered cache should be re-fetched: %+v %v", fp, err)
	}

	evil, _ := ParsePluginRef("github:cloudquery/evil@v1.0.0")
	if _, err := FetchPlugin(ctx, evil, cfg); err == nil || !strings.Contains(err.Error(), "published checksum") {
		t.Fatalf("checksum mismatch not refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.entryDir(evil, "v1.0.0"), "plugin"+exeSuffix())); err == nil {
		t.Fatal("a refused archive was unpacked")
	}
}

func TestFetchHubNeedsKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	ref, _ := ParsePluginRef("cloudquery/xkcd@v1.6.1")
	_, err := FetchPlugin(context.Background(), ref, fetchConfig{CacheDir: t.TempDir(), HubURL: srv.URL})
	if err == nil || !strings.Contains(err.Error(), "need an API key") {
		t.Fatalf("anonymous Hub download: %v", err)
	}
	_, err = FetchPlugin(context.Background(), ref, fetchConfig{CacheDir: t.TempDir(), HubURL: srv.URL, APIKey: "k"})
	if err == nil || !strings.Contains(err.Error(), "has no cloudquery/xkcd") {
		t.Fatalf("keyed Hub download of a missing plugin: %v", err)
	}
}

func TestPluginEnvDropsCloudQueryLogin(t *testing.T) {
	t.Setenv("CLOUDQUERY_API_KEY", "user-key")
	t.Setenv("XDG_DATA_HOME", "/home/user/.local/share")
	env := strings.Join(pluginEnv([]string{"CLOUDQUERY_API_KEY=from-secret"}), "\n")
	if strings.Contains(env, "user-key") || strings.Contains(env, "/home/user/.local/share") {
		t.Fatalf("plugin environment leaks the user's CloudQuery login:\n%s", env)
	}
	if !strings.Contains(env, "CLOUDQUERY_API_KEY=from-secret") {
		t.Fatal("the secret's API key is missing")
	}
}
