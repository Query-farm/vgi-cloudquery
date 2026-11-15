package cloudquery

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/cloudquery/plugin-pb-go/pb/plugin/v3"
)

// fixturePlugin returns the built test/fixture-plugin binary, or skips.
func fixturePlugin(t *testing.T) string {
	t.Helper()
	p := os.Getenv("FIXTURE_PLUGIN")
	if p == "" {
		p, _ = filepath.Abs("../../bin/cq-source-fixture")
	}
	if _, err := os.Stat(p); err != nil {
		t.Skipf("fixture plugin not built (make build): %v", err)
	}
	return p
}

func TestSyncChildTable(t *testing.T) {
	bin := fixturePlugin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ref, _ := ParsePluginRef(bin)
	ref.Schema = "fixture"
	ln := launch{Ref: ref, Binary: bin, Spec: []byte(`{"item_count":3}`)}
	l, err := host.acquire(ctx, ln)
	if err != nil {
		t.Fatal(err)
	}
	defer l.release()
	stream, err := l.p.client.Sync(ctx, &pb.Sync_Request{Tables: []string{"fixture_item_event"}, SkipDependentTables: true, DeterministicCqId: true, WithErrorMessages: true})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch m := resp.GetMessage().(type) {
		case *pb.Sync_Response_Insert:
			rec, err := pb.NewRecordFromBytes(m.Insert.GetRecord())
			if err != nil {
				t.Fatal(err)
			}
			counts[tableName(rec.Schema())] += rec.NumRows()
		case *pb.Sync_Response_Error:
			t.Logf("error message: %v", m.Error)
		default:
			t.Logf("message %T", m)
		}
	}
	t.Logf("counts %v", counts)
	if counts["fixture_item_event"] != 6 {
		t.Fatalf("want 6 events, got %v", counts)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	StopPlugins()
	os.Exit(code)
}

func TestGRPCRefSharesOneServer(t *testing.T) {
	bin := fixturePlugin(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	cmd := exec.Command(bin, "serve", "--network", "tcp", "--address", addr, "--log-level", "warn", "--no-sentry")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	ref, err := ParsePluginRef("grpc:" + addr)
	if err != nil {
		t.Fatal(err)
	}
	ref.Schema = "remote"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := host.info(ctx, launch{Ref: ref, Spec: []byte(`{"item_count": 4}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := info.table("fixture_item_event"); !ok || info.Name != "fixture" {
		t.Fatalf("grpc plugin info: %+v", info.tableNames())
	}
}
