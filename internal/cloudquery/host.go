// Hosting CloudQuery plugin processes.
//
// A CloudQuery plugin is a standalone executable that serves the plugin
// protocol v3 gRPC service (cloudquery.plugin.v3.Plugin) when run as
//
//	<binary> serve --network unix --address <socket> --log-format json
//
// This file plays the part of the CloudQuery CLI's managed plugin client:
//
//	start   spawn the binary on a private unix socket and dial it
//	init    Init(spec JSON, no_connection) once per process
//	tables  GetTables(["*"]) -> Arrow schemas, one per table (relations flattened)
//	sync    Sync(tables, skip_dependent_tables, deterministic_cq_id) -> stream
//
// A plugin serves one Sync at a time (plugin-sdk refuses a second with
// "plugin already in use"), so a process is leased exclusively for the
// length of a call. Processes are pooled per (plugin, schema, spec, env),
// stopped after sitting idle, when the pool is full, or when the worker
// exits. Plugin log lines go to the worker's stderr, never its stdout.

package cloudquery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	pb "github.com/cloudquery/plugin-pb-go/pb/plugin/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// maxPluginProcesses bounds how many plugin processes run at once.
	maxPluginProcesses = 16
	// pluginIdleTimeout stops a plugin nobody has used for this long.
	pluginIdleTimeout = 10 * time.Minute
	// pluginStartTimeout bounds process start + socket connect.
	pluginStartTimeout = 60 * time.Second
	// pluginInitTimeout bounds Init + GetTables.
	pluginInitTimeout = 5 * time.Minute
	// maxGRPCMessage matches the CloudQuery CLI's limit.
	maxGRPCMessage = 50 * 1024 * 1024
)

// pluginInfo is what a started plugin reports about itself.
type pluginInfo struct {
	Name       string
	Version    string
	SpecSchema string
	Tables     []*arrow.Schema
	byName     map[string]*arrow.Schema
}

func (pi *pluginInfo) table(name string) (*arrow.Schema, bool) {
	s, ok := pi.byName[name]
	return s, ok
}

func (pi *pluginInfo) tableNames() []string {
	names := make([]string, 0, len(pi.byName))
	for n := range pi.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// pluginProc is one running, initialised plugin.
type pluginProc struct {
	key      string
	cmd      *exec.Cmd
	lifeline *os.File // supervisor's stdin; closing it stops the plugin
	exited   chan struct{}
	sockDir  string
	conn     *grpc.ClientConn
	client   pb.PluginClient
	info     *pluginInfo
	busy     bool
	lastUsed time.Time
}

func (p *pluginProc) alive() bool {
	if p.cmd == nil {
		return true
	}
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

func (p *pluginProc) stop() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	if p.lifeline != nil {
		_ = p.lifeline.Close()
		select {
		case <-p.exited:
		case <-time.After(5 * time.Second):
		}
	}
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
		<-p.exited
	}
	if p.sockDir != "" {
		_ = os.RemoveAll(p.sockDir)
	}
}

type pluginHost struct {
	mu      sync.Mutex
	procs   map[*pluginProc]bool
	infos   map[string]*pluginInfo // last-known schema per key, for idle lookups
	reaping bool
	cond    *sync.Cond
}

var host = newPluginHost()

func newPluginHost() *pluginHost {
	h := &pluginHost{procs: map[*pluginProc]bool{}, infos: map[string]*pluginInfo{}}
	h.cond = sync.NewCond(&h.mu)
	return h
}

// launch is everything that identifies one plugin configuration.
type launch struct {
	Ref          PluginRef
	Binary       string // local binary; empty for grpc refs
	Spec         []byte // canonical JSON
	NoConnection bool
	Env          []string // extra environment (e.g. CLOUDQUERY_API_KEY), never logged
}

func (l launch) key() string {
	h := sha256.New()
	for _, part := range []string{l.Ref.Registry, l.Ref.Path, l.Binary, l.Ref.Schema, string(l.Spec), fmt.Sprint(l.NoConnection), strings.Join(l.Env, "\x00")} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// lease is a plugin process checked out for one call; release it when done.
type lease struct {
	p    *pluginProc
	host *pluginHost
	once sync.Once
}

func (l *lease) release() {
	l.once.Do(func() {
		l.host.mu.Lock()
		l.p.busy = false
		l.p.lastUsed = time.Now()
		l.host.cond.Broadcast()
		l.host.mu.Unlock()
	})
}

// discard releases a lease whose process must not be reused.
func (l *lease) discard() {
	l.once.Do(func() {
		l.host.mu.Lock()
		delete(l.host.procs, l.p)
		l.host.cond.Broadcast()
		l.host.mu.Unlock()
		go l.p.stop()
	})
}

// acquire leases an idle initialised plugin for the launch, starting one when
// none is free. A grpc ref has a single shared server, so callers wait for it.
func (h *pluginHost) acquire(ctx context.Context, ln launch) (*lease, error) {
	key := ln.key()
	h.mu.Lock()
	for {
		var shared *pluginProc
		for p := range h.procs {
			if p.key != key {
				continue
			}
			if !p.alive() {
				delete(h.procs, p)
				continue
			}
			if !p.busy {
				p.busy = true
				h.mu.Unlock()
				return &lease{p: p, host: h}, nil
			}
			shared = p
		}
		if shared == nil || ln.Ref.Registry != RegistryGRPC {
			break
		}
		// A grpc server is shared: wait for it to be free.
		if err := h.waitLocked(ctx); err != nil {
			h.mu.Unlock()
			return nil, err
		}
	}
	h.evictLocked()
	h.mu.Unlock()

	p, err := startPlugin(ctx, ln)
	if err != nil {
		return nil, err
	}
	p.key = key
	h.mu.Lock()
	h.procs[p] = true
	h.infos[key] = p.info
	p.busy = true
	p.lastUsed = time.Now()
	if !h.reaping {
		h.reaping = true
		go h.reap()
	}
	h.mu.Unlock()
	return &lease{p: p, host: h}, nil
}

// waitLocked waits (h.mu held) for a lease to be released or ctx to end.
func (h *pluginHost) waitLocked(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			h.mu.Lock()
			h.cond.Broadcast()
			h.mu.Unlock()
		case <-done:
		}
	}()
	h.cond.Wait()
	close(done)
	return ctx.Err()
}

// info returns the plugin's schema for a launch, starting the plugin if no
// process for it has reported one yet.
func (h *pluginHost) info(ctx context.Context, ln launch) (*pluginInfo, error) {
	h.mu.Lock()
	pi, ok := h.infos[ln.key()]
	h.mu.Unlock()
	if ok {
		return pi, nil
	}
	l, err := h.acquire(ctx, ln)
	if err != nil {
		return nil, err
	}
	defer l.release()
	return l.p.info, nil
}

// evictLocked stops idle processes until there is room for one more.
func (h *pluginHost) evictLocked() {
	for len(h.procs) >= maxPluginProcesses {
		var oldest *pluginProc
		for p := range h.procs {
			if !p.busy && (oldest == nil || p.lastUsed.Before(oldest.lastUsed)) {
				oldest = p
			}
		}
		if oldest == nil {
			return
		}
		delete(h.procs, oldest)
		go oldest.stop()
	}
}

func (h *pluginHost) reap() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		h.mu.Lock()
		for p := range h.procs {
			if !p.busy && time.Since(p.lastUsed) > pluginIdleTimeout {
				delete(h.procs, p)
				go p.stop()
			}
		}
		h.mu.Unlock()
	}
}

// StopPlugins kills every plugin process this worker started.
func StopPlugins() {
	host.mu.Lock()
	procs := make([]*pluginProc, 0, len(host.procs))
	for p := range host.procs {
		procs = append(procs, p)
	}
	host.procs = map[*pluginProc]bool{}
	host.mu.Unlock()
	for _, p := range procs {
		p.stop()
	}
}

// pluginEnv is the environment a plugin runs with: the worker's own (cloud
// SDK credential chains such as ~/.aws or AWS_PROFILE work as they do under
// the CloudQuery CLI), minus any CloudQuery login. The CloudQuery API key
// comes only from a cloudquery_hub secret (extra), and XDG_DATA_HOME points at
// a worker-private directory so a plugin cannot pick up the session token a
// `cloudquery login` left in the user's data directory.
func pluginEnv(extra []string) []string {
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "CLOUDQUERY_API_KEY", "_CQ_TEAM_NAME", "XDG_DATA_HOME", "CQ_TELEMETRY_LEVEL":
			continue
		}
		env = append(env, kv)
	}
	data := filepath.Join(filepath.Dir(DefaultCacheDir()), "plugin-data")
	_ = os.MkdirAll(data, 0o700)
	env = append(env, "XDG_DATA_HOME="+data, "CQ_TELEMETRY_LEVEL=none")
	return append(env, extra...)
}

func pluginLogLevel() string {
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return "debug"
	}
	return "warn"
}

// startPlugin starts (or, for grpc refs, connects to) a plugin and
// initialises it.
func startPlugin(ctx context.Context, ln launch) (*pluginProc, error) {
	p := &pluginProc{}
	var target string
	var dialOpts []grpc.DialOption
	if ln.Ref.Registry == RegistryGRPC {
		target = ln.Ref.Path
	} else {
		if st, err := os.Stat(ln.Binary); err != nil {
			return nil, fmt.Errorf("plugin binary %s: %w", ln.Binary, err)
		} else if st.IsDir() {
			return nil, fmt.Errorf("plugin binary %s is a directory", ln.Binary)
		}
		dir, err := os.MkdirTemp("", "vgicq")
		if err != nil {
			return nil, err
		}
		p.sockDir = dir
		sock := filepath.Join(dir, "p.sock")
		argv := []string{ln.Binary, "serve", "--network", "unix", "--address", sock,
			"--log-level", pluginLogLevel(), "--log-format", "json", "--no-sentry"}
		if supervisorBinary != "" {
			argv = append([]string{supervisorBinary, SuperviseArg}, argv...)
		}
		cmd := exec.Command(argv[0], argv[1:]...)
		// Plugins log to stdout; the worker's stdout carries the VGI protocol.
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		cmd.Env = pluginEnv(ln.Env)
		setPdeathsig(cmd)
		if supervisorBinary != "" {
			// The supervisor kills the plugin when this pipe closes.
			r, w, err := os.Pipe()
			if err != nil {
				_ = os.RemoveAll(dir)
				return nil, err
			}
			cmd.Stdin = r
			p.lifeline = w
			defer func() { _ = r.Close() }()
		}
		if err := cmd.Start(); err != nil {
			_ = os.RemoveAll(dir)
			if p.lifeline != nil {
				_ = p.lifeline.Close()
			}
			return nil, fmt.Errorf("starting plugin %s: %w", ln.Ref.Display(), err)
		}
		p.cmd = cmd
		p.exited = make(chan struct{})
		var waitErr error
		go func() { waitErr = cmd.Wait(); close(p.exited) }()
		deadline := time.Now().Add(pluginStartTimeout)
		for {
			if c, err := net.DialTimeout("unix", sock, time.Second); err == nil {
				_ = c.Close()
				break
			}
			select {
			case <-p.exited:
				_ = os.RemoveAll(dir)
				return nil, fmt.Errorf("plugin %s exited before serving (%v); is it a CloudQuery plugin built with plugin-sdk v4 (protocol v3)?", ln.Ref.Display(), waitErr)
			case <-ctx.Done():
				p.stop()
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
			if time.Now().After(deadline) {
				p.stop()
				return nil, fmt.Errorf("plugin %s did not open its socket within %s", ln.Ref.Display(), pluginStartTimeout)
			}
		}
		// passthrough + a dialer: socket paths (Windows ones included) are
		// used verbatim rather than parsed as a URL.
		target = "passthrough:///" + sock
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		}))
	}
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxGRPCMessage), grpc.MaxCallSendMsgSize(maxGRPCMessage)))
	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		p.stop()
		return nil, fmt.Errorf("connecting to plugin %s: %w", ln.Ref.Display(), err)
	}
	p.conn = conn
	p.client = pb.NewPluginClient(conn)
	info, err := initPlugin(ctx, p.client, ln)
	if err != nil {
		p.stop()
		return nil, err
	}
	p.info = info
	return p, nil
}

func newInvocationID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func initPlugin(ctx context.Context, c pb.PluginClient, ln launch) (*pluginInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, pluginInitTimeout)
	defer cancel()
	name := ln.Ref.Display()
	info := &pluginInfo{byName: map[string]*arrow.Schema{}}
	if r, err := c.GetName(ctx, &pb.GetName_Request{}); err == nil {
		info.Name = r.GetName()
	} else {
		return nil, fmt.Errorf("plugin %s does not speak CloudQuery plugin protocol v3: %s", name, grpcMessage(err))
	}
	if r, err := c.GetVersion(ctx, &pb.GetVersion_Request{}); err == nil {
		info.Version = r.GetVersion()
	}
	if r, err := c.GetSpecSchema(ctx, &pb.GetSpecSchema_Request{}); err == nil {
		info.SpecSchema = r.GetJsonSchema()
	}
	err := retryInUse(ctx, func() error {
		_, err := c.Init(ctx, &pb.Init_Request{Spec: ln.Spec, NoConnection: ln.NoConnection, InvocationId: newInvocationID()})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("plugin %s rejected its spec: %s", name, grpcMessage(err))
	}
	resp, err := c.GetTables(ctx, &pb.GetTables_Request{Tables: []string{"*"}})
	if err != nil {
		return nil, fmt.Errorf("plugin %s: GetTables: %s", name, grpcMessage(err))
	}
	schemas, err := pb.NewSchemasFromBytes(resp.GetTables())
	if err != nil {
		return nil, fmt.Errorf("plugin %s: decoding table schemas: %w", name, err)
	}
	for _, s := range schemas {
		t := tableName(s)
		if t == "" {
			continue
		}
		if _, dup := info.byName[t]; !dup {
			info.Tables = append(info.Tables, s)
			info.byName[t] = s
		}
	}
	return info, nil
}

// retryInUse retries fn while the plugin reports it is still busy with a
// previous call (plugin-sdk serves one Init/Sync at a time and releases the
// lock a moment after a cancelled stream returns).
func retryInUse(ctx context.Context, fn func() error) error {
	delay := 20 * time.Millisecond
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := fn()
		if err == nil || !strings.Contains(err.Error(), "already in use") || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay < time.Second {
			delay *= 2
		}
	}
}

// grpcMessage strips the gRPC framing from a plugin error.
func grpcMessage(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err.Error()
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.Unavailable {
			return "plugin unavailable: " + s.Message()
		}
		return strings.TrimSpace(s.Message())
	}
	return err.Error()
}
