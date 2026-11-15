// cloudquery.main.scan(schema, table_name, max_rows := 0, strict := false):
// read one plugin table. Every plugin table in the catalog is backed by this
// function, and it can be called directly too (for a plugin whose tables
// depend on credentials only a secret holds).
//
// Bind is two-phase: the first call asks DuckDB for the `cloudquery` secret in
// scope cloudquery://<schema> (and a cloudquery_hub secret, whose API key paid
// plugins read at run time); the second deep-merges the secret's spec over the
// ATTACH spec, starts (or reuses) the plugin with it, and returns the table's
// columns. Secrets are never stored in scan state.
//
// Each scan runs one plugin Sync of just that table (skip_dependent_tables, so
// child relations are not fetched; selecting a child table makes the plugin
// walk its parents, whose rows are dropped). Insert messages carry Arrow
// record batches: the projected columns are taken from them without copying
// (CloudQuery's uuid/json/inet columns keep their buffers too) and small
// batches are coalesced into batches of up to scanBatchRows rows, flushed
// after scanFlushInterval so a slow API still shows rows promptly.
// migrate_table and delete_record messages carry nothing a read needs and are
// skipped. Per-table errors are logged (the CloudQuery CLI's behaviour) unless
// strict := true makes them fail the query.
//
// Over HTTP a batch may be served by another process: the state keeps how many
// rows were emitted, and a process with no open stream for the scan re-syncs
// and skips that many rows (cq ids are deterministic, so rows line up).

package cloudquery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pb "github.com/cloudquery/plugin-pb-go/pb/plugin/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	scanFunctionName  = "scan"
	scanBatchRows     = 2048
	scanFlushInterval = 250 * time.Millisecond
	// abandonedScanTimeout cancels a plugin sync DuckDB stopped reading
	// (LIMIT satisfied, query cancelled) and never finished.
	abandonedScanTimeout = 2 * time.Minute
)

type scanArgs struct {
	Schema  string `vgi:"pos=0,doc=Plugin schema of this ATTACH, as named by the plugins option (e.g. 'hackernews')."`
	Table   string `vgi:"pos=1,doc=Plugin table name (e.g. 'hackernews_items' or 'aws_s3_buckets')."`
	MaxRows int64  `vgi:"default=0,ge=0,doc=Stop the plugin's sync after this many rows of the table (0 = no limit). Cancels the remaining API calls; CloudQuery has no limit of its own."`
	Strict  bool   `vgi:"default=false,doc=Fail the query when the plugin reports an error for a table it syncs. By default errors are logged and the rows fetched so far are returned (CloudQuery's own behaviour)."`
}

// ScanState is the per-scan cursor. It holds no secrets: the spec is
// recomputed from the ATTACH data and the resolved secret on every process.
type ScanState struct {
	ScanID  string
	Schema  string
	Table   string
	MaxRows int64
	Strict  bool
	Emitted int64
	Done    bool
}

// secretScope is the DuckDB secret scope a schema's spec is looked up under.
func secretScope(schema string) string { return "cloudquery://" + schema }

// effectiveSpec deep-merges the schema's `cloudquery` secret (when one
// applies) over the ATTACH spec.
func effectiveSpec(ap *attachedPlugin, secrets vgi.Secrets) (map[string]any, error) {
	spec := ap.Spec
	if spec == nil {
		spec = map[string]any{}
	}
	for i, fields := range secrets.OfType(SecretType) {
		name := fmt.Sprintf("#%d", i+1)
		if n := vgi.RenderSecretValue(fields["name"]); n != "" {
			name = n
		}
		target := strings.ToLower(strings.TrimSpace(vgi.RenderSecretValue(fields["plugin"])))
		if target != "" && target != ap.Ref.Schema && target != strings.ToLower(ap.Ref.Name) {
			slog.Debug("ignoring a cloudquery secret for another plugin", "secret", name, "plugin", target, "schema", ap.Ref.Schema)
			continue
		}
		overlay, err := parseObject(vgi.RenderSecretValue(fields["spec"]), fmt.Sprintf("cloudquery secret %q spec", name))
		if err != nil {
			return nil, err
		}
		return deepMerge(spec, overlay), nil
	}
	return spec, nil
}

// hubKey returns the cloudquery_hub secret's API key and team, if any.
func hubKey(secrets vgi.Secrets) (key, team, hubURL string) {
	for _, fields := range secrets.OfType(HubSecretType) {
		return strings.TrimSpace(vgi.RenderSecretValue(fields["api_key"])),
			strings.TrimSpace(vgi.RenderSecretValue(fields["team"])),
			strings.TrimSpace(vgi.RenderSecretValue(fields["hub_url"]))
	}
	return "", "", ""
}

// queryLaunch is the launch a query runs: the effective spec, with a
// connection, and the Hub API key in the plugin's environment when a
// cloudquery_hub secret holds one (paid plugins report usage with it).
func queryLaunch(ap *attachedPlugin, secrets vgi.Secrets) (launch, error) {
	spec, err := effectiveSpec(ap, secrets)
	if err != nil {
		return launch{}, err
	}
	data, err := specJSON(spec)
	if err != nil {
		return launch{}, err
	}
	ln := launch{Ref: ap.Ref, Binary: ap.Fetched.Binary, Spec: data}
	if key, team, _ := hubKey(secrets); key != "" {
		ln.Env = append(ln.Env, "CLOUDQUERY_API_KEY="+key)
		if team != "" {
			ln.Env = append(ln.Env, "_CQ_TEAM_NAME="+team)
		}
	}
	return ln, nil
}

// resolveTable finds the plugin and the table's schema under the query's
// effective spec.
func resolveTable(ctx context.Context, attach []byte, secrets vgi.Secrets, schema, table string) (launch, *arrow.Schema, error) {
	st, ok := decodeAttachState(attach)
	if !ok || len(st.Plugins) == 0 {
		return launch{}, nil, fmt.Errorf("no CloudQuery plugins are attached: ATTACH with the plugins option, e.g. plugins 'github:cloudquery/test@v4.1.0'")
	}
	ap, ok := st.plugin(schema)
	if !ok {
		return launch{}, nil, fmt.Errorf("no plugin schema %q in this ATTACH (attached: %s)", schema, strings.Join(st.schemaNames(), ", "))
	}
	ln, err := queryLaunch(ap, secrets)
	if err != nil {
		return launch{}, nil, err
	}
	info, err := host.info(ctx, ln)
	if err != nil {
		return launch{}, nil, err
	}
	s, ok := info.table(table)
	if !ok {
		names := info.tableNames()
		if len(names) > 25 {
			names = append(names[:25], "...")
		}
		return launch{}, nil, fmt.Errorf("plugin %s has no table %q (tables: %s)", ap.Ref.Display(), table, strings.Join(names, ", "))
	}
	return ln, s, nil
}

// Scan is the scan table function.
type Scan struct{}

var _ vgi.TypedTableFunc[ScanState] = (*Scan)(nil)

// Name is the SQL name.
func (*Scan) Name() string { return scanFunctionName }

// Metadata declares projection pushdown and documents the function.
// CloudQuery plugins take no per-query filters, so filters are left to DuckDB.
func (*Scan) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Read one table of an attached CloudQuery source plugin by running a sync of just that table",
		Stability:          vgi.StabilityVolatile,
		ProjectionPushdown: true,
		Tags:               withExamples(scanTags(), scanExamples),
		Examples:           scanExamples,
	}
}

// ArgumentSpecs derives the signature from scanArgs.
func (*Scan) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(scanArgs{}) }

func scanSecretLookups(schema string) []vgi.SecretLookup {
	return []vgi.SecretLookup{
		{SecretType: SecretType, Scope: secretScope(schema)},
		{SecretType: HubSecretType, Scope: DefaultHubURL},
	}
}

// OnBind resolves the table's columns, after asking for the schema's secrets.
func (*Scan) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var args scanArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	schema := strings.ToLower(args.Schema)
	if !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{SecretScopeRequest: scanSecretLookups(schema)}, nil
	}
	_, s, err := resolveTable(context.Background(), params.AttachOpaqueData, params.Secrets, schema, args.Table)
	if err != nil {
		return nil, err
	}
	return vgi.BindSchema(outputSchema(s))
}

// NewState starts a cursor for one scan.
func (*Scan) NewState(params *vgi.ProcessParams) (*ScanState, error) {
	var args scanArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	id := make([]byte, 12)
	if _, err := rand.Read(id); err != nil {
		return nil, err
	}
	return &ScanState{ScanID: hex.EncodeToString(id), Schema: strings.ToLower(args.Schema), Table: args.Table,
		MaxRows: args.MaxRows, Strict: args.Strict}, nil
}

// Process emits the next batch of rows.
func (*Scan) Process(ctx context.Context, params *vgi.ProcessParams, state *ScanState, out *vgirpc.OutputCollector) error {
	for !state.Done {
		if state.MaxRows > 0 && state.Emitted >= state.MaxRows {
			state.Done = true
			streams.close(state.ScanID)
			break
		}
		s, err := openScanStream(ctx, params, state)
		if err != nil {
			return err
		}
		want := scanBatchRows
		if state.MaxRows > 0 && state.MaxRows-state.Emitted < int64(want) {
			want = int(state.MaxRows - state.Emitted)
		}
		batch, eof, err := s.next(ctx, want, scanFlushInterval)
		if err != nil {
			streams.close(state.ScanID)
			return err
		}
		if eof {
			state.Done = true
			streams.close(state.ScanID)
		}
		if batch == nil {
			continue
		}
		if err := checkNotNull(params.OutputSchema, batch); err != nil {
			batch.Release()
			streams.close(state.ScanID)
			return fmt.Errorf("%s.%s: %w", state.Schema, state.Table, err)
		}
		state.Emitted += batch.NumRows()
		// The collector owns the batch from here on.
		if err := out.Emit(batch); err != nil {
			return err
		}
		if !state.Done {
			return nil
		}
	}
	return out.Finish()
}

// checkNotNull enforces the NOT NULL constraints the catalog attests.
func checkNotNull(schema *arrow.Schema, batch arrow.RecordBatch) error {
	for i, f := range schema.Fields() {
		if !f.Nullable && batch.Column(i).NullN() > 0 {
			return fmt.Errorf("the plugin returned NULL in column %s, which it declares NOT NULL", f.Name)
		}
	}
	return nil
}

// project builds the output batch for one plugin record: the requested
// columns, mapped to DuckDB types, sharing the record's buffers.
func project(mem memory.Allocator, outSchema *arrow.Schema, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	src := rec.Schema()
	cols := make([]arrow.Array, outSchema.NumFields())
	release := func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}
	for i, f := range outSchema.Fields() {
		idx := src.FieldIndices(f.Name)
		if len(idx) == 0 {
			cols[i] = array.MakeArrayOfNull(mem, f.Type, int(rec.NumRows()))
			continue
		}
		c, err := convertColumn(mem, rec.Column(idx[0]), src.Field(idx[0]))
		if err != nil {
			release()
			return nil, err
		}
		if !arrow.TypeEqual(c.DataType(), f.Type) {
			c.Release()
			release()
			return nil, fmt.Errorf("column %s arrived as %s, but the table declares %s", f.Name, c.DataType(), f.Type)
		}
		cols[i] = c
	}
	out := array.NewRecordBatch(outSchema, cols, rec.NumRows())
	release()
	return out, nil
}

// concatBatches merges small batches into one.
func concatBatches(mem memory.Allocator, schema *arrow.Schema, batches []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(batches) == 1 {
		return batches[0], nil
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	var n int64
	for _, b := range batches {
		n += b.NumRows()
	}
	cols := make([]arrow.Array, schema.NumFields())
	for i := range cols {
		parts := make([]arrow.Array, len(batches))
		for j, b := range batches {
			parts[j] = b.Column(i)
		}
		c, err := array.Concatenate(parts, mem)
		if err != nil {
			for _, done := range cols[:i] {
				done.Release()
			}
			return nil, err
		}
		cols[i] = c
	}
	out := array.NewRecordBatch(schema, cols, n)
	for _, c := range cols {
		c.Release()
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// Sync streams

type batchOrErr struct {
	batch arrow.RecordBatch
	err   error
}

type scanStream struct {
	ch      chan batchOrErr
	done    chan struct{}
	cancel  context.CancelFunc
	touched time.Time
	mu      sync.Mutex
	skip    int64
	schema  *arrow.Schema
	pending arrow.RecordBatch // rows beyond the last batch's limit
	closeMu sync.Once
}

type streamRegistry struct {
	mu      sync.Mutex
	streams map[string]*scanStream
	reaping bool
}

var streams = &streamRegistry{streams: map[string]*scanStream{}}

func (r *streamRegistry) get(id string) (*scanStream, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if ok {
		s.touched = time.Now()
	}
	return s, ok
}

func (r *streamRegistry) put(id string, s *scanStream) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s.touched = time.Now()
	r.streams[id] = s
	if !r.reaping {
		r.reaping = true
		go r.reap()
	}
}

func (r *streamRegistry) close(id string) {
	r.mu.Lock()
	s, ok := r.streams[id]
	delete(r.streams, id)
	r.mu.Unlock()
	if ok {
		s.stop()
	}
}

func (r *streamRegistry) reap() {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for range t.C {
		r.mu.Lock()
		for id, s := range r.streams {
			if time.Since(s.touched) > abandonedScanTimeout {
				delete(r.streams, id)
				go s.stop()
			}
		}
		r.mu.Unlock()
	}
}

// stop cancels the sync; the reader goroutine releases the plugin lease once
// the stream has ended.
func (s *scanStream) stop() {
	s.closeMu.Do(func() {
		close(s.done)
		s.cancel()
		s.mu.Lock()
		if s.pending != nil {
			s.pending.Release()
			s.pending = nil
		}
		s.mu.Unlock()
	})
}

// openScanStream returns the open stream for this scan, starting the plugin
// Sync if this process has none (first batch, or a resumed HTTP scan).
func openScanStream(ctx context.Context, params *vgi.ProcessParams, state *ScanState) (*scanStream, error) {
	if s, ok := streams.get(state.ScanID); ok {
		return s, nil
	}
	ln, _, err := resolveTable(ctx, params.AttachScope, params.Secrets, state.Schema, state.Table)
	if err != nil {
		return nil, err
	}
	l, err := host.acquire(ctx, ln)
	if err != nil {
		return nil, err
	}
	syncCtx, cancel := context.WithCancel(context.Background())
	req := &pb.Sync_Request{
		Tables:              []string{state.Table},
		SkipDependentTables: true,
		DeterministicCqId:   true,
		WithErrorMessages:   true,
	}
	slog.Debug("cloudquery sync", "schema", state.Schema, "table", state.Table, "skip", state.Emitted, "columns", params.OutputSchema.NumFields())
	s := &scanStream{ch: make(chan batchOrErr, 4), done: make(chan struct{}), cancel: cancel, skip: state.Emitted, schema: params.OutputSchema}
	send := func(x batchOrErr) bool {
		select {
		case s.ch <- x:
			return true
		case <-s.done:
			if x.batch != nil {
				x.batch.Release()
			}
			return false
		}
	}
	strict, table, label := state.Strict, state.Table, state.Schema+"."+state.Table
	go func() {
		defer close(s.ch)
		mem := memory.NewGoAllocator()
		healthy := true
		defer func() {
			if healthy {
				l.release()
			} else {
				l.discard()
			}
		}()
		// plugin-sdk serves one sync at a time and releases its lock a moment
		// after a cancelled stream ends, so a busy answer is retried.
		var stream pb.Plugin_SyncClient
		var resp *pb.Sync_Response
		err := retryInUse(syncCtx, func() error {
			var err error
			if stream, err = l.p.client.Sync(syncCtx, req); err != nil {
				return err
			}
			resp, err = stream.Recv()
			return err
		})
		for ; ; resp, err = stream.Recv() {
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				if syncCtx.Err() != nil {
					return // cancelled by us: the plugin stays usable
				}
				if status.Code(err) == codes.Unavailable {
					healthy = false
				}
				msg := grpcMessage(err)
				if strings.Contains(msg, "usage client") {
					msg += " (the table is paid: CloudQuery's premium plugins and tables report usage with an API key; " +
						"CREATE SECRET (TYPE cloudquery_hub, API_KEY '...') to supply one)"
				}
				send(batchOrErr{err: fmt.Errorf("%s: %s", label, msg)})
				return
			}
			switch m := resp.GetMessage().(type) {
			case *pb.Sync_Response_Insert:
				rec, err := pb.NewRecordFromBytes(m.Insert.GetRecord())
				if err != nil {
					send(batchOrErr{err: fmt.Errorf("%s: decoding a record batch: %w", label, err)})
					return
				}
				if rec == nil {
					continue
				}
				if tableName(rec.Schema()) != table { // a parent walked to reach a child table
					rec.Release()
					continue
				}
				out, err := project(mem, s.schema, rec)
				rec.Release()
				if err != nil {
					send(batchOrErr{err: fmt.Errorf("%s: %w", label, err)})
					return
				}
				if !send(batchOrErr{batch: out}) {
					return
				}
			case *pb.Sync_Response_Error:
				msg := fmt.Sprintf("plugin error syncing %s: %s", m.Error.GetTableName(), m.Error.GetError())
				if strict {
					send(batchOrErr{err: fmt.Errorf("%s: %s", label, msg)})
					return
				}
				slog.Warn(msg, "scan", label)
			case *pb.Sync_Response_MigrateTable, *pb.Sync_Response_DeleteRecord:
				// Schema announcements and stale-row deletions are for
				// destinations; a read has nothing to do with them.
			}
		}
	}()
	streams.put(state.ScanID, s)
	return s, nil
}

// next collects up to n rows into one batch, returning early with what it
// has once flush has passed since the first row. eof is true when the plugin
// is done. The batch is nil when no rows arrived.
func (s *scanStream) next(ctx context.Context, n int, flush time.Duration) (arrow.RecordBatch, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mem := memory.NewGoAllocator()
	var parts []arrow.RecordBatch
	var have int64
	var timer <-chan time.Time
	finish := func(eof bool) (arrow.RecordBatch, bool, error) {
		if len(parts) == 0 {
			return nil, eof, nil
		}
		b, err := concatBatches(mem, s.schema, parts)
		return b, eof, err
	}
	add := func(b arrow.RecordBatch) {
		if s.skip > 0 {
			if b.NumRows() <= s.skip {
				s.skip -= b.NumRows()
				b.Release()
				return
			}
			sl := b.NewSlice(s.skip, b.NumRows())
			b.Release()
			s.skip, b = 0, sl
		}
		room := int64(n) - have
		if b.NumRows() > room {
			head, tail := b.NewSlice(0, room), b.NewSlice(room, b.NumRows())
			b.Release()
			s.pending, b = tail, head
		}
		parts = append(parts, b)
		have += b.NumRows()
		if timer == nil {
			timer = time.After(flush)
		}
	}
	if s.pending != nil {
		p := s.pending
		s.pending = nil
		add(p)
	}
	for have < int64(n) {
		select {
		case <-ctx.Done():
			for _, p := range parts {
				p.Release()
			}
			return nil, false, ctx.Err()
		case <-timer:
			return finish(false)
		case x, ok := <-s.ch:
			if !ok {
				return finish(s.pending == nil)
			}
			if x.err != nil {
				for _, p := range parts {
					p.Release()
				}
				return nil, false, x.err
			}
			add(x.batch)
		}
	}
	return finish(false)
}
