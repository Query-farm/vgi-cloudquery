// cloudquery.main.install_plugin(ref): download a Hub or GitHub plugin into the
// verified cache, so a later ATTACH naming it starts without the network.
//
// CloudQuery Hub downloads need an API key, and an ATTACH cannot read DuckDB
// secrets, so this function is how a Hub plugin gets into the cache: it reads
// the key from a cloudquery_hub secret at bind time and never stores it.
//
//	CREATE SECRET (TYPE cloudquery_hub, API_KEY '...');
//	SELECT * FROM cq.main.install_plugin('cloudquery/xkcd@v1.6.1');
//	DETACH cq;
//	ATTACH 'cloudquery' AS cq (TYPE vgi, LOCATION '...', plugins 'cloudquery/xkcd@v1.6.1');

package cloudquery

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const installFunctionName = "install_plugin"

type installArgs struct {
	Ref string `vgi:"pos=0,doc=Plugin to download: a Hub ref (cloudquery/xkcd@v1.6.1 or hub:team/name for the latest version) or a GitHub ref (github:cloudquery/test@v4.1.0); append #sha256:<hex> to pin the archive."`
}

var installSchema = arrow.NewSchema([]arrow.Field{
	column("ref", arrow.BinaryTypes.String, "The plugin reference, with the version it resolved to.", false),
	column("registry", arrow.BinaryTypes.String, "cloudquery (Hub) or github.", false),
	column("plugin_version", arrow.BinaryTypes.String, "The version downloaded or found in the cache.", false),
	column("archive_sha256", arrow.BinaryTypes.String, "sha256 of the archive, verified against the registry's published checksum.", false),
	column("binary_sha256", arrow.BinaryTypes.String, "sha256 of the unpacked plugin binary.", false),
	column("binary_path", arrow.BinaryTypes.String, "Where the binary is cached.", false),
	column("source_url", arrow.BinaryTypes.String, "Where the archive came from (query string removed).", true),
	column("from_cache", arrow.FixedWidthTypes.Boolean, "True when the plugin was already cached and nothing was downloaded.", false),
}, nil)

// InstallState records whether the single row has been emitted.
type InstallState struct {
	Done bool
}

// Install is the install_plugin table function.
type Install struct{}

var _ vgi.TypedTableFunc[InstallState] = (*Install)(nil)

// Name is the SQL name.
func (*Install) Name() string { return installFunctionName }

// Metadata documents the function.
func (*Install) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Download a CloudQuery Hub or GitHub plugin into the worker's verified plugin cache",
		Stability:   vgi.StabilityVolatile,
		Tags:        withExamples(installTags(), installExamples),
		Examples:    installExamples,
	}
}

// ArgumentSpecs derives the signature from installArgs.
func (*Install) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(installArgs{}) }

// OnBind validates the ref and asks for the cloudquery_hub secret.
func (*Install) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var args installArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	r, err := ParsePluginRef(args.Ref)
	if err != nil {
		return nil, err
	}
	if r.Registry != RegistryHub && r.Registry != RegistryGitHub {
		return nil, fmt.Errorf("install_plugin downloads Hub and GitHub plugins; %q is a %s plugin, which needs no download", args.Ref, r.Registry)
	}
	if !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{SecretScopeRequest: []vgi.SecretLookup{{SecretType: HubSecretType, Scope: DefaultHubURL}}}, nil
	}
	return vgi.BindSchema(installSchema)
}

// NewState starts the one-row scan.
func (*Install) NewState(*vgi.ProcessParams) (*InstallState, error) { return &InstallState{}, nil }

// Process downloads (or finds) the plugin and emits one row.
func (*Install) Process(ctx context.Context, params *vgi.ProcessParams, state *InstallState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	var args installArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return err
	}
	r, err := ParsePluginRef(args.Ref)
	if err != nil {
		return err
	}
	st, _ := decodeAttachState(params.AttachScope)
	cfg := st.fetchConfig()
	key, team, hubURL := hubKey(params.Secrets)
	cfg.APIKey, cfg.Team = key, team
	if hubURL != "" {
		cfg.HubURL = hubURL
	}
	fp, err := FetchPlugin(ctx, r, cfg)
	if err != nil {
		return err
	}
	r.Version = fp.Version
	row := infoRow{r.Display(), r.Registry, fp.Version, fp.ArchiveSHA256, fp.BinarySHA256, fp.Binary, strPtr(fp.SourceURL), fp.FromCache}
	cols := buildColumns(memory.NewGoAllocator(), installSchema, []infoRow{row})
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	if err := out.EmitArrays(cols, 1); err != nil {
		return err
	}
	return out.Finish()
}

// installResultColumns describes install_plugin's fixed result for vgi.result_columns_schema.
func installResultColumns() []map[string]string {
	cols := []map[string]string{}
	for _, f := range installSchema.Fields() {
		cols = append(cols, map[string]string{"name": f.Name, "type": duckdbTypeName(f.Type), "description": mdValue(f.Metadata, "comment")})
	}
	return cols
}
