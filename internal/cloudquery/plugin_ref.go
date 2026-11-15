// Plugin references: which CloudQuery source plugin each schema runs.
//
// The `plugins` ATTACH option is a comma-separated list of `[schema=]ref`
// entries. A ref is one of:
//
//	/opt/cq-source-xkcd               a local plugin binary (also ./, ../, ~/)
//	cloudquery/xkcd@v1.6.1            CloudQuery Hub (registry "cloudquery", the CLI default)
//	hub:cloudquery/xkcd               Hub, latest version (resolved through the public Hub API)
//	github:cloudquery/test@v4.1.0     a GitHub release asset (the CLI's "github" registry)
//	grpc:127.0.0.1:7777               an already-running plugin server (e.g. a docker plugin)
//
// A Hub or GitHub ref can pin the archive's sha256 with a `#sha256:<hex>`
// suffix: github:cloudquery/test@v4.1.0#sha256:96342f...

package cloudquery

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Plugin registries.
const (
	RegistryLocal  = "local"
	RegistryHub    = "cloudquery"
	RegistryGitHub = "github"
	RegistryGRPC   = "grpc"
)

// PluginRef is one parsed entry of the plugins option.
type PluginRef struct {
	Schema   string `json:"schema"`
	Raw      string `json:"raw"`
	Registry string `json:"registry"`
	// Path is the local binary (registry local) or host:port (registry grpc).
	Path string `json:"path,omitempty"`
	// Team, Name and Version locate a Hub or GitHub plugin.
	Team    string `json:"team,omitempty"`
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	// SHA256 pins the downloaded archive (hex), when given.
	SHA256 string `json:"sha256,omitempty"`
}

// IsLocal reports whether the ref names a local binary.
func (r PluginRef) IsLocal() bool { return r.Registry == RegistryLocal }

// Display renders the ref the way a user writes it.
func (r PluginRef) Display() string {
	switch r.Registry {
	case RegistryLocal:
		return r.Path
	case RegistryGRPC:
		return "grpc:" + r.Path
	}
	s := r.Team + "/" + r.Name
	if r.Registry == RegistryGitHub {
		s = "github:" + s
	}
	if r.Version != "" {
		s += "@" + r.Version
	}
	if r.SHA256 != "" {
		s += "#sha256:" + r.SHA256
	}
	return s
}

var (
	identRe   = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	nameRe    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	teamRe    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	versionRe = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$`)
	sha256Re  = regexp.MustCompile(`^[a-f0-9]{64}$`)
	hostRe    = regexp.MustCompile(`^[A-Za-z0-9.\-\[\]:]+:[0-9]{1,5}$`)
)

func looksLocal(ref string) bool {
	return strings.HasPrefix(ref, "/") || strings.HasPrefix(ref, "./") || strings.HasPrefix(ref, "../") ||
		strings.HasPrefix(ref, "~/") || strings.HasPrefix(ref, `\\`) || filepath.VolumeName(ref) != "" ||
		strings.HasSuffix(ref, ".exe")
}

// localName derives a plugin name from a binary file name:
// cq-source-xkcd -> xkcd, plugin-xkcd-v1.6.1-linux-amd64 -> xkcd.
func localName(path string) string {
	base := strings.ToLower(strings.TrimSuffix(filepath.Base(path), ".exe"))
	for _, p := range []string{"cq-source-", "cq-plugin-", "plugin-"} {
		base = strings.TrimPrefix(base, p)
	}
	if i := strings.Index(base, "-v"); i > 0 {
		base = base[:i]
	}
	if i := strings.Index(base, "_"); i > 0 && (strings.Contains(base, "_darwin") || strings.Contains(base, "_linux") || strings.Contains(base, "_windows")) {
		base = base[:i]
	}
	return schemaIdent(base)
}

// schemaIdent turns a plugin name into a SQL identifier.
func schemaIdent(s string) string {
	s = strings.ToLower(strings.NewReplacer("-", "_", ".", "_").Replace(s))
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		s = "p_" + s
	}
	return s
}

func expandHome(p string) (string, error) {
	if !strings.HasPrefix(p, "~/") {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, p[2:]), nil
}

// ParsePluginRef parses one ref (without a schema= prefix).
func ParsePluginRef(raw string) (PluginRef, error) {
	ref := strings.TrimSpace(raw)
	r := PluginRef{Raw: ref}
	switch {
	case ref == "":
		return r, fmt.Errorf("empty plugin reference")
	case strings.HasPrefix(ref, "grpc:"):
		addr := strings.TrimPrefix(strings.TrimPrefix(ref, "grpc:"), "//")
		if !hostRe.MatchString(addr) {
			return r, fmt.Errorf("plugin %q: a grpc ref is grpc:<host>:<port>", ref)
		}
		r.Registry, r.Path = RegistryGRPC, addr
		r.Name = schemaIdent(strings.Split(addr, ":")[0])
		return r, nil
	case strings.HasPrefix(ref, "docker:"):
		return r, fmt.Errorf("plugin %q: docker plugins are not started by this worker; run the container yourself "+
			"(docker run -p 7777:7777 <image> serve --address 0.0.0.0:7777) and use grpc:127.0.0.1:7777", ref)
	case looksLocal(ref):
		p, err := expandHome(ref)
		if err != nil {
			return r, err
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			return r, err
		}
		r.Registry, r.Path, r.Name = RegistryLocal, abs, localName(abs)
		return r, nil
	}
	r.Registry = RegistryHub
	switch {
	case strings.HasPrefix(ref, "github:"):
		r.Registry, ref = RegistryGitHub, strings.TrimPrefix(ref, "github:")
	case strings.HasPrefix(ref, "hub:"):
		ref = strings.TrimPrefix(ref, "hub:")
	case strings.HasPrefix(ref, "cloudquery:"):
		ref = strings.TrimPrefix(ref, "cloudquery:")
	}
	if i := strings.Index(ref, "#"); i >= 0 {
		pin := ref[i+1:]
		ref = ref[:i]
		if !strings.HasPrefix(pin, "sha256:") || !sha256Re.MatchString(strings.TrimPrefix(pin, "sha256:")) {
			return r, fmt.Errorf("plugin %q: a pin is #sha256:<64 lowercase hex digits>", raw)
		}
		r.SHA256 = strings.TrimPrefix(pin, "sha256:")
	}
	if i := strings.Index(ref, "@"); i >= 0 {
		r.Version = ref[i+1:]
		ref = ref[:i]
		if !strings.HasPrefix(r.Version, "v") {
			r.Version = "v" + r.Version
		}
		if !versionRe.MatchString(r.Version) {
			return r, fmt.Errorf("plugin %q: version %q is not vMAJOR.MINOR.PATCH", raw, r.Version)
		}
	}
	parts := strings.Split(ref, "/")
	if len(parts) == 1 {
		parts = []string{"cloudquery", parts[0]}
	}
	if len(parts) != 2 || !teamRe.MatchString(parts[0]) || !nameRe.MatchString(parts[1]) {
		return r, fmt.Errorf("plugin %q is not a local path, grpc:<host:port>, or [hub:|github:]team/name[@version]", raw)
	}
	r.Team, r.Name = parts[0], parts[1]
	if r.Registry == RegistryGitHub && r.Version == "" {
		return r, fmt.Errorf("plugin %q: GitHub plugins need an explicit @version", raw)
	}
	return r, nil
}

// ParsePluginRefs parses the plugins option.
func ParsePluginRefs(src string) ([]PluginRef, error) {
	var out []PluginRef
	seen := map[string]bool{}
	for _, entry := range strings.Split(src, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		schema := ""
		if i := strings.Index(entry, "="); i > 0 && !strings.ContainsAny(entry[:i], "/:@#") {
			schema, entry = strings.TrimSpace(entry[:i]), strings.TrimSpace(entry[i+1:])
		}
		r, err := ParsePluginRef(entry)
		if err != nil {
			return nil, err
		}
		if schema == "" {
			schema = schemaIdent(r.Name)
		}
		if !identRe.MatchString(schema) {
			return nil, fmt.Errorf("schema name %q must be a lowercase identifier ([a-z_][a-z0-9_]*)", schema)
		}
		if schema == "main" || schema == "information_schema" || schema == "pg_catalog" {
			return nil, fmt.Errorf("schema name %q is reserved; give the plugin another one with name=ref", schema)
		}
		if seen[schema] {
			return nil, fmt.Errorf("schema %q is named twice in the plugins option; give one of them another name with name=ref", schema)
		}
		seen[schema] = true
		r.Schema = schema
		out = append(out, r)
	}
	return out, nil
}
