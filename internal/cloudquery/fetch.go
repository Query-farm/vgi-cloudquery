// Fetching plugin binaries into a verified cache.
//
// Two registries publish CloudQuery source plugins as per-platform zip archives:
//
//   - CloudQuery Hub (registry "cloudquery", api.cloudquery.io): the current
//     home of every official plugin. The asset endpoint returns a download
//     location and the archive's sha256; it answers 401 to anonymous callers
//     (checked 2026-11-15 for free plugins such as xkcd and hackernews too), so
//     Hub downloads need a CloudQuery API key held in a cloudquery_hub secret.
//   - GitHub releases (registry "github"): the original home. The cloudquery
//     monorepo's plugins-source-<name>-<version> releases carried assets until
//     early 2024 (e.g. test v4.1.0, hackernews v3.0.23, xkcd and others), with a
//     checksums.txt beside them; community plugins publish the same layout under
//     <team>/cq-source-<name>.
//
// Every archive is checked against its published sha256 (or the ref's
// #sha256: pin) before anything is unpacked, and a download with no checksum
// to compare against is refused. The unpacked binary's own sha256 is recorded
// and re-checked when the cache is reused, so a cached plugin that changed on
// disk is not run.
//
//	<cache_dir>/<registry>/<team>/<name>/<version>/<os>_<arch>/plugin
//	<cache_dir>/<registry>/<team>/<name>/<version>/<os>_<arch>/meta.json

package cloudquery

import (
	"archive/zip"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Default endpoints.
const (
	DefaultHubURL    = "https://api.cloudquery.io"
	DefaultGitHubURL = "https://github.com"
)

// FetchedPlugin is a plugin binary ready to run.
type FetchedPlugin struct {
	Binary        string `json:"binary"`
	Version       string `json:"version,omitempty"`
	ArchiveSHA256 string `json:"archive_sha256,omitempty"`
	BinarySHA256  string `json:"binary_sha256,omitempty"`
	SourceURL     string `json:"source_url,omitempty"`
	FetchedAt     string `json:"fetched_at,omitempty"`
	// FromCache is true when no download was needed.
	FromCache bool `json:"-"`
}

// fetchConfig says where plugins come from and go.
type fetchConfig struct {
	CacheDir  string
	HubURL    string
	GitHubURL string
	// APIKey and Team come from a cloudquery_hub secret; empty when none.
	APIKey string
	Team   string
}

// DefaultCacheDir is <user cache dir>/vgi-cloudquery/plugins.
func DefaultCacheDir() string {
	d, err := os.UserCacheDir()
	if err != nil {
		d = os.TempDir()
	}
	return filepath.Join(d, "vgi-cloudquery", "plugins")
}

func platform() string { return runtime.GOOS + "_" + runtime.GOARCH }

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

var httpClient = &http.Client{Timeout: 10 * time.Minute}

// fetchLocks serializes fetches of one cache entry within this process.
var fetchLocks sync.Map

func (c fetchConfig) entryDir(r PluginRef, version string) string {
	return filepath.Join(c.CacheDir, r.Registry, r.Team, r.Name, version, platform())
}

// FetchPlugin returns a verified binary for a Hub or GitHub ref, downloading
// it into the cache when needed.
func FetchPlugin(ctx context.Context, r PluginRef, cfg fetchConfig) (*FetchedPlugin, error) {
	if cfg.CacheDir == "" {
		cfg.CacheDir = DefaultCacheDir()
	}
	if cfg.HubURL == "" {
		cfg.HubURL = DefaultHubURL
	}
	if cfg.GitHubURL == "" {
		cfg.GitHubURL = DefaultGitHubURL
	}
	version := r.Version
	if version == "" {
		v, err := latestHubVersion(ctx, r, cfg)
		if err != nil {
			cached := cachedVersions(cfg, r)
			if len(cached) == 0 {
				return nil, fmt.Errorf("plugin %s: resolving the latest version: %w", r.Display(), err)
			}
			v = cached[len(cached)-1]
			slog.Warn("cloudquery hub unreachable; using the newest cached version", "plugin", r.Display(), "version", v, "err", err)
		}
		version = v
	}
	dir := cfg.entryDir(r, version)
	mu, _ := fetchLocks.LoadOrStore(dir, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	if fp, err := loadCached(dir, r); err == nil {
		fp.FromCache = true
		return fp, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		slog.Warn("discarding an unusable cached plugin", "dir", dir, "err", err)
	}
	var fp *FetchedPlugin
	var err error
	switch r.Registry {
	case RegistryHub:
		fp, err = fetchFromHub(ctx, r, version, cfg, dir)
	case RegistryGitHub:
		fp, err = fetchFromGitHub(ctx, r, version, cfg, dir)
	default:
		return nil, fmt.Errorf("plugin %s is not downloadable", r.Display())
	}
	if err != nil {
		return nil, fmt.Errorf("plugin %s: %w", r.Display(), err)
	}
	return fp, nil
}

// cachedVersions lists the versions of r already in the cache, oldest first.
func cachedVersions(cfg fetchConfig, r PluginRef) []string {
	entries, err := os.ReadDir(filepath.Join(cfg.CacheDir, r.Registry, r.Team, r.Name))
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join(cfg.CacheDir, r.Registry, r.Team, r.Name, e.Name(), platform(), "meta.json")); err == nil {
			out = append(out, e.Name())
		}
	}
	sort.Slice(out, func(i, j int) bool { return semverLess(out[i], out[j]) })
	return out
}

func semverLess(a, b string) bool {
	pa, pb := semverParts(a), semverParts(b)
	for i := range 3 {
		if pa[i] != pb[i] {
			return pa[i] < pb[i]
		}
	}
	return a < b
}

func semverParts(v string) [3]int {
	var out [3]int
	core := strings.SplitN(strings.TrimPrefix(v, "v"), "-", 2)[0]
	for i, p := range strings.SplitN(core, ".", 3) {
		out[i], _ = strconv.Atoi(p)
	}
	return out
}

// verifiedBinaries memoizes binaries whose sha256 matched this process's run.
var verifiedBinaries sync.Map

func loadCached(dir string, r PluginRef) (*FetchedPlugin, error) {
	data, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, err
	}
	var fp FetchedPlugin
	if err := json.Unmarshal(data, &fp); err != nil {
		return nil, fmt.Errorf("meta.json: %w", err)
	}
	if r.SHA256 != "" && fp.ArchiveSHA256 != r.SHA256 {
		return nil, fmt.Errorf("cached archive sha256 %s does not match the pin %s", fp.ArchiveSHA256, r.SHA256)
	}
	fp.Binary = filepath.Join(dir, "plugin"+exeSuffix())
	st, err := os.Stat(fp.Binary)
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s|%d|%d", fp.Binary, st.Size(), st.ModTime().UnixNano())
	if _, ok := verifiedBinaries.Load(key); !ok {
		sum, err := fileSHA256(fp.Binary)
		if err != nil {
			return nil, err
		}
		if sum != fp.BinarySHA256 {
			return nil, fmt.Errorf("cached binary sha256 %s differs from the recorded %s", sum, fp.BinarySHA256)
		}
		verifiedBinaries.Store(key, true)
	}
	return &fp, nil
}

func fileSHA256(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// httpError is a non-2xx answer.
type httpError struct {
	URL    string
	Status int
	Body   string
}

func (e *httpError) Error() string {
	msg := fmt.Sprintf("GET %s: HTTP %d", e.URL, e.Status)
	if e.Body != "" {
		msg += ": " + e.Body
	}
	return msg
}

// redactURL drops the query string (signed download URLs carry credentials).
func redactURL(u string) string {
	if i := strings.IndexAny(u, "?#"); i >= 0 {
		return u[:i]
	}
	return u
}

// get performs a GET with retries on 429/5xx, honouring Retry-After.
func get(ctx context.Context, rawURL string, header http.Header) (*http.Response, error) {
	var lastErr error
	for attempt := range 4 {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		for k, v := range header {
			req.Header[k] = v
		}
		req.Header.Set("User-Agent", "vgi-cloudquery/"+Version)
		resp, err := httpClient.Do(req)
		if err != nil {
			inner := err
			var ue *url.Error
			if errors.As(err, &ue) {
				inner = ue.Err // *url.Error repeats the URL, query string included
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = fmt.Errorf("GET %s: %w", redactURL(rawURL), inner)
		} else if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			wait := time.Duration(1<<attempt) * time.Second
			if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s >= 0 && s <= 60 {
				wait = time.Duration(s) * time.Second
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			_ = resp.Body.Close()
			lastErr = &httpError{URL: redactURL(rawURL), Status: resp.StatusCode, Body: strings.TrimSpace(string(body))}
			if attempt < 3 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(wait):
				}
			}
			continue
		} else if resp.StatusCode/100 != 2 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			_ = resp.Body.Close()
			return nil, &httpError{URL: redactURL(rawURL), Status: resp.StatusCode, Body: strings.TrimSpace(string(body))}
		} else {
			return resp, nil
		}
		if attempt < 3 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(1<<attempt) * time.Second):
			}
		}
	}
	return nil, lastErr
}

func hubHeader(cfg fetchConfig) http.Header {
	h := http.Header{"Accept": {"application/json"}}
	if cfg.APIKey != "" {
		h.Set("Authorization", "Bearer "+cfg.APIKey)
	}
	return h
}

// latestHubVersion asks the Hub for a plugin's latest version. The plugin
// endpoint is public: no API key is needed.
func latestHubVersion(ctx context.Context, r PluginRef, cfg fetchConfig) (string, error) {
	if r.Registry != RegistryHub {
		return "", fmt.Errorf("only Hub plugins resolve a latest version")
	}
	u := fmt.Sprintf("%s/plugins/%s/source/%s", strings.TrimRight(cfg.HubURL, "/"), url.PathEscape(r.Team), url.PathEscape(r.Name))
	resp, err := get(ctx, u, hubHeader(cfg))
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		LatestVersion string `json:"latest_version"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return "", fmt.Errorf("decoding %s: %w", u, err)
	}
	if !versionRe.MatchString(body.LatestVersion) {
		return "", fmt.Errorf("the Hub reported no usable latest version for %s/%s", r.Team, r.Name)
	}
	return body.LatestVersion, nil
}

func fetchFromHub(ctx context.Context, r PluginRef, version string, cfg fetchConfig, dir string) (*FetchedPlugin, error) {
	base := strings.TrimRight(cfg.HubURL, "/")
	rel := fmt.Sprintf("plugins/%s/source/%s/versions/%s/assets/%s", url.PathEscape(r.Team), url.PathEscape(r.Name), url.PathEscape(version), platform())
	u := base + "/" + rel
	if cfg.Team != "" {
		u = fmt.Sprintf("%s/teams/%s/%s", base, url.PathEscape(cfg.Team), rel)
	}
	resp, err := get(ctx, u, hubHeader(cfg))
	if err != nil {
		var he *httpError
		if errors.As(err, &he) {
			switch he.Status {
			case http.StatusUnauthorized, http.StatusForbidden:
				if cfg.APIKey == "" {
					return nil, fmt.Errorf("CloudQuery Hub downloads need an API key (the Hub answers %d to anonymous downloads, free plugins included): "+
						"CREATE SECRET (TYPE cloudquery_hub, API_KEY '...') and run SELECT * FROM <catalog>.main.install_plugin('%s'), "+
						"or use a local binary or a github: ref", he.Status, r.Display())
				}
				return nil, fmt.Errorf("the CloudQuery Hub refused the API key (HTTP %d); check the key, its team, and whether the plugin needs a paid plan", he.Status)
			case http.StatusNotFound:
				return nil, fmt.Errorf("the CloudQuery Hub has no %s/%s %s for %s", r.Team, r.Name, version, platform())
			}
		}
		return nil, err
	}
	var asset struct {
		Location string `json:"location"`
		Checksum string `json:"checksum"`
	}
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&asset)
	_ = resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("decoding the Hub asset response: %w", err)
	}
	if asset.Location == "" {
		return nil, fmt.Errorf("the Hub returned no download location")
	}
	if asset.Checksum == "" && r.SHA256 == "" {
		return nil, fmt.Errorf("the Hub published no checksum for this asset; pin one with #sha256:<hex> to download it")
	}
	member := fmt.Sprintf("plugin-%s-%s-%s-%s%s", r.Name, version, runtime.GOOS, runtime.GOARCH, exeSuffix())
	return downloadArchive(ctx, asset.Location, nil, strings.ToLower(asset.Checksum), r.SHA256, []string{member}, version, dir)
}

func fetchFromGitHub(ctx context.Context, r PluginRef, version string, cfg fetchConfig, dir string) (*FetchedPlugin, error) {
	base := strings.TrimRight(cfg.GitHubURL, "/")
	var releaseURL, asset string
	var members []string
	if r.Team == "cloudquery" {
		releaseURL = fmt.Sprintf("%s/cloudquery/cloudquery/releases/download/plugins-source-%s-%s", base, r.Name, version)
		asset = fmt.Sprintf("%s_%s.zip", r.Name, platform())
		members = []string{"plugins/source/" + r.Name + exeSuffix()}
	} else {
		releaseURL = fmt.Sprintf("%s/%s/cq-source-%s/releases/download/%s", base, r.Team, r.Name, version)
		asset = fmt.Sprintf("cq-source-%s_%s.zip", r.Name, platform())
		members = []string{"cq-source-" + r.Name + exeSuffix()}
	}
	published := ""
	if sums, err := get(ctx, releaseURL+"/checksums.txt", nil); err == nil {
		sc := bufio.NewScanner(io.LimitReader(sums.Body, 1<<20))
		for sc.Scan() {
			f := strings.Fields(sc.Text())
			if len(f) == 2 && strings.TrimPrefix(f[1], "*") == asset {
				published = strings.ToLower(f[0])
			}
		}
		_ = sums.Body.Close()
	} else {
		var he *httpError
		if !errors.As(err, &he) || he.Status != http.StatusNotFound {
			return nil, err
		}
	}
	if published == "" && r.SHA256 == "" {
		return nil, fmt.Errorf("release %s publishes no checksum for %s (no checksums.txt entry); pin one with #sha256:<hex> to download it", redactURL(releaseURL), asset)
	}
	fp, err := downloadArchive(ctx, releaseURL+"/"+asset, nil, published, r.SHA256, members, version, dir)
	var he *httpError
	if errors.As(err, &he) && he.Status == http.StatusNotFound {
		return nil, fmt.Errorf("no %s asset for %s %s on GitHub (the cloudquery monorepo stopped attaching plugin binaries to releases in early 2024; newer versions are on the Hub)", platform(), r.Name, version)
	}
	return fp, err
}

// downloadArchive downloads a plugin zip, checks its sha256 against the
// published checksum and the pin, and unpacks the plugin binary into dir.
func downloadArchive(ctx context.Context, rawURL string, header http.Header, published, pin string, members []string, version, dir string) (*FetchedPlugin, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(dir, ".download-*.zip")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	defer func() { _ = tmp.Close() }()
	resp, err := get(ctx, rawURL, header)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("downloading %s: %w", redactURL(rawURL), err)
	}
	if resp.ContentLength >= 0 && n != resp.ContentLength {
		return nil, fmt.Errorf("downloading %s: got %d of %d bytes", redactURL(rawURL), n, resp.ContentLength)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if published != "" && sum != published {
		return nil, fmt.Errorf("archive sha256 %s does not match the published checksum %s; refusing to run it", sum, published)
	}
	if pin != "" && sum != pin {
		return nil, fmt.Errorf("archive sha256 %s does not match the pinned sha256 %s; refusing to run it", sum, pin)
	}
	zr, err := zip.OpenReader(tmp.Name())
	if err != nil {
		return nil, fmt.Errorf("opening the plugin archive: %w", err)
	}
	defer func() { _ = zr.Close() }()
	var file *zip.File
	var regular []*zip.File
	for _, f := range zr.File {
		if f.FileInfo().Mode().IsRegular() {
			regular = append(regular, f)
		}
		for _, m := range members {
			if path.Clean(f.Name) == m {
				file = f
			}
		}
	}
	if file == nil && len(regular) == 1 {
		file = regular[0]
	}
	if file == nil {
		return nil, fmt.Errorf("the plugin archive holds no %s", strings.Join(members, " or "))
	}
	rc, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	bin, err := os.CreateTemp(dir, ".plugin-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.Remove(bin.Name()) }()
	bh := sha256.New()
	if _, err := io.Copy(io.MultiWriter(bin, bh), io.LimitReader(rc, 2<<30)); err != nil {
		_ = bin.Close()
		return nil, fmt.Errorf("unpacking the plugin: %w", err)
	}
	if err := bin.Close(); err != nil {
		return nil, err
	}
	if err := os.Chmod(bin.Name(), 0o755); err != nil {
		return nil, err
	}
	target := filepath.Join(dir, "plugin"+exeSuffix())
	if err := os.Rename(bin.Name(), target); err != nil {
		return nil, err
	}
	fp := &FetchedPlugin{
		Binary: target, Version: version, ArchiveSHA256: sum, BinarySHA256: hex.EncodeToString(bh.Sum(nil)),
		SourceURL: redactURL(rawURL), FetchedAt: time.Now().UTC().Format(time.RFC3339),
	}
	meta, err := json.MarshalIndent(fp, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), meta, 0o644); err != nil {
		return nil, err
	}
	slog.Info("fetched cloudquery plugin", "source", fp.SourceURL, "sha256", sum)
	return fp, nil
}
