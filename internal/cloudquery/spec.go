// Plugin specs.
//
// A CloudQuery source plugin is configured with its plugin-specific spec: the
// object under `spec:` in a CloudQuery source config (`item_concurrency: 10`,
// AWS regions, API tokens, ...). The plugin receives it as JSON in its Init
// call. This worker takes it from two places:
//
//   - the `spec` ATTACH option: non-secret settings, a JSON or YAML object
//     keyed by plugin schema ({"hackernews": {"item_concurrency": 10}}), used
//     to discover tables at ATTACH and for every query;
//   - a `cloudquery` secret: credentials, a JSON or YAML object deep-merged
//     over the ATTACH settings at query time (a key set in both is taken from
//     the secret).

package cloudquery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// parseObject parses a JSON or YAML object.
func parseObject(src, what string) (map[string]any, error) {
	src = strings.TrimSpace(src)
	if src == "" {
		return map[string]any{}, nil
	}
	var v any
	if strings.HasPrefix(src, "{") {
		dec := json.NewDecoder(strings.NewReader(src))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("%s looks like JSON but does not parse: %w", what, err)
		}
	} else if err := yaml.Unmarshal([]byte(src), &v); err != nil {
		return nil, fmt.Errorf("%s is neither a JSON object nor YAML: %w", what, err)
	}
	if v == nil {
		return map[string]any{}, nil
	}
	m, ok := normalizeYAML(v).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object (key: value pairs), not %T", what, v)
	}
	return m, nil
}

// normalizeYAML turns yaml's map[any]any (non-string keys) into
// map[string]any so the value can be marshalled as JSON.
func normalizeYAML(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = normalizeYAML(e)
		}
		return x
	case map[any]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[fmt.Sprint(k)] = normalizeYAML(e)
		}
		return out
	case []any:
		for i, e := range x {
			x[i] = normalizeYAML(e)
		}
		return x
	}
	return v
}

// parseAttachSpec parses the `spec` ATTACH option: one object per schema.
func parseAttachSpec(src string) (map[string]map[string]any, error) {
	top, err := parseObject(src, "spec option")
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]any{}
	for schema, v := range top {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("spec option: %q must map to an object holding that plugin's spec, e.g. {\"%s\": {\"key\": \"value\"}}", schema, schema)
		}
		out[strings.ToLower(schema)] = m
	}
	return out, nil
}

// deepMerge returns base with overlay's keys laid over it; nested objects
// merge, any other value is replaced.
func deepMerge(base, overlay map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(overlay))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overlay {
		if bm, ok := out[k].(map[string]any); ok {
			if om, ok := v.(map[string]any); ok {
				out[k] = deepMerge(bm, om)
				continue
			}
		}
		out[k] = v
	}
	return out
}

// specJSON renders a spec as canonical JSON (sorted keys), the bytes sent to
// the plugin's Init and hashed into the process pool key.
func specJSON(spec map[string]any) ([]byte, error) {
	if spec == nil {
		spec = map[string]any{}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(spec); err != nil {
		return nil, fmt.Errorf("encoding the plugin spec: %w", err)
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}
