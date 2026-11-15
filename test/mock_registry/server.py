#!/usr/bin/env python3
"""A mock CloudQuery Hub API and GitHub release host serving the fixture plugin.

The worker's download paths are tested against this server, so the suite runs
offline and needs no CloudQuery account. Archives are built from
FIXTURE_PLUGIN for the host platform (MOCK_PLATFORM, e.g. ``darwin_arm64``).

GitHub release layout (``github_url``):

``/cloudquery/cloudquery/releases/download/plugins-source-<name>-<version>/``
    ``<name>_<platform>.zip`` (binary at ``plugins/source/<name>``) and
    ``checksums.txt``. Names:

    - ``fixture``: a correct checksum
    - ``tampered``: checksums.txt lists a different sha256
    - ``nosum``: no checksums.txt at all
``/query-farm/cq-source-fixture/releases/download/<version>/``
    The community layout: ``cq-source-fixture_<platform>.zip`` + ``checksums.txt``.

CloudQuery Hub API (``hub_url``):

``GET /plugins/<team>/source/<name>``
    Public plugin info with ``latest_version`` (no key needed, as on the Hub).
``GET [/teams/<team>]/plugins/<team>/source/fixture/versions/v1.0.0/assets/<platform>``
    401 without ``Authorization: Bearer test-hub-key`` (the real Hub answers 401
    to anonymous downloads); otherwise ``{"location", "checksum"}``. The
    location carries a signed query string the worker must not log.
``GET /stats``
    JSON request counters, for cache tests.

Prints ``PORT:<n>`` and ``SHA256:<sha256 of the fixture GitHub zip>`` once
listening.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

API_KEY = "test-hub-key"
VERSION = "v1.0.0"


def make_zip(member: str, data: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as z:
        info = zipfile.ZipInfo(member, date_time=(2024, 1, 1, 0, 0, 0))
        info.external_attr = 0o755 << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        z.writestr(info, data)
    return buf.getvalue()


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


class Store:
    def __init__(self, binary: bytes, plat: str) -> None:
        self.plat = plat
        goos, goarch = plat.split("_", 1)
        exe = ".exe" if goos == "windows" else ""
        self.files: dict[str, bytes] = {}
        mono = "/cloudquery/cloudquery/releases/download"
        for name in ("fixture", "tampered", "nosum"):
            z = make_zip(f"plugins/source/{name}{exe}", binary)
            base = f"{mono}/plugins-source-{name}-{VERSION}"
            self.files[f"{base}/{name}_{plat}.zip"] = z
            if name == "fixture":
                self.github_sha = sha(z)
                self.files[f"{base}/checksums.txt"] = f"{sha(z)}  {name}_{plat}.zip\n".encode()
            elif name == "tampered":
                self.files[f"{base}/checksums.txt"] = f"{'0' * 64}  {name}_{plat}.zip\n".encode()
            elif name == "nosum":
                self.nosum_sha = sha(z)
        z = make_zip(f"cq-source-fixture{exe}", binary)
        base = f"/query-farm/cq-source-fixture/releases/download/{VERSION}"
        self.files[f"{base}/cq-source-fixture_{plat}.zip"] = z
        self.files[f"{base}/checksums.txt"] = f"{sha(z)}  cq-source-fixture_{plat}.zip\n".encode()
        self.hub_zip = make_zip(f"plugin-fixture-{VERSION}-{goos}-{goarch}{exe}", binary)
        self.files["/download/hub-fixture.zip"] = self.hub_zip
        self.stats: dict[str, int] = {}
        self.lock = threading.Lock()

    def count(self, key: str) -> None:
        with self.lock:
            self.stats[key] = self.stats.get(key, 0) + 1


HUB_INFO = re.compile(r"^/plugins/([^/]+)/source/([^/]+)$")
HUB_ASSET = re.compile(r"^(?:/teams/[^/]+)?/plugins/([^/]+)/source/([^/]+)/versions/([^/]+)/assets/([^/]+)$")


def make_handler(store: Store):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args) -> None:  # quiet
            pass

        def send(self, status: int, body: bytes, ctype: str = "application/octet-stream") -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status: int, obj) -> None:
            self.send(status, json.dumps(obj).encode(), "application/json")

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/stats":
                with store.lock:
                    return self.send_json(200, store.stats)
            m = HUB_INFO.match(path)
            if m:
                store.count("hub_info")
                if m.group(2) != "fixture":
                    return self.send_json(404, {"message": "not found", "status": 404})
                return self.send_json(200, {"name": "fixture", "team_name": m.group(1), "latest_version": VERSION, "tier": "free"})
            m = HUB_ASSET.match(path)
            if m:
                store.count("hub_asset")
                if self.headers.get("Authorization") != f"Bearer {API_KEY}":
                    return self.send_json(401, {"message": "You must be authenticated to access this resource", "status": 401})
                team, name, version, plat = m.groups()
                if name != "fixture" or version != VERSION or plat != store.plat:
                    return self.send_json(404, {"message": "not found", "status": 404})
                host = self.headers.get("Host")
                return self.send_json(200, {"location": f"http://{host}/download/hub-fixture.zip?X-Signature=do-not-log",
                                            "checksum": sha(store.hub_zip)})
            body = store.files.get(path)
            if body is None:
                store.count("404")
                return self.send(404, b"Not Found", "text/plain")
            store.count("download" if path.endswith(".zip") else "checksums")
            return self.send(200, body)

    return Handler


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--port", type=int, default=0)
    args = ap.parse_args()
    binary_path = os.environ.get("FIXTURE_PLUGIN")
    plat = os.environ.get("MOCK_PLATFORM")
    if not binary_path or not plat:
        print("FIXTURE_PLUGIN and MOCK_PLATFORM (e.g. linux_amd64) are required", file=sys.stderr)
        return 2
    with open(binary_path, "rb") as f:
        store = Store(f.read(), plat)
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(store))
    print(f"PORT:{srv.server_address[1]}", flush=True)
    print(f"SHA256:{store.github_sha}", flush=True)
    print(f"NOSUM_SHA256:{store.nosum_sha}", flush=True)
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
