#!/usr/bin/env python3
"""Check primary routing and convergence on each requested read replica."""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid


def request(base, token, path, sql=None, primary=False):
    headers = {"Authorization": "Bearer " + token}
    data = None
    if sql is not None:
        data = json.dumps({"sql": sql}).encode()
        headers["Content-Type"] = "application/json"
    if primary:
        headers["X-tinySQL-Consistency"] = "primary"
    req = urllib.request.Request(base.rstrip("/") + path, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as response:
        payload = json.load(response)
        if payload.get("error") or payload.get("success") is False:
            raise RuntimeError(str(payload))
        return payload, response.headers.get("X-tinySQL-Backend")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--replicas", nargs="+", default=["replica1", "replica2"])
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    token = os.environ.get("TINYSQL_AUTH_TOKEN")
    if not token:
        parser.error("TINYSQL_AUTH_TOKEN must be set")
    deadline = time.monotonic() + args.timeout
    while True:
        try:
            with urllib.request.urlopen(args.url.rstrip("/") + "/readyz/write", timeout=2):
                break
        except (urllib.error.URLError, OSError):
            if time.monotonic() >= deadline:
                raise RuntimeError("primary did not become ready")
            time.sleep(0.2)

    table = "cluster_smoke_" + uuid.uuid4().hex
    try:
        # Writes are issued once: a timeout does not prove a write failed.
        request(args.url, token, "/api/exec", f"CREATE TABLE {table} (id INT PRIMARY KEY, label TEXT)")
        request(args.url, token, "/api/exec", f"INSERT INTO {table} VALUES (1,'cluster')")
        sql = f"SELECT id,label FROM {table}"
        expected = [{"id": 1, "label": "cluster"}]
        payload, backend = request(args.url, token, "/api/query", sql, primary=True)
        if backend != "primary" or payload.get("rows") != expected:
            raise RuntimeError("forced-primary read did not observe the write")
        seen = set()
        while seen != set(args.replicas):
            try:
                payload, backend = request(args.url, token, "/api/query", sql)
                if backend in args.replicas and payload.get("rows") == expected:
                    seen.add(backend)
            except (urllib.error.URLError, OSError, RuntimeError):
                pass  # Reads may fail temporarily during bootstrap/reconnect.
            if time.monotonic() >= deadline:
                raise RuntimeError("replicas did not converge: " + ", ".join(sorted(set(args.replicas) - seen)))
            time.sleep(0.1)
        print("Verified primary writes, primary consistency and replicas: " + ", ".join(sorted(seen)))
    finally:
        try:
            request(args.url, token, "/api/exec", f"DROP TABLE IF EXISTS {table}")
        except (urllib.error.URLError, OSError, RuntimeError) as error:
            print(f"Cleanup failed for {table}: {error}", file=sys.stderr)


if __name__ == "__main__":
    main()
