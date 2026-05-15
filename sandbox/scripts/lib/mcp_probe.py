#!/usr/bin/env python3
"""MCP stdio probe for p2p-sync-mcp.

Spawns the MCP server, walks the standard handshake, then exercises
`sync.ping` (= local, no daemon) and `sync.health-check` (= via daemon RPC).
Exit code 0 on all-pass, 1 on any assertion failure.

Usage:
    python3 mcp_probe.py /path/to/p2p-sync-mcp
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any


PROTOCOL_VERSION = "2024-11-05"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} /path/to/p2p-sync-mcp", file=sys.stderr)
        return 2
    bin_path = argv[1]
    if not os.path.isfile(bin_path) or not os.access(bin_path, os.X_OK):
        print(f"FAIL: not executable: {bin_path}", file=sys.stderr)
        return 1

    env = os.environ.copy()
    # 落ち着いた log level (= stderr が大量に出ないように)
    env.setdefault("P2P_SYNC_LOG", "warn")

    proc = subprocess.Popen(
        [bin_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        # stderr は捨てる (= 起動 banner / log を出す)
        stderr=subprocess.DEVNULL,
        env=env,
        text=True,
        bufsize=1,
    )

    def send(msg: dict[str, Any]) -> None:
        assert proc.stdin is not None
        proc.stdin.write(json.dumps(msg) + "\n")
        proc.stdin.flush()

    def recv() -> dict[str, Any]:
        assert proc.stdout is not None
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError("MCP server closed stdout before responding")
        return json.loads(line)

    fail = 0

    def step(name: str, ok: bool, detail: str = "") -> None:
        nonlocal fail
        marker = "✓" if ok else "✗"
        print(f"    {marker} {name}{(' (' + detail + ')') if detail else ''}")
        if not ok:
            fail += 1

    try:
        # 1. initialize
        send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "install-smoke", "version": "0.1"},
                },
            }
        )
        resp = recv()
        step(
            "initialize",
            "result" in resp,
            f"server={resp.get('result', {}).get('serverInfo', {}).get('name', '?')}",
        )

        # 2. notifications/initialized (no response expected)
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        # 3. tools/list
        send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        resp = recv()
        tools = resp.get("result", {}).get("tools", [])
        names = sorted(t["name"] for t in tools)
        expected_names = sorted(
            [
                "sync.ping",
                "sync.health-check",
                "sync.status",
                "sync.invite",
                "sync.accept-invite",
                "sync.allow-peer",
                "sync.list-peers",
                "sync.revoke",
                "sync.list-pending",
                "sync.recent-log",
            ]
        )
        step(
            "tools/list",
            names == expected_names,
            f"count={len(names)}",
        )
        if names != expected_names:
            missing = sorted(set(expected_names) - set(names))
            extra = sorted(set(names) - set(expected_names))
            print(f"      missing: {missing}", file=sys.stderr)
            print(f"      extra:   {extra}", file=sys.stderr)

        # 4. tools/call sync.ping (= 単体で完結、 daemon 不要)
        send(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "sync.ping", "arguments": {}},
            }
        )
        resp = recv()
        content = resp.get("result", {}).get("content", [])
        text = content[0].get("text", "") if content else ""
        step("sync.ping", text == "pong", f"got={text!r}")

        # 5. tools/call sync.health-check (= daemon RPC)
        send(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "sync.health-check", "arguments": {}},
            }
        )
        resp = recv()
        result = resp.get("result", {})
        is_error = result.get("isError", False)
        content = result.get("content", [])
        text = content[0].get("text", "") if content else ""
        if is_error:
            step(
                "sync.health-check",
                False,
                f"daemon RPC failed: {text[:200]}",
            )
        else:
            try:
                info = json.loads(text)
                ok = bool(info.get("watched_dir_exists")) and info.get("key_path")
                detail = f"watched_dir_exists={info.get('watched_dir_exists')} group_initialized={info.get('dynamic_info', {}).get('group_initialized')}"
                step("sync.health-check", ok, detail)
            except json.JSONDecodeError as e:
                step("sync.health-check", False, f"non-JSON content: {e}")

    finally:
        if proc.stdin:
            proc.stdin.close()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    if fail != 0:
        print(f"\nMCP probe FAIL ({fail} step(s) failed)", file=sys.stderr)
        return 1
    print("\nMCP probe: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
