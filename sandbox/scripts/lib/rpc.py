#!/usr/bin/env python3
"""Send 1 RPC to a dirhive daemon Unix socket and print `result` as JSON.

Usage:
    rpc.py <socket_path> <method> [<json_params>]

Exit 0 on RPC `result`, exit 1 on `error` or transport failure. Stdout = the
`result` JSON value as a one-line string. Stderr = error message on failure.

Designed to be called from shell smoke scripts (2peer-smoke etc.).
"""

from __future__ import annotations

import json
import socket
import sys


CONNECT_TIMEOUT = 5.0
RECV_TIMEOUT = 10.0


def main(argv: list[str]) -> int:
    if len(argv) < 3 or len(argv) > 4:
        print(f"usage: {argv[0]} <socket> <method> [<json_params>]", file=sys.stderr)
        return 2
    sock_path, method = argv[1], argv[2]
    params_raw = argv[3] if len(argv) == 4 else "{}"
    try:
        params = json.loads(params_raw)
    except json.JSONDecodeError as e:
        print(f"invalid params JSON: {e}", file=sys.stderr)
        return 2

    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(CONNECT_TIMEOUT)
        s.connect(sock_path)
        s.settimeout(RECV_TIMEOUT)
    except OSError as e:
        print(f"connect {sock_path}: {e}", file=sys.stderr)
        return 1

    req = json.dumps({"method": method, "params": params}) + "\n"
    s.sendall(req.encode())

    buf = b""
    while b"\n" not in buf:
        try:
            chunk = s.recv(8192)
        except socket.timeout:
            print(
                f"RPC {method!r} timed out after {RECV_TIMEOUT}s waiting for response",
                file=sys.stderr,
            )
            return 1
        if not chunk:
            print(f"daemon closed connection before response for {method!r}", file=sys.stderr)
            return 1
        buf += chunk

    line = buf.split(b"\n", 1)[0]
    try:
        resp = json.loads(line)
    except json.JSONDecodeError as e:
        print(f"non-JSON response: {e}\nbody: {line[:200]!r}", file=sys.stderr)
        return 1

    if resp.get("error"):
        print(f"daemon RPC error: {resp['error']}", file=sys.stderr)
        return 1

    print(json.dumps(resp.get("result")))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
