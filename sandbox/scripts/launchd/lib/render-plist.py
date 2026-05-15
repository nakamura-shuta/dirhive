#!/usr/bin/env python3
"""Render the dirhive launchd plist using `plistlib`.

Usage:
    render-plist.py <HOME> <BIN_PATH> <WATCH_DIR>

stdout = the rendered plist XML (= ready to write to
`~/Library/LaunchAgents/com.user.dirhive.plist`).

Why `plistlib` instead of sed-substituting a template (= Phase 5 review H1):
- A path containing `&` makes `sed` interpret it as "the matched text",
  silently producing `__WATCH__` instead of the path.
- XML reserved characters (`<`, `>`, `&`, `"`) in path-likes corrupt the
  output even when `sed` itself succeeds.
- `plistlib.dump` handles XML escaping and type encoding for us.
"""

from __future__ import annotations

import plistlib
import sys


LABEL = "com.user.dirhive"


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print(
            f"usage: {argv[0]} <HOME> <BIN_PATH> <WATCH_DIR>",
            file=sys.stderr,
        )
        return 2
    home, bin_path, watch = argv[1], argv[2], argv[3]

    data = {
        "Label": LABEL,
        "ProgramArguments": [bin_path, "--watch", watch],
        # boot 時 + 異常終了時に auto start
        "RunAtLoad": True,
        "KeepAlive": True,
        # SIGTERM 後 15s で SIGKILL (= daemon 10s graceful budget + 5s buffer)
        "ExitTimeOut": 15,
        # crash loop 防止 (= 起動失敗時 10s 待って再起動)
        "ThrottleInterval": 10,
        # launchd の stdout/stderr redirect 先。 daemon 本体の file appender
        # (= ~/Library/Logs/dirhive.log) とは **別 file** にする。
        "StandardOutPath": f"{home}/Library/Logs/dirhive.stdout.log",
        "StandardErrorPath": f"{home}/Library/Logs/dirhive.stderr.log",
        "EnvironmentVariables": {
            "HOME": home,
            "DIRHIVE_LOG": "info,dirhive=debug",
        },
    }

    plistlib.dump(data, sys.stdout.buffer, sort_keys=False)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
