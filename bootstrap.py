"""Mint one webhook signing key per identity and print the .env lines.

Run this ONCE. Inkbox returns a signing key in plaintext at creation and never
again, so a second run rotates the key and silently breaks verification for
every subscription already pointing at this gateway. The script refuses to
rotate a key that is already configured unless you pass --rotate, which is the
guard rail rather than a suggestion.

Output goes to stdout so you can read it and paste into .env. It is not written
to a file, because a secret that writes itself to disk in the repo directory is
a secret that reaches GitHub eventually.
"""

import os
import sys

from inkbox import Inkbox

HANDLES = ["theworldofai", "srj", "coordinator"]


def env_key(handle: str) -> str:
    return os.environ.get("INKBOX_KEY_" + handle.upper().replace("-", "_"), "")


def main() -> None:
    rotate = "--rotate" in sys.argv
    lines = []
    for handle in HANDLES:
        key = env_key(handle)
        if not key:
            print(f"# {handle}: no INKBOX_KEY_{handle.upper()}, skipped", file=sys.stderr)
            continue
        client = Inkbox(api_key=key)
        status = client.signing_keys.get_status(handle)
        if getattr(status, "configured", False) and not rotate:
            print(
                f"# {handle}: signing key already exists and was NOT rotated.\n"
                f"#   If you no longer have its value, re-run with --rotate and\n"
                f"#   expect every existing subscription for {handle} to start\n"
                f"#   failing verification until this gateway restarts.",
                file=sys.stderr,
            )
            continue
        created = client.signing_keys.create_or_rotate(handle)
        secret = getattr(created, "signing_key", None) or getattr(created, "key", None)
        if not secret:
            print(f"# {handle}: could not read the key off {type(created).__name__}; "
                  f"fields: {dir(created)}", file=sys.stderr)
            continue
        lines.append(f"INKBOX_SIGNING_{handle.upper().replace('-', '_')}={secret}")

    if lines:
        print("\n# Paste these into .env, then never print them again.")
        print("\n".join(lines))


if __name__ == "__main__":
    main()
