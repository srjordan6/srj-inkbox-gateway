# srj-inkbox-gateway

Turns inbound Inkbox mail into `project_bridge` rows within a second or two of
arrival, instead of within a day, by holding one Inkbox tunnel open per
identity.

## What it does and does not do

| Channel | Path | Latency |
|---|---|---|
| Email | webhook to the tunnel, this container | seconds |
| A2A tasks | `inkbox_pull` stage in srj-pipeline | the cron interval |

**A2A is not a webhook channel.** Inkbox publishes `message.*`, `text.*`,
`imessage.*`, `call.ended` and inbound-call, and nothing for agent-to-agent.
Tasks are stored and read. No gateway changes that, so the pull stage stays.

The pull stage is also the reconciler. A webhook that fires while this
container is down is gone; the next pull picks the message up. Both paths write
through `inkbox_sync_log`, unique on `(kind, external_id)`, so whichever sees a
message first wins and the other skips it. They cannot double-post.

## Setup

Once, on the host, with the `INKBOX_KEY_*` values in the environment:

    cp .env.example .env
    # fill in the three INKBOX_KEY_* and DATABASE_URL
    docker compose run --rm gateway python bootstrap.py

`bootstrap.py` mints one webhook signing key per identity and prints the three
`INKBOX_SIGNING_*` lines. Inkbox returns a signing key in plaintext exactly
once. Paste them into `.env` immediately; there is no way to read them back.
The script refuses to rotate a key that already exists unless you pass
`--rotate`, because rotating breaks verification for every subscription already
pointing here.

Then:

    docker compose up -d
    docker compose logs -f

Expect three `tunnel up at https://{handle}.inkboxwire.com` lines and, on first
run only, three `subscription created` lines.

## Verifying

Send an email to `theworldofai@inkboxmail.com`. The log should show
`theworldofai: stored <subject>` within a couple of seconds, and the row should
appear in `project_bridge` with `from_project = 'inkbox'`.

If the log shows `duplicate`, the pull stage got there first. That is the dedup
working, not a fault.

## Security notes

- **No published ports.** Inbound traffic arrives down the tunnel's own
  outbound HTTP/2 connection. Nothing on this machine listens on a public or
  LAN address, and no port is forwarded. Publishing 8080 would only expose the
  receiver to your LAN.
- **Unsigned payloads are refused**, with 503 if no secret is configured and
  403 if the signature fails. Anyone who learns a tunnel URL can POST to it,
  and the bridge is what a Claude session trusts at start, so an unverified
  write is a write into that trust.
- **Runs as uid 10001**, not root. It needs outbound HTTPS and outbound
  Postgres and nothing else.
- Revoking an identity's API key revokes its tunnel. There is no separate
  per-tunnel secret to rotate.

## If you add an identity

Add it to `IDENTITIES` here **and** to `ibIdentities` in
`srj-pipeline/twoai_inkbox.go`. Two lists, one truth; if they drift, one path
delivers a message the other cannot dedup.
