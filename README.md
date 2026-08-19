# srj-inkbox-gateway

Turns inbound Inkbox mail into `project_bridge` rows within a second or two of
arrival, instead of within a day, by holding one Inkbox tunnel open per
identity.

## What it does and does not do

| Channel | Path | Latency |
|---|---|---|
| Email | webhook to the tunnel, this container | seconds |
| Render platform events | webhook to the tunnel, this container | seconds |
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

## Render platform events

`POST /hook/render` accepts Render webhooks and writes failures into
`project_bridge` for `theworldofai`. The tunnel is what makes this possible:
Render needs a public HTTPS endpoint, and the tunnel gives this container one
without exposing anything on the host.

**Only failures reach the bridge.** A cron that succeeds daily would otherwise
write 365 rows a year saying nothing happened, and a bridge full of routine
success is a bridge a session stops reading. `status: succeeded` is logged and
dropped.

**What this cannot see, and it matters more than what it can.** Render reports
the process exit code. `pipeline all` deliberately ignores individual stage
failures so one bad source cannot block the site build, so a run where six
stages failed still exits 0 and arrives here as *succeeded*. This endpoint
catches the run dying, hanging, or never starting. Catching a stage failing
inside a healthy run needs the pipeline's own run ledger, which is a separate
piece of work.

### Setup

1. Render dashboard, workspace home, **Integrations > Webhooks > Create
   Webhook**. Note that **webhooks require a Pro workspace plan or higher**; on
   a lower plan this endpoint will simply never be called and the email
   notification route is the fallback.
2. URL: `https://theworldofai.inkboxwire.com/hook/render`
3. Events: at minimum **Cron Job Run Ended**. Worth adding: Build Ended, Deploy
   Ended, Server Failed, and the Postgres failure events.
4. Copy the **signing secret** from the webhook's Settings page into `.env` as
   `RENDER_WEBHOOK_SECRET`, then `docker compose up -d --build`.

### Verification

The signature check follows Standard Webhooks: HMAC-SHA256 over
`{webhook-id}.{webhook-timestamp}.{body}`, base64, in a `v1,<sig>` header that
may carry several space-separated versions during a rotation. Deliveries older
than five minutes are refused so a captured request cannot be replayed.

Render's published docs render the signed string as ending in `.SIGNING_SECRET`,
which reads as concatenation rather than an HMAC key. Rather than guess between
the two readings and fail silently inside Render's retry-then-disable loop, the
receiver computes both and accepts either, using `compare_digest` throughout.

Tested against synthetic deliveries: a valid signature is accepted, a forged one
rejected, a tampered body rejected, a timestamp outside the window rejected, and
a header carrying two signatures accepted on the matching one.

**Retries are idempotent.** `webhook-id` is stable across Render's eight retry
attempts and is used as the `inkbox_sync_log` external id under kind `render`,
so a lost 2xx cannot post the same incident twice.
