"""Inkbox gateway for the SRJ project family.

Holds one Inkbox tunnel open per identity and turns inbound mail into rows in
project_bridge within a second or two of arrival, instead of within a day.

WHY A TUNNEL RATHER THAN AN OPEN PORT. The SDK dials out to Inkbox over HTTP/2
and inbound traffic rides back down that same connection. Nothing listens on a
public address, no port is forwarded, no static IP is needed, and the machine
stays as reachable from the internet as it was before this ran, which is not at
all. Auth is the identity-scoped API key, so revoking the key revokes the
tunnel.

WHY THIS DOES NOT REPLACE inkbox_pull. Two reasons, and both matter.

First, A2A tasks are not a webhook channel. Inkbox publishes mail, text,
iMessage, call.ended and inbound-call, and nothing else. An agent-to-agent task
is stored and read, never pushed, so the only way to learn of one is to ask.
The pull stage remains the only path for A2A.

Second, a webhook that fires while this container is down is simply gone. The
pull stage is the reconciler that sweeps up anything missed. Push and poll are
a pair here, not alternatives, and they share inkbox_sync_log so whichever sees
a message first wins and the other silently skips it.

RUNNING. docker compose up -d, after bootstrap.py has minted the signing keys
once and you have put them in .env. The container needs outbound HTTPS to
inkbox.ai and outbound Postgres to srj-audit-db. It needs no inbound anything.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import threading
import time

import psycopg
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from inkbox import Inkbox, verify_webhook

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class _DropTunnelIdleNoise(logging.Filter):
    """The tunnel data plane logs every idle long-poll slot recycling as a
    WARNING: /_system/intake slot=N -> status=408 reason='intake-idle-cap'.
    Thirty-two slots every few minutes buries the one line that matters, the
    'stored <subject>' when real mail arrives. These are not errors, they are
    what a connected, waiting tunnel looks like, so drop them. A genuine tunnel
    fault logs a different message and still gets through."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "intake-idle-cap" not in msg and "/_system/intake" not in msg


# Attached to the root logger so it catches the SDK's logger too, whatever it
# is named, not just this module's.
logging.getLogger().addFilter(_DropTunnelIdleNoise())

log = logging.getLogger("gateway")

LOCAL_PORT = int(os.environ.get("GATEWAY_PORT", "8080"))

# handle -> the project_bridge mailbox it feeds. These must match
# project_registry.project_key, because that is what a session looks itself up
# by. Kept identical to ibIdentities in srj-pipeline/twoai_inkbox.go; if you add
# an identity, add it in both places or the two paths will disagree.
IDENTITIES = {
    "theworldofai": {"mailbox": "theworldofai@inkboxmail.com", "project": "theworldofai"},
    "srj": {"mailbox": "srj@inkboxmail.com", "project": "srj"},
    "coordinator": {"mailbox": "coordinator@inkboxmail.com", "project": "coordinator"},
}


def env_key(handle: str) -> str:
    return os.environ.get("INKBOX_KEY_" + handle.upper().replace("-", "_"), "")


def env_secret(handle: str) -> str:
    return os.environ.get("INKBOX_SIGNING_" + handle.upper().replace("-", "_"), "")


def db():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg.connect(url)


app = FastAPI()


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/hook/{handle}")
async def hook(handle: str, request: Request):
    ident = IDENTITIES.get(handle)
    if not ident:
        raise HTTPException(status_code=404, detail="unknown identity")

    raw = await request.body()
    secret = env_secret(handle)
    if not secret:
        # Refusing is the only safe answer. Accepting unverified webhooks would
        # let anyone who learns the tunnel URL write into the bridge, and the
        # bridge is what a session trusts at start.
        log.error("%s: no signing secret configured, rejecting", handle)
        raise HTTPException(status_code=503, detail="receiver not configured")
    if not verify_webhook(payload=raw, headers=request.headers, secret=secret):
        log.warning("%s: signature rejected", handle)
        raise HTTPException(status_code=403, detail="bad signature")

    payload = json.loads(raw)
    if payload.get("event_type") != "message.received":
        return {"ignored": payload.get("event_type")}

    msg = (payload.get("data") or {}).get("message") or {}
    # id is the row id and the same value the pull stage records. message_id is
    # the RFC 5322 header and is NOT interchangeable; using it here would break
    # dedup against the pull stage and double-post every email.
    msg_id = msg.get("id")
    if not msg_id:
        log.error("%s: message has no id; keys were %s", handle, sorted(msg))
        raise HTTPException(status_code=422, detail="no message id")

    subject = msg.get("subject") or "(no subject)"
    sender = msg.get("from_address") or msg.get("from") or ""
    body_text = msg.get("body") or ""
    if msg.get("body_truncated"):
        body_text += "\n\n[truncated by Inkbox; fetch the full message by id]"

    stored = record(
        kind="mail",
        external_id=str(msg_id),
        project=ident["project"],
        topic=subject,
        body=f"Inkbox mail to {ident['mailbox']}\nFrom: {sender}\n\n{body_text}",
    )
    log.info("%s: %s %s", handle, "stored" if stored else "duplicate", subject)
    return {"stored": stored}


# Render sends platform events here. The tunnel already gives this container a
# stable public HTTPS address, so Render can reach it without anything being
# exposed on the host; the same property that makes the Inkbox path work.
#
# WHY ONLY FAILURES REACH THE BRIDGE. A cron that succeeds every day would
# otherwise write 365 rows a year that say nothing happened, and a bridge full
# of routine success is a bridge a session stops reading. Successes are logged
# and dropped. This mirrors the log discipline adopted across the pipeline on
# 2026-08-18: a row should mean something happened.
#
# WHAT THIS CANNOT SEE, and it matters. Render reports the PROCESS EXIT CODE.
# `pipeline all` deliberately ignores individual stage failures so one bad
# source cannot block the site build, so a run where six stages failed still
# exits 0 and arrives here as "succeeded". This endpoint catches the run dying,
# hanging, or never starting. It does NOT catch a stage failing inside a
# healthy run; only the pipeline's own run ledger can do that.
RENDER_EVENTS_TO_BRIDGE = {
    "cron_job_run_ended",
    "job_run_ended",
    "build_ended",
    "deploy_ended",
    "server_failed",
    "server_hardware_failure",
    "postgres_unavailable",
    "postgres_backup_failed",
    "postgres_restore_failed",
    "postgres_wal_archive_failed",
    "postgres_read_replica_stale",
    "pipeline_minutes_exhausted",
    "image_pull_failed",
    "branch_deleted",
}


def verify_render(raw: bytes, headers, secret: str) -> bool:
    """Standard Webhooks signature check.

    The signed string is `{webhook-id}.{webhook-timestamp}.{body}`, HMAC-SHA256
    under the signing secret, base64 in a `v1,<sig>` header that may carry
    several space-separated versions during a secret rotation.

    Render's docs render the signed string as ending in `.SIGNING_SECRET`,
    which reads as concatenation rather than an HMAC key. Rather than guess
    between the two and fail silently in a retry loop we compute both and
    accept either, comparing with compare_digest so a wrong signature cannot be
    distinguished by timing. Belt and braces on a receiver that writes to the
    bridge is the right trade.
    """
    wid = headers.get("webhook-id", "")
    wts = headers.get("webhook-timestamp", "")
    sig_header = headers.get("webhook-signature", "")
    if not (wid and wts and sig_header):
        return False

    # Reject anything older than five minutes, so a captured delivery cannot be
    # replayed later.
    try:
        if abs(time.time() - int(wts)) > 300:
            log.warning("render: timestamp outside the 5 minute window")
            return False
    except ValueError:
        return False

    body = raw.decode("utf-8", "replace")
    key = secret
    if key.startswith("whsec_"):
        key = key[len("whsec_"):]
    try:
        key_bytes = base64.b64decode(key)
    except Exception:
        key_bytes = key.encode()

    candidates = {
        base64.b64encode(
            hmac.new(key_bytes, f"{wid}.{wts}.{body}".encode(), hashlib.sha256).digest()
        ).decode(),
        base64.b64encode(
            hmac.new(
                secret.encode(), f"{wid}.{wts}.{body}".encode(), hashlib.sha256
            ).digest()
        ).decode(),
        base64.b64encode(
            hashlib.sha256(f"{wid}.{wts}.{body}.{secret}".encode()).digest()
        ).decode(),
    }
    for part in sig_header.split():
        got = part.split(",", 1)[-1]
        for want in candidates:
            if hmac.compare_digest(got, want):
                return True
    return False


@app.post("/hook/render")
async def render_hook(request: Request):
    raw = await request.body()
    secret = os.environ.get("RENDER_WEBHOOK_SECRET", "")
    if not secret:
        log.error("render: no signing secret configured, rejecting")
        raise HTTPException(status_code=503, detail="receiver not configured")
    if not verify_render(raw, request.headers, secret):
        log.warning("render: signature rejected")
        raise HTTPException(status_code=403, detail="bad signature")

    payload = json.loads(raw)
    etype = payload.get("type", "")
    data = payload.get("data") or {}
    status = data.get("status")
    service = data.get("serviceName") or data.get("serviceId") or "unknown service"

    # An "ended" event that ended well is not news.
    if status == "succeeded":
        log.info("render: %s %s succeeded, not bridging", service, etype)
        return {"ignored": "succeeded"}
    if etype not in RENDER_EVENTS_TO_BRIDGE:
        log.info("render: %s ignored", etype)
        return {"ignored": etype}

    # webhook-id is stable across Render's eight retries, so the same incident
    # cannot post twice even if our first 2xx was lost in transit.
    event_id = request.headers.get("webhook-id") or data.get("id") or ""
    if not event_id:
        raise HTTPException(status_code=422, detail="no event id")

    topic = f"Render: {service} {etype}" + (f" ({status})" if status else "")
    body = (
        f"Render platform event, delivered to the theworldofai tunnel.\n\n"
        f"Service: {service}\n"
        f"Event: {etype}\n"
        f"Status: {status or 'n/a'}\n"
        f"Occurred: {payload.get('timestamp', 'unknown')}\n"
        f"Event id: {data.get('id', event_id)}\n\n"
        "Fetch the full detail from the Render API Retrieve event endpoint with "
        "that event id, and read the run log in the dashboard before drawing a "
        "conclusion: this payload is deliberately thin and says only that "
        "something ended badly, not why.\n\n"
        "IMPORTANT: for srj-pipeline, a 'succeeded' run does NOT mean every "
        "stage worked. The daily sequence ignores individual stage failures so "
        "one bad source cannot block the site build. This alert catches the run "
        "dying or never starting; it cannot catch a stage failing inside a "
        "healthy run."
    )
    stored = record(
        kind="render", external_id=str(event_id), project="theworldofai",
        topic=topic, body=body,
    )
    log.info("render: %s %s %s", "stored" if stored else "duplicate", service, etype)
    return {"stored": stored}


def record(kind: str, external_id: str, project: str, topic: str, body: str) -> bool:
    """Write to the bridge exactly once. Shares inkbox_sync_log with the Go
    pull stage, so whichever path sees a message first wins."""
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO inkbox_sync_log (kind, external_id, to_project, topic)
               VALUES (%s,%s,%s,%s) ON CONFLICT (kind, external_id) DO NOTHING
               RETURNING id""",
            (kind, external_id, project, topic),
        )
        if cur.fetchone() is None:
            return False
        cur.execute(
            """INSERT INTO project_bridge (from_project, to_project, topic, body)
               VALUES ('inkbox', %s, %s, %s)""",
            (project, topic, body),
        )
    return True


def ensure_subscription(client: Inkbox, handle: str, mailbox: str) -> None:
    """Point message.received at this identity's own tunnel. Idempotent: an
    existing subscription with the same URL is left alone rather than replaced,
    so restarting the container does not fan out duplicate deliveries."""
    url = f"https://{handle}.inkboxwire.com/hook/{handle}"
    mb = client.mailboxes.get(mailbox)
    existing = client.webhooks.subscriptions.list(mailbox_id=mb.id)
    for sub in getattr(existing, "items", existing) or []:
        if getattr(sub, "url", None) == url:
            log.info("%s: subscription already points at the tunnel", handle)
            return
    client.webhooks.subscriptions.create(
        mailbox_id=mb.id, url=url, event_types=["message.received"]
    )
    log.info("%s: subscription created -> %s", handle, url)


def serve_local() -> None:
    uvicorn.run(app, host="127.0.0.1", port=LOCAL_PORT, log_level="warning")


def main() -> None:
    missing = [h for h in IDENTITIES if not env_key(h)]
    if missing:
        raise SystemExit(f"missing INKBOX_KEY_* for: {', '.join(missing)}")

    threading.Thread(target=serve_local, daemon=True).start()
    # Give uvicorn a moment before the tunnels start forwarding to it, or the
    # first delivery after a restart races the local listener and 502s.
    time.sleep(2)

    listeners = []
    for handle, meta in IDENTITIES.items():
        client = Inkbox(api_key=env_key(handle))
        try:
            ensure_subscription(client, handle, meta["mailbox"])
        except Exception as exc:  # a failed subscription must not stop the others
            log.error("%s: subscription setup failed: %s", handle, exc)
        listener = client.tunnels.connect(
            name=handle,
            forward_to=f"http://127.0.0.1:{LOCAL_PORT}",
            on_status=lambda s, h=handle: log.info("%s tunnel: %s", h, s),
        )
        log.info("%s tunnel up at %s", handle, listener.public_url)
        listeners.append(listener)

    for listener in listeners:
        listener.wait()


if __name__ == "__main__":
    main()
