# Hermes × mq9 Plugin — Detailed Implementation Plan

> **Audience:** Hermes core contributors implementing the mq9 plugin, and mq9
> SDK maintainers reviewing the consumer integration.
> **Scope:** Make mq9 the agent communication / registration / discovery
> infrastructure for Hermes, shipped as a Hermes plugin (no core forks).
> **Companion:** `hermes-agent/plans/mq-async-agent-communication.md` (the
> layering thesis). This document is the code-level "how."

---

## 0. Executive Summary

| Concern | Decision |
|---|---|
| Where does the integration live? | A single Hermes plugin at `plugins/mq9/`. Zero modifications to `run_agent.py`, `cli.py`, `gateway/run.py`, `model_tools.py`. |
| What protocol does mq9 carry? | Opaque bytes. The plugin's transport layer never parses A2A / MCP semantics. |
| Where is A2A serialized? | At the worker edge only (`plugins/mq9/codec_a2a.py`). Pluggable so MCP / custom envelopes ride the same plumbing. |
| Identity & discovery | mq9 `AGENT.REGISTER` / `AGENT.DISCOVER`. The Agent body is an A2A AgentCard with a `mailbox` field. |
| Reachability | Outbound NATS connection to the mq9 broker. **No HTTP server**, no port to open, works behind NAT. |
| Reliability | mq9 mailbox = persistent, replayable, key-compactable. Reconnect replays missed events automatically. |
| Hermes net-new code budget | ≤ 1,500 LOC across 9 files (excl. tests). Going past this means the layering rule has been violated. |
| Profile isolation | Broker URL, agent ID, persistent group names live under `get_hermes_home()`. Per-profile `.env` holds the broker token. |
| Prompt-cache safety | One Task ⇄ one Hermes session; steering arrives as user-role messages; no mid-conversation system-prompt rewrites. |

---

## 1. Layer Model (Reference)

```
┌─────────────────────────────────────────────────────────────┐
│  A2A application semantics                                  │
│  Task / Message / Artifact / Parts / state machine /        │
│  pushNotificationConfig                                     │
│                                                              │
│  Hermes side:  codec_a2a.py   (serialize at the edge)       │
└─────────────────────────────────┬───────────────────────────┘
                                  │ opaque bytes
┌─────────────────────────────────┴───────────────────────────┐
│  mq9 transport + registry                                   │
│  $mq9.AI.MAILBOX.* / $mq9.AI.MSG.* / $mq9.AI.AGENT.*        │
│                                                              │
│  Python SDK:  mq9.Mq9Client                                 │
│  Hermes side: transport.py (thin facade) + registry.py      │
└─────────────────────────────────────────────────────────────┘
```

Anti-pattern guardrails enforced by the file boundary:

- `transport.py` MUST NOT `import` from `codec_a2a.py` (would couple transport to A2A).
- `codec_a2a.py` MUST NOT `import` from `transport.py` (would prevent unit-testing serialization in isolation).
- `worker.py` is the only module allowed to import both.

A `tests/plugins/mq9/test_layering.py` test asserts these forbidden imports never appear (using `ast.parse` of the plugin source — no runtime cost).

---

## 2. mq9 → Hermes Concept Map

| mq9 primitive | Hermes use |
|---|---|
| `MAILBOX.CREATE name=hermes.agent.{aid}.inbox ttl=0` | Hermes worker's durable inbox |
| `MAILBOX.CREATE name=hermes.agent.{aid}.task.{tid}.events` (ttl = task deadline) | Outbound A2A status/artifact stream |
| `MAILBOX.CREATE name=hermes.agent.{aid}.task.{tid}.control` (ttl = task deadline) | Inbound steer / cancel / pause |
| `MAILBOX.CREATE name=hermes.agent.{aid}.task.{tid}.input` (ttl ≥ task deadline) | `INPUT_REQUIRED` answers |
| `MSG.SEND` with `mq9-key={task_id}` | Latest-wins state snapshot for `tasks/resubscribe` |
| `MSG.SEND` with `mq9-priority=critical` | Cancel / interrupt control frames |
| `MSG.FETCH` with `group_name=hermes-worker-{aid}` | Worker's durable, resumable consumption |
| `MSG.FETCH` without `group_name`, `deliver=earliest` | Parent agent's replay-from-zero on reconnect |
| `AGENT.REGISTER {AgentCard, mailbox=mq9://broker/hermes.agent.{aid}.inbox}` | Publish self to registry on startup |
| `AGENT.DISCOVER {semantic: "..."}` | `mq9_discover` tool — returns ranked AgentCards |

**Mailbox naming rules** (mq9 `mail_address` spec: `[a-z0-9.]{1,128}`, dots only as visual grouping):

```text
hermes.agent.{aid}.inbox
hermes.agent.{aid}.task.{tid}.events
hermes.agent.{aid}.task.{tid}.control
hermes.agent.{aid}.task.{tid}.input
```

Where `aid` and `tid` must be `[a-z0-9]+` (UUIDs are lowercased and stripped of dashes). The plugin provides `mailbox_names.py` constants/builders so no string is hand-formatted.

---

## 3. Plugin Layout

```
plugins/mq9/
├── plugin.yaml              # manifest
├── README.md                # user-facing setup
├── __init__.py              # register(ctx) — hooks, tools, CLI
├── config_schema.py         # config.yaml keys + .env vars + validation
├── mailbox_names.py         # mailbox naming helpers (single source of truth)
├── url_scheme.py            # mq9:// parser/builder for pushNotification.url
├── transport.py             # Mq9Transport — facade over mq9.Mq9Client
├── registry.py              # AgentCard publish/refresh/discover
├── codec_a2a.py             # A2A ↔ bytes — pluggable; replaces never imports transport
├── agent_card.py            # Build AgentCard from toolsets.py + skin/identity
├── worker.py                # Long-lived inbound consumer; spawns AIAgent per Task
├── tools.py                 # mq9_discover / mq9_send_task / mq9_steer / mq9_cancel
├── cli.py                   # `hermes mq9 …` subcommands
└── tests/                   # contract tests (skip if HERMES_MQ9_URL not set)
    ├── test_layering.py
    ├── test_url_scheme.py
    ├── test_mailbox_names.py
    ├── test_codec_a2a.py
    ├── test_transport_contract.py
    ├── test_worker_lifecycle.py
    └── test_tools_smoke.py
```

Strict file count: **12 source files + tests.** Any new file added later requires a justification on the PR.

---

## 4. `plugin.yaml`

```yaml
name: mq9
version: 0.1.0
description: |
  mq9 transport for inter-agent communication (registration, discovery,
  reliable async messaging). Carries A2A (and any other) protocols opaquely
  over the mq9 broker. Exposes mq9_discover / mq9_send_task / mq9_steer /
  mq9_cancel tools and a `hermes mq9 worker` long-running agent surface.

pip_dependencies:
  - mq9>=0.2.0   # the official Python SDK

# Lifecycle hooks the plugin uses
hooks:
  - on_session_start
  - on_session_end

# Optional config block injected into config.yaml on `hermes setup`
config:
  mq9:
    enabled: false
    broker_url: "nats://localhost:4222"      # mq9 broker NATS URL
    agent_id: ""                             # auto-generated on first run
    auto_register: true
    discover_default_limit: 5
    worker:
      enabled: false                         # set true to expose Hermes as a server
      group_name: ""                         # default: hermes-worker-{agent_id}
      max_concurrent_tasks: 4
      task_timeout_seconds: 900
    mailbox_ttl_seconds: 0                   # 0 = inbox never expires
    events_mailbox_ttl_seconds: 3600
    codec: "a2a"                             # "a2a" | "raw" | "mcp"
```

Secrets live in `.env` (`OPTIONAL_ENV_VARS` registration):

```python
"HERMES_MQ9_TOKEN": {
    "description": "Bearer token / NATS credentials for the mq9 broker. "
                   "Leave blank for unauthenticated local brokers.",
    "prompt": "mq9 Broker Token",
    "url": "https://mq9.robustmq.com/for-engineer",
    "password": True,
    "category": "messaging",
},
```

Per `hermes-agent/AGENTS.md`, non-secret settings (URLs, IDs, timeouts) belong in `config.yaml`. Only the token goes in `.env`.

---

## 5. `mailbox_names.py` — Single Source of Truth

```python
"""mq9 mailbox name builders. ALL mailbox names go through this module."""
from __future__ import annotations
import re

_VALID = re.compile(r"^[a-z0-9](?:[a-z0-9]|\.(?!\.))*[a-z0-9]$")

def _safe(part: str) -> str:
    """Normalize an identifier into the mq9 lowercase-alnum-dot grammar."""
    s = part.lower().replace("-", "").replace("_", "")
    if not s.isalnum():
        raise ValueError(f"identifier {part!r} cannot be expressed as mq9 mail_address")
    return s

def inbox(agent_id: str) -> str:
    name = f"hermes.agent.{_safe(agent_id)}.inbox"
    assert _VALID.match(name) and len(name) <= 128
    return name

def events(agent_id: str, task_id: str) -> str:
    return f"hermes.agent.{_safe(agent_id)}.task.{_safe(task_id)}.events"

def control(agent_id: str, task_id: str) -> str:
    return f"hermes.agent.{_safe(agent_id)}.task.{_safe(task_id)}.control"

def input_mb(agent_id: str, task_id: str) -> str:
    return f"hermes.agent.{_safe(agent_id)}.task.{_safe(task_id)}.input"
```

Test contract: every name returned must satisfy mq9's spec (lowercase, `[a-z0-9.]`, no leading/trailing/consecutive dot, ≤ 128 chars). Property-based test with Hypothesis.

---

## 6. `url_scheme.py` — `mq9://` bridge

```python
"""
Wire format:
    mq9://<broker-host>[:port]/<mail_address>[?ttl=<sec>&compact_key=<key>]

Hermes publishes its AgentCard with:
    pushNotificationConfig.url = mq9://broker.example.com/hermes.agent.abc.inbox

A2A spec is unchanged. mq9 SDKs that recognize the scheme route via mailbox;
SDKs that don't will simply ignore the unknown scheme (per A2A's RFC 3986
permissiveness).
"""
from __future__ import annotations
from dataclasses import dataclass
from urllib.parse import urlparse, parse_qs, urlencode

@dataclass(frozen=True)
class Mq9Url:
    host: str
    port: int | None
    mail_address: str
    ttl: int | None = None
    compact_key: str | None = None

    def to_str(self) -> str:
        netloc = f"{self.host}:{self.port}" if self.port else self.host
        qs = {}
        if self.ttl is not None:        qs["ttl"] = self.ttl
        if self.compact_key is not None: qs["compact_key"] = self.compact_key
        suffix = ("?" + urlencode(qs)) if qs else ""
        return f"mq9://{netloc}/{self.mail_address}{suffix}"

def is_mq9(url: str) -> bool:
    return url.startswith("mq9://")

def parse(url: str) -> Mq9Url:
    if not is_mq9(url):
        raise ValueError(f"not an mq9 URL: {url!r}")
    p = urlparse(url)
    qs = parse_qs(p.query)
    return Mq9Url(
        host=p.hostname or "",
        port=p.port,
        mail_address=p.path.lstrip("/"),
        ttl=int(qs["ttl"][0]) if "ttl" in qs else None,
        compact_key=qs["compact_key"][0] if "compact_key" in qs else None,
    )

def build(broker_host: str, mail_address: str, *, port: int | None = None,
          ttl: int | None = None, compact_key: str | None = None) -> str:
    return Mq9Url(broker_host, port, mail_address, ttl, compact_key).to_str()
```

---

## 7. `transport.py` — The Facade

`transport.py` exists purely to:

1. Construct a single `mq9.Mq9Client` from Hermes config.
2. Provide an async lifecycle (`connect`, `close`) tied to the worker / CLI.
3. Re-export the SDK's `Message`, `Priority`, `Consumer` types so the rest of
   the plugin imports from `transport`, not `mq9` directly. This makes
   stubbing trivial in tests.

```python
from __future__ import annotations
import logging
import os
from typing import AsyncIterator

from mq9 import Mq9Client, Message, Priority, Consumer, Mq9Error  # re-exported

logger = logging.getLogger(__name__)

class Mq9Transport:
    """Thin facade over mq9.Mq9Client. Knows nothing about A2A."""

    def __init__(self, *, broker_url: str, token: str | None = None,
                 request_timeout: float = 5.0):
        self._client = Mq9Client(
            server=broker_url,
            request_timeout=request_timeout,
            # token handed off in NATS URL or via creds env — see SDK docs
        )
        self._connected = False

    async def connect(self) -> None:
        if not self._connected:
            await self._client.connect()
            self._connected = True

    async def close(self) -> None:
        if self._connected:
            await self._client.close()
            self._connected = False

    # Mailbox -------------------------------------------------------------

    async def ensure_mailbox(self, name: str, *, ttl: int = 0) -> str:
        """Create a mailbox if missing; silently succeed if it already exists."""
        try:
            return await self._client.mailbox_create(name=name, ttl=ttl)
        except Mq9Error as exc:
            if "already exists" in str(exc):
                return name
            raise

    # Messaging -----------------------------------------------------------

    async def publish(self, mailbox: str, payload: bytes, *,
                      priority: Priority = Priority.NORMAL,
                      key: str | None = None, ttl: int | None = None,
                      tags: list[str] | None = None) -> int:
        return await self._client.send(
            mailbox, payload, priority=priority, key=key, ttl=ttl, tags=tags,
        )

    async def subscribe(self, mailbox: str, *, group_name: str | None,
                        deliver: str = "earliest") -> AsyncIterator[Message]:
        """Async-iterator wrapper around SDK's pull semantics."""
        while True:
            messages = await self._client.fetch(
                mailbox, group_name=group_name, deliver=deliver,
                num_msgs=32, max_wait_ms=5000,
            )
            for m in messages:
                yield m
                if group_name is not None:
                    await self._client.ack(mailbox, group_name, m.msg_id)

    # Agent registry ------------------------------------------------------

    async def register(self, body: dict) -> None:
        await self._client.agent_register(body)

    async def unregister(self, mailbox_url: str) -> None:
        await self._client.agent_unregister(mailbox_url)

    async def report(self, body: dict) -> None:
        await self._client.agent_report(body)

    async def discover(self, *, text: str | None = None,
                       semantic: str | None = None,
                       limit: int = 5, page: int = 1) -> list[dict]:
        return await self._client.agent_discover(
            text=text, semantic=semantic, limit=limit, page=page,
        )
```

The SDK methods `agent_register / agent_unregister / agent_report / agent_discover` follow the same pattern as the existing `mailbox_create / send / fetch / ack` in `mq9/python/mq9/client.py`. If a method does not yet exist in the SDK, it is added there (not in Hermes).

---

## 8. `codec_a2a.py` — Edge Serialization

The codec is the **only** place that knows A2A. It implements a small interface so MCP or custom envelopes can plug in later by setting `mq9.codec: mcp` in config.

```python
from __future__ import annotations
from typing import Protocol

class TaskCodec(Protocol):
    """Convert wire bytes ↔ structured Task events. NOT an mq9 concept."""
    def decode_inbound(self, payload: bytes) -> "InboundTask": ...
    def encode_event(self, ev: "TaskEvent") -> bytes: ...
    def encode_control(self, ctrl: "ControlFrame") -> bytes: ...
    def decode_control(self, payload: bytes) -> "ControlFrame": ...
    def encode_input_response(self, text: str) -> bytes: ...
    def decode_input_response(self, payload: bytes) -> str: ...

# Concrete A2A codec
class A2ACodec:
    def decode_inbound(self, payload: bytes) -> InboundTask:
        msg = json.loads(payload)
        return InboundTask(
            task_id=msg.get("taskId") or _new_task_id(),
            text="\n".join(p["text"] for p in msg.get("parts", [])
                           if p.get("type") == "text"),
            context_id=msg.get("contextId"),
            metadata=msg.get("metadata") or {},
        )

    def encode_event(self, ev: TaskEvent) -> bytes:
        return json.dumps({
            "taskId": ev.task_id, "seq": ev.seq, "state": ev.state,
            "parts": [{"type": "text", "text": ev.text}] if ev.text else [],
            "final": ev.final, "artifacts": ev.artifacts or [],
        }).encode()
    # … etc.

def get_codec(name: str) -> TaskCodec:
    if name == "a2a": return A2ACodec()
    if name == "raw": return RawCodec()
    if name == "mcp": raise NotImplementedError("Phase 6")
    raise ValueError(f"unknown codec {name!r}")
```

The codec module never imports `transport.py`. This is enforced by `test_layering.py`.

---

## 9. `agent_card.py` — Self-Description

Generated from the live Hermes process state at registration time:

- Identity → `agent_id` from config; `name` from `display.skin.branding.agent_name`.
- `description` → first line of the user's `~/.hermes/AGENTS.md` if present.
- `version` → `hermes.__version__`.
- `capabilities.streaming = true` (mailbox replays events).
- `capabilities.pushNotifications = true` (the mailbox **is** the push target).
- `skills[]` → derived from the resolved toolset list (active core + plugin
  tools + installed skills). Each becomes a `SkillDescriptor` with a `name`,
  `description`, and `tags` synthesized from the tool's toolset key.
- `pushNotificationConfig.url` → `mq9://{broker_host}/{inbox_name}`.

```python
def build_agent_card(agent_id: str, broker_host: str, *, enabled_tools: list[dict],
                     skills_meta: list[dict]) -> dict:
    return {
        "agent_id":    agent_id,
        "name":        get_active_skin().branding.agent_name,
        "description": _read_agents_md_summary(),
        "version":     __version__,
        "capabilities": {
            "streaming": True,
            "pushNotifications": True,
            "stateTransitionHistory": True,
        },
        "skills": [
            {
                "id":   t["name"],
                "name": t["name"],
                "description": t.get("description", ""),
                "tags": [t.get("toolset", "core")],
            }
            for t in enabled_tools
        ] + [
            {"id": s["name"], "name": s["name"],
             "description": s.get("description", ""), "tags": s.get("tags", [])}
            for s in skills_meta
        ],
        "mailbox": build_mq9_url(broker_host, inbox(agent_id)),
        "pushNotificationConfig": {
            "url": build_mq9_url(broker_host, inbox(agent_id)),
        },
    }
```

This card is what `AGENT.REGISTER` carries. mq9 stores it opaquely; only the `mailbox` field is meaningful to the broker for routing.

---

## 10. `worker.py` — The Server-Side Surface

The worker is the **only stateful, long-lived** piece. It is opt-in via
`mq9.worker.enabled: true` and started by `hermes mq9 worker` (or the gateway
runner if we later choose to host it there).

```python
"""
Long-lived mq9 worker. Subscribes to inbox; spawns one AIAgent per Task;
publishes events back; consumes per-task control mailboxes.
"""
import asyncio, json, logging, uuid
from collections.abc import Awaitable
from concurrent.futures import ThreadPoolExecutor

from run_agent import AIAgent
from plugins.mq9 import mailbox_names as mb
from plugins.mq9.codec_a2a import get_codec, TaskEvent, ControlFrame
from plugins.mq9.transport import Mq9Transport, Priority

log = logging.getLogger(__name__)

class Mq9Worker:
    def __init__(self, transport: Mq9Transport, *, agent_id: str,
                 codec_name: str = "a2a", group_name: str | None = None,
                 max_concurrent: int = 4, task_timeout: int = 900):
        self._mq = transport
        self._aid = agent_id
        self._codec = get_codec(codec_name)
        self._group = group_name or f"hermes-worker-{agent_id}"
        self._sem = asyncio.Semaphore(max_concurrent)
        self._timeout = task_timeout
        self._running: dict[str, tuple[AIAgent, asyncio.Task]] = {}
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent,
                                            thread_name_prefix="mq9-task")
        self._stop = asyncio.Event()

    async def run(self) -> None:
        await self._mq.connect()
        await self._mq.ensure_mailbox(mb.inbox(self._aid), ttl=0)
        log.info("mq9 worker listening on %s (group=%s)",
                 mb.inbox(self._aid), self._group)
        async for envelope in self._mq.subscribe(
            mb.inbox(self._aid), group_name=self._group, deliver="earliest",
        ):
            if self._stop.is_set(): break
            await self._sem.acquire()
            asyncio.create_task(self._handle(envelope))

    async def stop(self) -> None:
        self._stop.set()
        for tid, (agent, task) in list(self._running.items()):
            agent.interrupt()
            task.cancel()
        await self._mq.close()
        self._executor.shutdown(wait=False)

    # ---------------- per-Task pipeline ----------------------------------

    async def _handle(self, envelope) -> None:
        try:
            inbound = self._codec.decode_inbound(envelope.payload)
        except Exception as e:
            log.exception("malformed inbound; dropping: %s", e)
            self._sem.release()
            return

        tid = inbound.task_id
        events_mb   = mb.events(self._aid, tid)
        control_mb  = mb.control(self._aid, tid)
        await self._mq.ensure_mailbox(events_mb,
            ttl=int(os.environ.get("HERMES_MQ9_EVENTS_TTL", "3600")))
        await self._mq.ensure_mailbox(control_mb, ttl=self._timeout * 2)

        # One Task ⇄ one Hermes session.  Cache-stable: system prompt is
        # built once for this AIAgent instance and never mutated mid-run.
        agent = AIAgent(
            platform="mq9", session_id=f"mq9:{tid}",
            skip_context_files=False, skip_memory=False,
        )
        ctrl_task = asyncio.create_task(self._consume_control(tid, agent))
        run_task  = asyncio.create_task(self._run_agent(agent, inbound, events_mb))
        self._running[tid] = (agent, run_task)
        try:
            await asyncio.wait_for(run_task, timeout=self._timeout)
        except asyncio.TimeoutError:
            agent.interrupt()
            await self._emit(events_mb, tid, state="failed",
                             text="task exceeded timeout", final=True)
        finally:
            ctrl_task.cancel()
            self._running.pop(tid, None)
            self._sem.release()

    async def _run_agent(self, agent: AIAgent, inbound, events_mb: str) -> None:
        seq = 0
        await self._emit(events_mb, inbound.task_id, seq, "working", final=False)
        try:
            # AIAgent.chat is sync; run in executor so the event loop is free
            # to service control + emit progress.  Streaming progress is a
            # Phase-3 deliverable; v0 emits start/end only.
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self._executor, agent.chat, inbound.text,
            )
            seq += 1
            await self._emit(events_mb, inbound.task_id, seq,
                             "completed", text=result, final=True)
        except Exception as exc:
            seq += 1
            await self._emit(events_mb, inbound.task_id, seq,
                             "failed", text=str(exc), final=True)

    async def _consume_control(self, tid: str, agent: AIAgent) -> None:
        cmb = mb.control(self._aid, tid)
        async for msg in self._mq.subscribe(cmb, group_name=None,
                                            deliver="earliest"):
            try:
                ctrl: ControlFrame = self._codec.decode_control(msg.payload)
            except Exception:
                continue
            if   ctrl.kind == "cancel": agent.interrupt()
            elif ctrl.kind == "steer":  agent.queue_user_message(ctrl.text)
            elif ctrl.kind == "pause":  agent.pause()
            elif ctrl.kind == "resume": agent.resume()

    async def _emit(self, mb_addr: str, task_id: str, seq: int = 0,
                    state: str = "working", *, text: str = "",
                    final: bool = False) -> None:
        ev = TaskEvent(task_id=task_id, seq=seq, state=state,
                       text=text, final=final, artifacts=[])
        # `key=task_id` triggers mq9 latest-wins compaction so `tasks/resubscribe`
        # can pull the current snapshot in one call.
        await self._mq.publish(
            mb_addr, self._codec.encode_event(ev),
            key=task_id,
            priority=Priority.CRITICAL if state == "failed" else Priority.NORMAL,
        )
```

### 10.1 The three minimal core hooks the worker needs

These are added to `AIAgent` (in `run_agent.py`) and are independently useful
for CLI, TUI, and the gateway. They are NOT mq9-specific.

| Method | Behaviour |
|---|---|
| `interrupt()` | Already exists. Sets `_interrupt_requested`. |
| `pause()` | New. Sets `_pause_event.clear()`; main loop awaits it before each API call. |
| `resume()` | New. Sets `_pause_event.set()`. |
| `queue_user_message(text)` | New. Appends a user-role message into a queue drained at the top of every iteration of the main loop. Cache-safe: never rewrites history. |

These four methods are the entire core surface area touched by this plugin.
Per `AGENTS.md` ("plugins MUST NOT modify core files"), these are added as
*generic* extensions, not mq9-specific code paths — exactly the precedent set
by PR #5295.

---

## 11. `tools.py` — Agent-facing Tools

Registered via `ctx.register_tool(...)` in `__init__.py`. All handlers
return JSON strings (Hermes tool contract). None of them touch `AIAgent`
internals — they only call `transport.py`.

| Tool | Purpose |
|---|---|
| `mq9_discover(query, tags?, limit?)` | Calls `AGENT.DISCOVER`. Returns ranked list of AgentCards. |
| `mq9_send_task(agent_id, message, await_completion=true, timeout=120)` | Resolves agent_id → mailbox URL → publishes A2A message bytes → optionally subscribes to events mailbox until `final=true`. |
| `mq9_steer(agent_id, task_id, text)` | Publishes a `steer` control frame to `…task.{tid}.control`. |
| `mq9_cancel(agent_id, task_id)` | Publishes a `cancel` control frame (priority=critical). |
| `mq9_register_self()` | One-shot: rebuild AgentCard from current toolsets and re-publish. |

Each schema is dynamically annotated in `get_tool_definitions()` if a
sibling capability is missing (e.g., `mq9_send_task` description warns when
`worker.enabled = false`, since the agent can dispatch but not receive
replies on its own mailbox). This follows the `browser_navigate` /
`execute_code` post-processing pattern in `model_tools.py` (per the
"DO NOT hardcode cross-tool references" pitfall in `AGENTS.md`).

### 11.1 `mq9_send_task` flow

```text
1. resolve target → AGENT.DISCOVER(text=agent_id) → mailbox URL
2. encode A2A message bytes via codec
3. publish to target.inbox  (priority=normal)
4. if await_completion:
     subscribe to events_mailbox (stateless, deliver="earliest")
     yield events back to the agent until state ∈ {completed,failed,cancelled}
     return final state + concatenated artifacts
   else:
     return {"task_id": tid, "status": "dispatched"}
```

If the broker disconnects mid-await, the SDK reconnects and the mq9 mailbox
replays from the last `mq9-key={task_id}` snapshot. Hermes loses zero
events without writing any reconnection logic itself.

---

## 12. `cli.py` — `hermes mq9 …`

```text
hermes mq9 register         # build card + AGENT.REGISTER (one-shot)
hermes mq9 unregister       # AGENT.UNREGISTER for this profile
hermes mq9 status            # show agent_id, broker, last register time
hermes mq9 worker            # run Mq9Worker in foreground
hermes mq9 worker --daemon   # detach (writes PID to $HERMES_HOME/mq9/worker.pid)
hermes mq9 discover "query"  # CLI front-end to mq9_discover
hermes mq9 send AGENT MSG    # CLI front-end to mq9_send_task
```

Wired via `ctx.register_cli_command(...)` per the general-plugin contract
(`hermes_cli/plugins.py`). No edit to `main.py` (the lesson of PR #5295 is
explicit in `AGENTS.md`).

---

## 13. `__init__.py` — Wiring

```python
"""mq9 plugin registration entry point."""
from __future__ import annotations
import logging
import uuid
from pathlib import Path

from hermes_constants import get_hermes_home
from plugins.mq9 import tools as _tools
from plugins.mq9.cli import build_subparser

log = logging.getLogger(__name__)

def register(ctx):
    cfg = ctx.config.get("mq9", {}) or {}
    if not cfg.get("enabled"):
        log.debug("mq9 plugin disabled in config; skipping registration")
        return

    # Generate + persist agent_id on first run (profile-scoped)
    aid_file = get_hermes_home() / "mq9" / "agent_id"
    aid_file.parent.mkdir(parents=True, exist_ok=True)
    if not cfg.get("agent_id"):
        cfg["agent_id"] = aid_file.read_text().strip() if aid_file.exists() \
            else uuid.uuid4().hex
        aid_file.write_text(cfg["agent_id"])

    # Tools
    for tool_def, handler in _tools.iter_tools(cfg):
        ctx.register_tool(name=tool_def["name"], schema=tool_def,
                          handler=handler, toolset="mq9")

    # Lifecycle hooks
    @ctx.on_session_start
    async def _register_card(session):
        if cfg.get("auto_register"):
            from plugins.mq9.registry import register_self
            try:
                await register_self(cfg)
            except Exception as e:
                log.warning("mq9 auto-register failed: %s", e)

    @ctx.on_session_end
    async def _flush(session):
        from plugins.mq9.registry import flush_report
        try:
            await flush_report(cfg)
        except Exception:
            pass

    # CLI subcommand
    ctx.register_cli_command("mq9", build_subparser, dispatcher=...)
```

Crucially, `register()` does **not** start the worker. Worker startup is a
deliberate, explicit user action (`hermes mq9 worker`). This prevents every
`hermes` invocation from accidentally registering as a callable server.

---

## 14. Toolset Wiring

Per `AGENTS.md`, registering a tool in code is not enough — its name must
appear in a toolset for any agent to see it. Because the plugin must not
modify `toolsets.py`, the plugin declares a new toolset implicitly via
`ctx.register_tool(toolset="mq9")`. Users opt in with:

```yaml
# config.yaml
tools:
  cli:
    enabled: ["mq9", ...]    # adds the mq9 toolset
  messaging:
    enabled: ["mq9", ...]
```

`hermes tools` (the curses UI) will show "mq9" alongside built-ins.

---

## 15. Profile Safety Checklist

Following the rules in `AGENTS.md` "Profiles: Multi-Instance Support":

- ✅ Agent ID file path: `get_hermes_home() / "mq9" / "agent_id"`.
- ✅ Worker PID file: `get_hermes_home() / "mq9" / "worker.pid"`.
- ✅ Group name default: `hermes-worker-{agent_id}` — distinct per profile.
- ✅ User-facing print uses `display_hermes_home()`.
- ✅ Broker token in `.env`, not `config.yaml`.
- ✅ Token scope lock via `gateway.status.acquire_scoped_lock()` keyed on
  `("mq9", broker_url, agent_id)` so two profiles cannot accidentally claim
  the same `(broker, agent_id)` pair.

---

## 16. Prompt-Cache Invariants

Verified against `AGENTS.md` "Prompt Caching Must Not Break":

- One inbound Task = one `AIAgent` instance = one `session_id`. The system
  prompt is built once at `AIAgent.__init__` and is never reloaded.
- `steer` control frames enqueue **user-role** messages via
  `queue_user_message()`. They never mutate the system prompt, the toolset,
  or memory.
- `mq9_register_self` (re-publishing the AgentCard) takes effect for
  **future** inbound Tasks only. The currently-running Task keeps its
  cached prompt. This is the canonical "defer invalidation, opt-in `--now`"
  pattern.

---

## 17. Failure Matrix

| Failure | Where detected | Response |
|---|---|---|
| Broker unreachable at startup | `transport.connect()` raises | `hermes mq9 worker` exits non-zero; systemd / docker restarts |
| Broker disconnects mid-task | `nats-py` SDK reconnects | Subscribe resumes from group offset; events mailbox replays via `key={task_id}` |
| Worker crash mid-task | mq9 redelivers (group offset unchanged because no ACK) | Idempotent: handler re-emits `state=working` then continues |
| Steering arrives after `final=true` | Worker validates Task state | Drop the frame; log warning |
| Inbound payload malformed | `codec.decode_inbound` raises | Drop; log; do NOT ACK so a future codec version may re-process |
| `INPUT_REQUIRED` answer never arrives | TTL on input mailbox | Task times out (configurable); emit `state=failed` with reason |
| Two workers race on same `(broker, agent_id)` | Scoped lock | Second worker prints clear error and exits |
| A2A spec change | Pinned `a2a-sdk` version in codec | Deliberate version bump in a PR; transport/registry untouched |

---

## 18. Testing Strategy

Tests live under `tests/plugins/mq9/`. Two tiers:

### 18.1 Hermetic (always run)

- `test_layering.py` — AST scan of plugin source; forbidden imports must not
  appear.
- `test_url_scheme.py` — round-trip parse/build; reject malformed schemes.
- `test_mailbox_names.py` — Hypothesis property-based; every generated name
  satisfies the mq9 mail_address grammar and length cap.
- `test_codec_a2a.py` — fixed A2A wire samples → decode → re-encode →
  byte-equal (canonical JSON).
- `test_worker_lifecycle.py` — `Mq9Transport` is monkey-patched with an
  in-memory fake. Verifies: register, decode inbound, spawn `AIAgent`
  (mocked), emit `working` + `completed`, ACK semantics, control frame
  processing, timeout path.

### 18.2 Contract (skipped without `HERMES_MQ9_URL`)

- `test_transport_contract.py` — real broker. Sends, fetches, acks,
  registers, discovers. Decorated with
  `pytest.mark.skipif(not os.environ.get("HERMES_MQ9_URL"), ...)`.
- `test_tools_smoke.py` — agent-A registers, agent-B discovers A, sends a
  Task, A replies, B receives final state.

CI runs tier 1 always. A separate workflow brings up the
mq9 broker (`docker-compose -f docs/examples/mq9-broker-compose.yml up -d`)
and runs tier 2. The wrapper `scripts/run_tests.sh` is used everywhere
(per `AGENTS.md` hermetic-test rule); tests never write to real
`~/.hermes/`.

---

## 19. Phased Delivery

| Phase | Deliverable | LOC budget |
|---|---|---|
| **0** | Plugin skeleton: `plugin.yaml`, `__init__.py`, `config_schema.py`, `mailbox_names.py`, `url_scheme.py`, `agent_card.py` (no worker, no tools) | 250 |
| **1** | `transport.py` + `codec_a2a.py` + tier-1 tests | 250 |
| **2** | `tools.py` (`mq9_discover`, `mq9_send_task`) — outbound only | 200 |
| **3** | `worker.py` + `hermes mq9 worker` CLI — inbound | 300 |
| **4** | `mq9_steer`, `mq9_cancel` + core `pause/resume/queue_user_message` hooks in `AIAgent` | 150 |
| **5** | `INPUT_REQUIRED` dual-mailbox helper + Telegram ↔ mq9 origin-context bridge | 200 |
| **6** | MCP codec (`codec_mcp.py`) so mq9 plugin transports MCP, not just A2A | 150 |

Cumulative: ~1,500 LOC excl. tests — within the budget set in §0.

### Definition of Done per phase

Each phase ships with:

1. Contract tests against a real broker (`docker-compose up` snippet in PR).
2. A docs page under `website/docs/user-guide/messaging/mq9.md` (or this
   repo's site, depending on where Hermes docs are hosted at that point).
3. An entry in the plugin's `README.md` describing the new tools / CLI verbs.

### Whole-effort acceptance

- `hermes mq9 worker --agent-id alice` makes Hermes a discoverable, callable
  agent over mq9 **without opening any port**.
- A vanilla A2A client that understands `mq9://` can drive a Hermes worker
  end-to-end. No A2A spec changes.
- A Hermes parent discovers a peer with `mq9_discover`, sends a Task with
  `mq9_send_task`, steers it with `mq9_steer`, and receives streaming
  events. Parent disconnect / reconnect mid-task loses no events.
- All four anti-pattern boundaries enforced by `test_layering.py`.

---

## 20. mq9 SDK Asks (Companion Work)

Implementing the plan above presumes the following methods exist in the
mq9 Python SDK. The current `mq9/python/mq9/client.py` already has
`mailbox_create / send / fetch / ack`; the gaps are:

| Method | Need |
|---|---|
| `agent_register(body: dict)` | Wraps `$mq9.AI.AGENT.REGISTER`. |
| `agent_unregister(mailbox_url: str)` | Wraps `$mq9.AI.AGENT.UNREGISTER`. |
| `agent_report(body: dict)` | Wraps `$mq9.AI.AGENT.REPORT`. |
| `agent_discover(*, text=None, semantic=None, limit=20, page=1)` | Wraps `$mq9.AI.AGENT.DISCOVER`. |
| `query(mail_address, *, key=None, limit=None, since=None)` | Wraps `$mq9.AI.MSG.QUERY.*`. |
| `delete(mail_address, msg_id)` | Wraps `$mq9.AI.MSG.DELETE.*.*`. |

All four AGENT.* methods are thin request/reply wrappers identical in shape
to the existing `mailbox_create`. They are SDK work, not Hermes work.

---

## 21. Open Questions Carried Forward

1. **Streaming granularity.** Token-per-message vs. compact-by-task-id vs.
   hybrid (snapshot + tail). Decision deferred to Phase 3 benchmarking.
2. **Cross-org discovery.** If the A2A working group standardizes a
   registry (Discussion #741), do we (a) make mq9 implement it, (b) bridge,
   or (c) keep mq9 as the intra-org option only? Decision deferred until
   the A2A standard is concrete.
3. **MCP carriage.** Phase 6 ships `codec_mcp.py`; the rest of the plugin
   is already protocol-agnostic.
4. **In-gateway worker.** Whether to allow `gateway.run` to host the mq9
   worker (mirroring `kanban.dispatch_in_gateway`). Default no — keep the
   worker as its own process for failure isolation.
5. **`mq9` as a Hermes messaging-gateway platform.** Today mq9 is exposed
   as tools + a worker. Should it also appear under
   `gateway/platforms/mq9.py` so a human user can DM "an mq9 agent" from
   inside the gateway? Likely yes once Phase 5 is done — that's where the
   origin-context bridge lives anyway.

---

## 22. References

- Layering thesis: `hermes-agent/plans/mq-async-agent-communication.md`
- Hermes plugin conventions: `hermes-agent/AGENTS.md` "Plugins" section
- mq9 protocol spec: `mq9/protocol.md`
- mq9 Python SDK: `mq9/python/mq9/client.py`
- Companion sync transport: `hermes-agent/plans/a2a-protocol-integration.md`
- Hermes profile rules: `hermes-agent/hermes_constants.py`
- Upstream A2A discussion: `https://github.com/NousResearch/hermes-agent/issues/514`
- Cross-tool reference policy: `hermes-agent/AGENTS.md` "DO NOT hardcode cross-tool references"
- Prompt-cache rule: `hermes-agent/AGENTS.md` "Prompt Caching Must Not Break"
