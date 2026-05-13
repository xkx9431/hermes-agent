# mq9 as the Transport + Discovery Layer for A2A in Hermes

Status: Proposal (revision 2 — reframed from generic-MQ to mq9-specific)
Companion to: [a2a-protocol-integration.md](a2a-protocol-integration.md)
Upstream issue: [NousResearch/hermes-agent#514](https://github.com/NousResearch/hermes-agent/issues/514)
Source thinking: RobustMQ / mq9 vs A2A relationship essay (May 2026)
Target: Make mq9 (RobustMQ) the default transport-and-discovery substrate for Hermes's A2A integration, so Hermes does **not** re-implement HTTP server, SSE, Agent Card hosting, registry, or push-notification plumbing.

---

## 0. The Reframe

The previous revision of this plan treated "MQ" as a generic async sibling of A2A and proposed building broker abstractions over NATS / Redis / Kafka. That was the wrong framing.

The correct framing, after re-reading the mq9 design notes:

> **A2A and mq9 are not peers. They live on different layers.**
> A2A is the application protocol (Task / Message / Artifact / state machine).
> mq9 is the transport + discovery infrastructure that A2A's spec deliberately leaves to the ecosystem.
> They compose. A2A spec does not change. mq9 carries A2A bytes opaquely.

This single shift removes most of what Hermes would otherwise have to build for #514.

---

## 1. The Five Gaps in A2A That mq9 Closes

A2A spec is intentionally minimal. The following are explicit non-goals of the spec, left to "the ecosystem":

| # | Gap in A2A | What today's options look like | What mq9 provides |
|---|---|---|---|
| 1 | **Discovery** beyond a hardcoded URL | hardcoded URLs, internal wiki, MCP-as-registry hack, custom KV | Built-in agent registry with tag + semantic search; AgentCards stored opaquely |
| 2 | **Reliable async delivery** | SSE (drops on disconnect), webhook (needs public endpoint, no spec'd retry), polling (wasteful) | Persistent mailboxes with TTL + reliable delivery |
| 3 | **State recovery for long-running Tasks** | `tasks/resubscribe` requires server to re-stream full history | Mailbox replays on subscribe; key-based compaction keeps the latest cumulative state |
| 4 | **N-to-N collaboration** | Each agent maintains URL list, connection pool, retries | Mailbox composition is topology-agnostic |
| 5 | **Per-framework re-implementation tax** | Every framework writes client + server + Card hosting + SSE + state machine + registry + auth + observability | One mq9 SDK; framework adapters are thin wrappers |

Hermes's #514 plan, as written today, requires us to solve all five of these inside the Hermes repo. Adopting mq9 as the transport layer means we solve **zero** of them in Hermes — the SDK does it.

---

## 2. The Layering Rule (and what it forbids)

```text
┌─────────────────────────────────────────────────────────┐
│  A2A application protocol                               │
│  (Agent Card, Task, Message, Artifact, state machine,   │
│   INPUT_REQUIRED, Parts, push-notification config)      │
│                                                          │
│  Owned by: A2A working group. Hermes is a consumer.     │
└──────────────────────────┬──────────────────────────────┘
                           │  opaque bytes
┌──────────────────────────┴──────────────────────────────┐
│  mq9 transport + discovery                              │
│  (mailbox, TTL, key compaction, full-snapshot replay,   │
│   registry with tag + semantic search)                  │
│                                                          │
│  Owned by: RobustMQ. Hermes is a consumer.              │
└─────────────────────────────────────────────────────────┘
```

Four hard rules — these are the anti-patterns the original plan would have walked into:

1. **mq9 transport never parses A2A messages.** It carries opaque bytes. If A2A spec adds a new field tomorrow, mq9 doesn't change and Hermes's mq9 plugin doesn't change.
2. **mq9 does not try to standardize Agent registries.** The A2A working group is debating that (Discussion #741). mq9 stores AgentCards as opaque blobs; if A2A defines a registry standard later, mq9 either complies or interoperates.
3. **mq9 is not the only protocol mq9 carries.** The plugin must keep MCP / proprietary-protocol carriage on the table. Betting Hermes on a single Agent protocol is a known fragility.
4. **Use A2A's spec'd extension points; do not modify the spec.**
   - A2A's `pushNotification.url` is RFC 3986; any URL scheme is allowed.
   - We define `mq9://` and route it in the SDK: `https://` → webhook, `mq9://` → mailbox.
   - A2A working group does nothing. A2A spec changes nothing.
   - This is the "use space the spec leaves" path, not the "change the spec" path.

If a proposed change would force a modification to A2A spec or to the mq9 protocol layer, it is in the wrong layer and must be redesigned.

---

## 3. Mapping Hermes #514 Phases onto mq9 Primitives

`#514` proposed three Hermes phases. Each one shrinks dramatically when mq9 carries the transport.

### 3.1 Phase 1 — Hermes as A2A client

| | Hermes-only plan (what #514 proposes) | Hermes + mq9 plan |
|---|---|---|
| Discovery tool | `a2a_discover` — query a hardcoded URL list, fetch `/.well-known/agent.json`, cache it | `mq9.discover(query="agents that can write Python")` — semantic search over the registry |
| Send tool | `a2a_call` — `a2a-sdk` JSON-RPC client, HTTP pool, SSE parser | `mq9.send(target.mailbox, a2a_message_bytes)` |
| Receive results | SSE handler with reconnect logic | `mq9.subscribe(callback_mailbox)` — replay on reconnect is automatic |
| Hermes net-new code | ~400–600 lines (HTTP client, SSE, Card cache, error taxonomy) | ~80 lines (tool wrappers around mq9 SDK) |
| Hermes optional deps added | `a2a-sdk`, `httpx`, `sse-starlette` (client) | `mq9-sdk` (single dep) |

### 3.2 Phase 2 — Hermes as A2A server

| | Hermes-only plan | Hermes + mq9 plan |
|---|---|---|
| Hosting | New HTTP server, port allocation, `/.well-known/agent.json` route, A2A endpoints | `mq9.register(card)` at startup; `mq9.subscribe(self.mailbox)` for inbound tasks |
| Card generation | Build AgentCard from `toolsets.py`, expose via well-known | Same generation; published into mq9 registry |
| Public reachability | TLS cert, reverse proxy, firewall config | None — connection is outbound to the mq9 broker |
| Hermes net-new code | New gateway adapter (~600 lines) + Agent Card generator | Worker module (~150 lines) + same Agent Card generator |
| Operational story | "Open a port" — hard for laptops, NAT'd users, CI | "Connect to the broker" — works behind NAT |

### 3.3 Phase 3 — Multi-agent orchestration

| | Hermes-only plan | Hermes + mq9 plan |
|---|---|---|
| Registry | Hermes-internal table | mq9 registry already has it |
| Capability matching | We write capability-matching logic | mq9 semantic search returns ranked candidates |
| Fan-out | Hermes orchestrates HTTP calls + SSE streams in parallel | Mailbox composition; replay handles slow/partial responses |
| Hermes net-new code | `delegate_task(transport="a2a")` glue + aggregator | `delegate_task(transport="mq9")` glue + aggregator |

The aggregator and `delegate_task` extension are still Hermes work — but the substrate underneath them is no longer Hermes work.

---

## 4. The mq9 Plugin for Hermes

Layout follows the existing Hermes plugin conventions (see [REPO_UNDERSTANDING.md](../REPO_UNDERSTANDING.md) §9–10 and the `plugins/memory/` precedent).

### 4.1 Files

| File | Purpose |
|---|---|
| `plugins/mq9/plugin.yaml` | Plugin manifest (kind: standalone) |
| `plugins/mq9/__init__.py` | `register(ctx)` — wires tools, lifecycle hooks, CLI subcommand |
| `plugins/mq9/transport.py` | Thin facade over the `mq9-sdk`: `register/discover/send/subscribe` |
| `plugins/mq9/url_scheme.py` | `mq9://` URL parser/builder; bridges A2A `pushNotification.url` |
| `plugins/mq9/agent_card.py` | Build AgentCard from Hermes toolsets + skin metadata |
| `plugins/mq9/worker.py` | Inbound consumer: subscribe → spawn `AIAgent` per Task → publish events |
| `plugins/mq9/tools.py` | `mq9_discover`, `mq9_send_task`, `mq9_register_self`, `mq9_steer`, `mq9_cancel` |
| `plugins/mq9/cli.py` | `hermes mq9 register|discover|status|worker` subcommands |
| `tests/plugins/mq9/…` | Adapter contract tests (skip if broker unavailable) |

Note: **no new directories outside `plugins/mq9/`**. Per the May 2026 plugin rule (Teknium), plugins MUST NOT modify core files. The only Hermes-core touch is the optional `[mq9]` extra in `pyproject.toml`.

### 4.2 The mailbox conventions

mq9 mailbox names are opaque to A2A but follow a documented Hermes convention:

```text
hermes/agent/{agent_id}/inbox                  # tasks targeted at this agent
hermes/agent/{agent_id}/task/{tid}/events      # outbound status stream
hermes/agent/{agent_id}/task/{tid}/control     # inbound steering / cancel
hermes/agent/{agent_id}/task/{tid}/input       # INPUT_REQUIRED responses
```

mq9's per-mailbox features map cleanly:

| A2A need | mq9 mailbox setting |
|---|---|
| Replayable Task events | full-snapshot push on subscribe + key-compaction on `task_id` |
| TTL on stale Task channels | mailbox TTL = Task deadline |
| INPUT_REQUIRED resumability | dedicated `input` mailbox with longer TTL |
| Cancel mid-task | non-persistent mailbox, latest-wins |

### 4.3 Tool schemas (sketch)

```python
# plugins/mq9/tools.py — registered via ctx.register_tool(...)

mq9_discover = {
    "name": "mq9_discover",
    "description": "Find agents in the mq9 registry by tag or semantic query.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Free-text capability query."},
            "tags":  {"type": "array",  "items": {"type": "string"}},
            "limit": {"type": "integer", "default": 5},
        },
    },
}

mq9_send_task = {
    "name": "mq9_send_task",
    "description": "Send an A2A Task to an agent over mq9. Returns task_id and "
                   "the final state when await_completion is true.",
    "parameters": {
        "type": "object",
        "properties": {
            "agent_id": {"type": "string"},
            "message":  {"type": "string"},
            "task_id":  {"type": "string"},
            "await_completion": {"type": "boolean", "default": True},
            "timeout_seconds":  {"type": "number",  "default": 120},
            "replay_from_seq":  {"type": "integer", "default": 0},
        },
        "required": ["agent_id", "message"],
    },
}

mq9_steer = {
    "name": "mq9_steer",
    "description": "Push a steering message into a running remote Task. "
                   "Closes the gap that A2A SSE leaves open (no parent → child push).",
    "parameters": {
        "type": "object",
        "properties": {
            "agent_id": {"type": "string"},
            "task_id":  {"type": "string"},
            "text":     {"type": "string"},
        },
        "required": ["agent_id", "task_id", "text"],
    },
}
```

### 4.4 The `mq9://` URL scheme bridge

This is the cleanest interop story with vanilla A2A clients.

```python
# plugins/mq9/url_scheme.py
"""
mq9://<broker-host>[:port]/<mailbox-path>?ttl=<sec>&compact_key=<key>

Hermes publishes its AgentCard with:

  pushNotificationConfig.url = "mq9://broker.example.com/hermes/agent/abc123/inbox"

A vanilla A2A SDK that knows the mq9:// scheme will route push notifications
through mq9 instead of HTTP webhooks. A2A SDKs that don't know it fall back to
their default behaviour (typically: ignore unknown scheme).

A2A spec is unchanged. The mq9 SDK provides the resolver.
"""

def build(broker: str, mailbox: str, ttl: int | None = None) -> str: ...
def parse(url: str) -> dict: ...
def is_mq9(url: str) -> bool: return url.startswith("mq9://")
```

---

## 5. Worker Skeleton (the only stateful piece)

```python
# plugins/mq9/worker.py
"""
Long-lived Hermes worker: subscribes to its inbox, spawns AIAgent per A2A Task,
publishes events back. mq9 transport is opaque to A2A semantics.
"""

import asyncio, json, logging, uuid

from run_agent import AIAgent
from plugins.mq9.transport import Mq9Transport
from plugins.mq9.agent_card import build_agent_card

logger = logging.getLogger(__name__)

INBOX  = "hermes/agent/{aid}/inbox"
EVENTS = "hermes/agent/{aid}/task/{tid}/events"
CTRL   = "hermes/agent/{aid}/task/{tid}/control"


class Mq9Worker:
    def __init__(self, transport: Mq9Transport, agent_id: str):
        self._mq = transport
        self._aid = agent_id
        self._running: dict[str, AIAgent] = {}

    async def run(self) -> None:
        await self._mq.connect()
        await self._mq.register(build_agent_card(self._aid))
        async for envelope in self._mq.subscribe(INBOX.format(aid=self._aid)):
            asyncio.create_task(self._handle(envelope))

    async def _handle(self, envelope: dict) -> None:
        # mq9 carries opaque bytes; we deserialize the A2A message at the edge.
        a2a_msg = json.loads(envelope["payload"])
        task_id = a2a_msg.get("taskId") or str(uuid.uuid4())
        agent = AIAgent(platform="mq9", session_id=f"mq9:{task_id}")
        self._running[task_id] = agent

        events_mb = EVENTS.format(aid=self._aid, tid=task_id)
        ctrl_task = asyncio.create_task(self._consume_control(task_id, agent))

        try:
            await self._emit(events_mb, task_id, 0, "working", final=False)
            user_text = self._extract_text(a2a_msg)
            result = await asyncio.to_thread(agent.chat, user_text)
            await self._emit(events_mb, task_id, 1, "completed",
                             text=result, final=True)
        except Exception as e:
            await self._emit(events_mb, task_id, 1, "failed",
                             text=str(e), final=True)
        finally:
            ctrl_task.cancel()
            self._running.pop(task_id, None)

    async def _consume_control(self, task_id: str, agent: AIAgent) -> None:
        ctrl_mb = CTRL.format(aid=self._aid, tid=task_id)
        async for msg in self._mq.subscribe(ctrl_mb):
            ctrl = json.loads(msg["payload"])
            if   ctrl["kind"] == "cancel": agent.interrupt()
            elif ctrl["kind"] == "steer":  agent.queue_user_message(ctrl["text"])
            elif ctrl["kind"] == "pause":  agent.pause()
            elif ctrl["kind"] == "resume": agent.resume()

    async def _emit(self, mailbox: str, task_id: str, seq: int, state: str,
                    text: str = "", final: bool = False) -> None:
        # A2A-shaped payload; mq9 doesn't care about its shape.
        payload = json.dumps({
            "taskId": task_id, "seq": seq, "state": state,
            "parts": [{"type": "text", "text": text}] if text else [],
            "final": final,
        }).encode()
        await self._mq.publish(mailbox, payload, compact_key=task_id)

    @staticmethod
    def _extract_text(a2a_msg: dict) -> str:
        return "\n".join(
            p.get("text", "") for p in (a2a_msg.get("parts") or [])
            if isinstance(p, dict) and p.get("type") == "text"
        ).strip()
```

`AIAgent.queue_user_message`, `pause`, `resume` are minimal additions to core. They are independently useful (CLI, TUI, gateway), so they aren't mq9-only churn.

---

## 6. Phased Delivery (Hermes side)

| Phase | Deliverable | Net-new Hermes code |
|---|---|---|
| **0** | mq9 plugin scaffold + Agent Card generator from `toolsets.py` | ~150 lines |
| **1** | Outbound: `mq9_discover`, `mq9_send_task` tools (read-only A2A client) | ~120 lines |
| **2** | Inbound: `Mq9Worker` + `hermes mq9 worker` CLI command | ~200 lines |
| **3** | Mid-task control: `mq9_steer`, `mq9_cancel`; `AIAgent.queue_user_message` core hook | ~100 lines |
| **4** | INPUT_REQUIRED dual-mailbox pattern through SDK helpers | ~150 lines |
| **5** | Bridge to Hermes messaging gateway: human → Telegram → agent-A → mq9 → agent-B → back | Design work, then ~200 lines |

Total Hermes-side code: roughly **900–1000 LOC** plugin + minor core hooks. Compare to the original #514 estimate of ~3500 LOC for HTTP server + Agent Card hosting + SSE + registry + capability-matching.

---

## 7. The Five Open Questions (carried forward from the source thinking)

These are not blockers; they are decisions to make as we go.

### 7.1 A2A is pre-1.0 (v0.3.24). What if the spec breaks?

**Low impact for mq9 transport.** mq9 carries opaque bytes; A2A spec changes don't touch the transport. Hermes's A2A serialization code (which lives at the edge of the worker, not inside mq9) may need updates. Pin `a2a-sdk` version in the plugin; bump deliberately.

### 7.2 What if A2A working group standardizes a registry?

**Three viable paths**, decide once the standard is concrete:

1. mq9 registry implements the standard (cheap if standard is API-shaped).
2. mq9 registry interoperates via a bridge.
3. mq9 registry stays the intra-org option; A2A standard handles cross-org.

To stay flexible: store AgentCards opaquely (no schema enforcement), keep the discovery API minimal, do not encode A2A vocabulary into mq9 internals.

### 7.3 Token-by-token streaming through mailboxes

Three implementation options to benchmark in Phase 3:

| Strategy | Storage | Latency | Recovery |
|---|---|---|---|
| One message per token | High | Best | Replay all messages on reconnect |
| Compact-by-task-id (latest cumulative wins) | Low | Best | Fetch latest snapshot — lossy of intermediate tokens |
| Hybrid: per-token messages + periodic snapshot | Medium | Best | Snapshot + tail since snapshot |

Default to **hybrid** unless measured cost says otherwise.

### 7.4 INPUT_REQUIRED across mailboxes

Two-mailbox pattern (server status mailbox + client input mailbox, both keyed by `task_id`). The worker pauses by awaiting on the input mailbox. SDK encapsulates the switch; the agent-author API is just `await agent.request_input(prompt)`.

### 7.5 Hermes messaging gateway × mq9 — combined topology

Concrete scenario: human DMs the Hermes Telegram bot → agent A delegates to remote agent B over mq9 → B's reply flows back through mq9 to A → A replies on Telegram.

Required: messages travelling over mq9 carry an opaque `origin_context` field with the original gateway platform and user. The mq9 plugin attaches it on outbound; the worker propagates it back on inbound; the Telegram adapter uses it to route replies to the right user.

This is a Phase 5 deliverable. Not blocking.

---

## 8. What Hermes Explicitly Does NOT Do

This list matters. It is the discipline that keeps the layering clean.

- ❌ Hermes does not run an HTTP server for A2A inbound. mq9 worker subscribes outbound to a broker.
- ❌ Hermes does not host `/.well-known/agent.json`. Cards are published into the mq9 registry.
- ❌ Hermes does not implement an Agent registry. mq9 has one.
- ❌ Hermes does not implement push-notification webhook delivery. A2A's `pushNotification.url` accepting `mq9://` is the delivery.
- ❌ Hermes does not parse A2A messages inside the mq9 plugin transport layer. Parsing happens at the worker edge only.
- ❌ Hermes does not modify A2A spec. We use `pushNotification.url` exactly as spec'd.
- ❌ Hermes does not modify mq9 protocol. We write a plugin against the mq9 SDK.
- ❌ Hermes does not assume mq9 is the only protocol mq9 carries. The plugin's transport layer must remain agnostic of A2A so MCP / future protocols can ride the same plumbing.

---

## 9. Cross-Cutting Concerns (Hermes-specific)

### 9.1 Profile Safety
- Broker URL, agent ID, registered Agent Card, durable subscription names live under `get_hermes_home()`.
- Different Hermes profiles get different agent IDs; mq9 broker auth credentials are per-profile in `.env`.

### 9.2 Prompt-Cache Invariants
- One A2A Task ⇄ one Hermes session (`session_id = "mq9:{task_id}"`).
- Multi-turn within a Task does not rebuild the system prompt.
- Steering messages enter as user-role messages, not system mutations. Cache stays valid (per AGENTS.md prompt-caching rule).

### 9.3 Auth & Network
- All broker creds in `.env` (`HERMES_MQ9_URL`, `HERMES_MQ9_TOKEN`), never `config.yaml`.
- Mandatory TLS for non-localhost brokers; warn on startup otherwise (mirror `api_server`'s `is_network_accessible` check).
- Per-mailbox ACLs at the mq9 broker, not in Hermes — defence in depth.
- Constant-time compare for any in-Hermes shared-secret check.

### 9.4 Failure Modes

| Failure | Detection | Response |
|---|---|---|
| Broker unreachable at startup | `mq9.connect()` raises | Worker exits non-zero; supervisor restarts |
| Broker drops mid-task | SDK reports disconnect | Reconnect with backoff; replay events from mailbox snapshot |
| Worker crash mid-task | mq9 redelivers | Idempotent handler resumes; emits `state=working` again |
| Parent disconnects | Worker keeps running | Parent reattaches via `replay_from_seq` (mailbox replays) |
| Steering arrives after `final=true` | Worker validates state | Drop control frame; emit `ops` warning |
| A2A spec change | Edge serializer fails | Pinned `a2a-sdk` version; deliberate bump in PR |

### 9.5 Interaction with Existing Hermes Surfaces

- `delegate_task` gains an optional `transport="mq9"` mode. Local in-process delegation stays the default.
- The synchronous A2A plan (`tools/a2a_client_tool.py` from the sibling doc) and the mq9 plugin are siblings: a future `agent_call(target=…)` meta-tool resolves the AgentCard, sees `mq9://` in `pushNotification.url`, and routes accordingly.
- The messaging gateway (Telegram/Slack/etc.) is orthogonal until Phase 5 of §6.

---

## 10. Definition of Done

### Per-phase
Same DoD pattern as the sibling A2A plan — each phase ships with: passing contract tests against a real broker, a docs page under `website/docs/user-guide/messaging/mq9.md`, and a `docker-compose.yml` snippet for local broker bring-up.

### Whole-effort acceptance
- `hermes mq9 worker --agent-id alice` exposes Hermes as an A2A-capable agent without opening a port.
- A vanilla A2A client that understands `mq9://` can drive a Hermes worker end-to-end.
- A Hermes parent agent discovers a remote agent via `mq9_discover`, sends a Task via `mq9_send_task`, steers it via `mq9_steer`, and receives streaming events.
- Disconnect / reconnect mid-task does not lose events.
- Net-new Hermes code stays under 1.5k LOC (excluding tests). If it grows past that, the layering rule is being violated.

---

## 11. References

- This thinking re-frames the previous revision of this file, which proposed building a generic NATS/Redis/Kafka MQ abstraction. That direction is abandoned in favour of the mq9-specific plan above.
- Companion sync transport plan: [a2a-protocol-integration.md](a2a-protocol-integration.md)
- Repo navigation: [REPO_UNDERSTANDING.md](../REPO_UNDERSTANDING.md)
- Upstream Hermes A2A discussion: [issue #514](https://github.com/NousResearch/hermes-agent/issues/514)
- A2A spec extension point used: `pushNotificationConfig.url` (RFC 3986 — accepts any scheme)
- mq9 / RobustMQ project home: <https://robustmq.com/> · <https://github.com/robustmq/robustmq>
- Existing Hermes patterns this plan follows:
  - Plugin layout: [plugins/memory/](../plugins/memory/)
  - Agent Card generation source: [toolsets.py](../toolsets.py)
  - Profile-safe paths: [hermes_constants.py](../hermes_constants.py)
  - Plugin must-not-modify-core rule: AGENTS.md "Plugins" section
