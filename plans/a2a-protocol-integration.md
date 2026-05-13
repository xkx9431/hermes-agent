# A2A (Agent-to-Agent) Protocol Integration Plan

Status: Proposal
Owner: TBD
Target: Add bidirectional [A2A](https://a2a-protocol.org/) protocol support to Hermes Agent so it can both **call** other A2A agents (client) and **be called by** other A2A agents (server).

---

## 1. Goals and Non-Goals

### Goals
- Allow Hermes to invoke any A2A-compliant remote agent as a tool.
- Allow any A2A-compliant client to invoke Hermes as a remote agent.
- Reuse existing Hermes primitives — no parallel state model, no core file rewrites.
- Profile-safe, prompt-cache safe, and consistent with existing gateway/tool conventions.

### Non-Goals (initial release)
- Implementing the full optional A2A surface (agent discovery registries, multi-tenant federation).
- Replacing the existing `delegate_task` in-process delegation pattern.
- Building a hosted A2A directory.

### Why A2A on top of existing delegation
Hermes already supports in-process subagent delegation via `delegate_task`
(see [AGENTS.md](../AGENTS.md) "Delegation" section). A2A is complementary:
it adds **cross-process, cross-host, cross-vendor** agent communication
through an open standard.

---

## 2. Concept Mapping

| A2A concept | Hermes equivalent | Source of truth |
|---|---|---|
| Agent Card (`/.well-known/agent.json`) | Static JSON served by adapter | New `gateway/platforms/a2a.py` |
| `Task` (`id`, `status`, `history`) | Hermes session (`session_key`) | [hermes_state.py](../hermes_state.py), [gateway/session.py](../gateway/session.py) |
| `Message` with `parts` | `MessageEvent` in gateway | [gateway/platforms/base.py](../gateway/platforms/base.py) |
| `tasks/send` JSON-RPC | Inbound message → existing `_process_message_background()` | [gateway/run.py](../gateway/run.py) |
| `tasks/sendSubscribe` (SSE) | Streaming via existing tool/agent callbacks | Reuse TUI/SSE callback machinery |
| `tasks/get`, `tasks/cancel` | Session read; `running_agent.interrupt()` | [gateway/run.py](../gateway/run.py) |
| Auth (Bearer / API key) | Allowlist + token check | Mirror `webhook` / `api_server` adapters |

Key invariant: **one A2A Task = one Hermes session**. Multi-turn within a Task
maps to multi-turn within a session and preserves prompt caching.

---

## 3. Phased Delivery

| Phase | Scope | Risk | Outcome |
|---|---|---|---|
| 1 | Outbound client tool | Low | Hermes can call other A2A agents |
| 2 | Inbound gateway adapter (server) | Medium | External agents can call Hermes |
| 3 | Polish: SSE streaming, push notifications, multi-skill cards | Medium | Spec parity for production use |

Each phase is independently shippable.

---

## 4. Phase 1 — Outbound A2A Client Tool

### 4.1 Files

| File | Purpose |
|---|---|
| `tools/a2a_client_tool.py` | New tool module with `a2a_discover` + `a2a_send_task` handlers |
| `toolsets.py` | Add `"a2a"` toolset entry |
| `hermes_cli/config.py` | Add optional `A2A_BEARER_TOKEN` to `OPTIONAL_ENV_VARS` |
| `tests/tools/test_a2a_client.py` | Unit tests with mocked HTTP |

No existing files are modified beyond `toolsets.py` and `OPTIONAL_ENV_VARS`.

### 4.2 Tool Schemas

`a2a_discover`
- Inputs:
  - `agent_url` (string, required) — base URL of the remote agent
- Behavior: GET `{agent_url}/.well-known/agent.json`
- Output: JSON string containing the Agent Card or `{"error": "..."}`

`a2a_send_task`
- Inputs:
  - `agent_url` (string, required)
  - `message` (string, required) — user-text part to send
  - `task_id` (string, optional) — pass to continue a Task; omit to create one
  - `wait_for_completion` (boolean, default `true`)
  - `timeout_seconds` (number, default 120)
- Behavior: POST JSON-RPC `tasks/send` (or `tasks/sendSubscribe` if streaming added later)
- Output: JSON string with `task_id`, `status`, `messages`

### 4.3 Code Skeleton

```python
# tools/a2a_client_tool.py
"""A2A protocol client tool — call remote A2A-compliant agents."""

import json
import os
import uuid
import logging
import urllib.request
import urllib.error
from typing import Any, Dict, Optional

from tools.registry import registry

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 120
USER_AGENT = "hermes-agent-a2a-client/1.0"


def _http_json(method: str, url: str, body: Optional[dict],
               bearer: Optional[str], timeout: float) -> Dict[str, Any]:
    """Minimal JSON-over-HTTP helper. Returns parsed JSON or raises."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw) if raw else {}


def _bearer() -> Optional[str]:
    return os.getenv("A2A_BEARER_TOKEN") or None


def a2a_discover(args: Dict[str, Any], **_: Any) -> str:
    agent_url = (args.get("agent_url") or "").rstrip("/")
    if not agent_url:
        return json.dumps({"error": "agent_url is required"})
    try:
        card = _http_json(
            "GET",
            f"{agent_url}/.well-known/agent.json",
            None,
            _bearer(),
            timeout=10.0,
        )
        return json.dumps({"agent_card": card})
    except urllib.error.HTTPError as e:
        return json.dumps({"error": f"http_{e.code}", "detail": e.reason})
    except Exception as e:
        return json.dumps({"error": "discover_failed", "detail": str(e)})


def a2a_send_task(args: Dict[str, Any], **_: Any) -> str:
    agent_url = (args.get("agent_url") or "").rstrip("/")
    message = args.get("message") or ""
    if not agent_url or not message:
        return json.dumps({"error": "agent_url and message are required"})
    task_id = args.get("task_id") or str(uuid.uuid4())
    timeout = float(args.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)

    rpc = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "tasks/send",
        "params": {
            "id": task_id,
            "message": {
                "role": "user",
                "parts": [{"type": "text", "text": message}],
            },
        },
    }
    try:
        result = _http_json("POST", agent_url, rpc, _bearer(), timeout=timeout)
    except Exception as e:
        return json.dumps({"error": "send_failed", "detail": str(e)})

    if "error" in result:
        return json.dumps({"error": "rpc_error", "detail": result["error"]})

    task = result.get("result") or {}
    return json.dumps({
        "task_id": task.get("id") or task_id,
        "status": task.get("status"),
        "messages": task.get("history") or [],
    })


_DISCOVER_SCHEMA = {
    "name": "a2a_discover",
    "description": "Fetch the Agent Card from a remote A2A-compliant agent.",
    "parameters": {
        "type": "object",
        "properties": {
            "agent_url": {"type": "string", "description": "Base URL of the remote agent."},
        },
        "required": ["agent_url"],
    },
}

_SEND_SCHEMA = {
    "name": "a2a_send_task",
    "description": (
        "Send a task message to a remote A2A agent and return the response. "
        "Pass task_id to continue an existing task."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "agent_url": {"type": "string"},
            "message": {"type": "string"},
            "task_id": {"type": "string"},
            "wait_for_completion": {"type": "boolean", "default": True},
            "timeout_seconds": {"type": "number", "default": DEFAULT_TIMEOUT_SECONDS},
        },
        "required": ["agent_url", "message"],
    },
}

registry.register(
    name="a2a_discover",
    toolset="a2a",
    schema=_DISCOVER_SCHEMA,
    handler=a2a_discover,
)
registry.register(
    name="a2a_send_task",
    toolset="a2a",
    schema=_SEND_SCHEMA,
    handler=a2a_send_task,
)
```

### 4.4 Toolset Wiring

In `toolsets.py`, add:

```python
"a2a": {
    "description": "Call remote A2A-compliant agents",
    "tools": ["a2a_discover", "a2a_send_task"],
    "includes": [],
},
```

Decide explicitly whether to include `"a2a"` in `_HERMES_CORE_TOOLS`.
Recommended: **do not** include by default; users opt in via `hermes tools`.

### 4.5 Optional Env Var

In `hermes_cli/config.py` `OPTIONAL_ENV_VARS`:

```python
"A2A_BEARER_TOKEN": {
    "description": "Bearer token for outbound A2A calls (if remote agent requires auth)",
    "prompt": "A2A bearer token",
    "url": "",
    "password": True,
    "category": "tool",
},
```

### 4.6 Tests

`tests/tools/test_a2a_client.py`:
- Mock `urllib.request.urlopen` to return a canned Agent Card; assert handler returns valid JSON.
- Mock JSON-RPC response; assert `task_id`, `status`, `messages` shape.
- Assert error path returns `{"error": ...}` and never raises.
- Assert handler signature accepts `**kwargs`.

### 4.7 Definition of Done — Phase 1
- [ ] Tool module created and self-registers via the registry.
- [ ] Toolset entry added to `toolsets.py`.
- [ ] Optional bearer env var documented.
- [ ] Tests pass under `scripts/run_tests.sh tests/tools/test_a2a_client.py`.
- [ ] Manual verification: enable `a2a` toolset and call `a2a_discover` against a public A2A demo.

---

## 5. Phase 2 — Inbound A2A Gateway Platform Adapter

### 5.1 Files

| File | Purpose |
|---|---|
| `gateway/platforms/a2a.py` | New platform adapter (HTTP server + JSON-RPC dispatch) |
| `gateway/config.py` | Add `Platform.A2A` enum value and config schema |
| `gateway/run.py` | Register adapter (only if registration is platform-list driven) |
| `tests/gateway/platforms/test_a2a.py` | Adapter tests |

Read first:
- [gateway/platforms/api_server.py](../gateway/platforms/api_server.py) — closest analogue (HTTP + JSON, SSE streaming)
- [gateway/platforms/webhook.py](../gateway/platforms/webhook.py) — minimal HTTP adapter shape
- [gateway/platforms/ADDING_A_PLATFORM.md](../gateway/platforms/ADDING_A_PLATFORM.md) — adapter contract
- Dual message-guard rule in [AGENTS.md](../AGENTS.md)

### 5.2 Adapter Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/.well-known/agent.json` | Serve Agent Card |
| POST | `/` | JSON-RPC dispatcher |
| GET | `/health` | Health probe |

JSON-RPC methods to handle on `POST /`:
- `tasks/send`
- `tasks/get`
- `tasks/cancel`
- `tasks/sendSubscribe` (Phase 3 — returns 501 in Phase 2)

### 5.3 Adapter Skeleton

```python
# gateway/platforms/a2a.py
"""A2A protocol platform adapter — exposes Hermes as an A2A agent."""

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, Optional

try:
    from aiohttp import web
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    web = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    is_network_accessible,
)

logger = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8650
PLATFORM_NAME = "a2a"


class A2APlatformAdapter(BasePlatformAdapter):
    """Inbound A2A server adapter."""

    PLATFORM = Platform.A2A

    def __init__(self, config: PlatformConfig, runner=None):
        super().__init__(config, runner)
        self._host = config.options.get("host", DEFAULT_HOST)
        self._port = int(config.options.get("port", DEFAULT_PORT))
        self._bearer = config.options.get("bearer_token") or None
        self._agent_name = config.options.get("agent_name", "hermes-agent")
        self._description = config.options.get("description", "Hermes A2A agent")
        self._app: Optional[web.Application] = None
        self._runner_obj: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        # task_id -> session_key mapping; persist via session metadata in production.
        self._task_sessions: Dict[str, str] = {}

    # -- Lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp is required for the a2a platform adapter")
        self._app = web.Application()
        self._app.add_routes([
            web.get("/.well-known/agent.json", self._handle_agent_card),
            web.get("/health", self._handle_health),
            web.post("/", self._handle_jsonrpc),
        ])
        self._runner_obj = web.AppRunner(self._app)
        await self._runner_obj.setup()
        self._site = web.TCPSite(self._runner_obj, self._host, self._port)
        await self._site.start()
        if is_network_accessible(self._host):
            logger.warning("A2A adapter bound to %s:%s — externally reachable", self._host, self._port)

    async def stop(self) -> None:
        if self._site:
            await self._site.stop()
        if self._runner_obj:
            await self._runner_obj.cleanup()

    # -- Outbound (replies) ------------------------------------------------
    # A2A is request/response, not push; deliveries are returned via the
    # JSON-RPC response. This adapter does not initiate outbound messages
    # outside of an in-flight RPC handler.

    async def send_message(self, *args, **kwargs) -> SendResult:
        return SendResult(success=False, error="A2A adapter does not push messages")

    # -- HTTP handlers -----------------------------------------------------

    async def _handle_agent_card(self, request: "web.Request") -> "web.Response":
        card = self._build_agent_card(request)
        return web.json_response(card)

    async def _handle_health(self, request: "web.Request") -> "web.Response":
        return web.json_response({"status": "ok", "platform": PLATFORM_NAME})

    async def _handle_jsonrpc(self, request: "web.Request") -> "web.Response":
        if not self._authorized(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await request.json()
        except Exception:
            return self._rpc_error(None, -32700, "Parse error")

        rpc_id = body.get("id")
        method = body.get("method")
        params = body.get("params") or {}

        if method == "tasks/send":
            return await self._rpc_tasks_send(rpc_id, params)
        if method == "tasks/get":
            return await self._rpc_tasks_get(rpc_id, params)
        if method == "tasks/cancel":
            return await self._rpc_tasks_cancel(rpc_id, params)
        if method == "tasks/sendSubscribe":
            return self._rpc_error(rpc_id, -32601, "Streaming not implemented (Phase 3)")
        return self._rpc_error(rpc_id, -32601, f"Method not found: {method}")

    # -- JSON-RPC method handlers -----------------------------------------

    async def _rpc_tasks_send(self, rpc_id, params):
        task_id = params.get("id") or str(uuid.uuid4())
        message = params.get("message") or {}
        text = self._extract_text(message)
        if not text:
            return self._rpc_error(rpc_id, -32602, "Invalid params: empty text")

        session_key = self._task_sessions.setdefault(task_id, f"a2a:{task_id}")
        # Hand off to the gateway runner like any other inbound message.
        # Pseudocode — actual call shape mirrors how api_server.py creates
        # MessageEvent + dispatches via self.runner.
        response_text = await self._dispatch_to_runner(session_key, text)

        return web.json_response({
            "jsonrpc": "2.0",
            "id": rpc_id,
            "result": {
                "id": task_id,
                "status": {"state": "completed"},
                "history": [
                    message,
                    {"role": "agent", "parts": [{"type": "text", "text": response_text}]},
                ],
            },
        })

    async def _rpc_tasks_get(self, rpc_id, params):
        task_id = params.get("id")
        session_key = self._task_sessions.get(task_id)
        if not session_key:
            return self._rpc_error(rpc_id, -32001, "Task not found")
        # In production, read history from session store via self.runner.
        return web.json_response({
            "jsonrpc": "2.0",
            "id": rpc_id,
            "result": {"id": task_id, "status": {"state": "unknown"}, "history": []},
        })

    async def _rpc_tasks_cancel(self, rpc_id, params):
        task_id = params.get("id")
        session_key = self._task_sessions.get(task_id)
        if not session_key:
            return self._rpc_error(rpc_id, -32001, "Task not found")
        # NOTE: bypass both message guards described in AGENTS.md and call
        # the runner's interrupt path directly (mirror /stop handling).
        if self.runner:
            self.runner.interrupt_session(session_key)
        return web.json_response({
            "jsonrpc": "2.0",
            "id": rpc_id,
            "result": {"id": task_id, "status": {"state": "canceled"}},
        })

    # -- Helpers ----------------------------------------------------------

    def _authorized(self, request) -> bool:
        if not self._bearer:
            return True
        header = request.headers.get("Authorization", "")
        return header == f"Bearer {self._bearer}"

    def _extract_text(self, message: Dict[str, Any]) -> str:
        parts = message.get("parts") or []
        out = []
        for p in parts:
            if isinstance(p, dict) and p.get("type") == "text":
                t = p.get("text")
                if isinstance(t, str):
                    out.append(t)
        return "\n".join(out).strip()

    def _build_agent_card(self, request) -> Dict[str, Any]:
        scheme = request.scheme
        host = request.host
        return {
            "name": self._agent_name,
            "description": self._description,
            "url": f"{scheme}://{host}/",
            "version": "1.0.0",
            "capabilities": {"streaming": False, "pushNotifications": False},
            "authentication": {"schemes": ["bearer"] if self._bearer else []},
            "skills": [
                {
                    "id": "hermes.general",
                    "name": "Hermes general assistant",
                    "description": "General-purpose tool-using agent.",
                }
            ],
        }

    def _rpc_error(self, rpc_id, code, message):
        return web.json_response({
            "jsonrpc": "2.0",
            "id": rpc_id,
            "error": {"code": code, "message": message},
        }, status=200)

    async def _dispatch_to_runner(self, session_key: str, text: str) -> str:
        """Bridge to GatewayRunner. Replace with real implementation
        that builds a MessageEvent and routes via self.runner._handle_message
        (or the equivalent public entry point), awaiting the agent response.
        """
        raise NotImplementedError("Wire to GatewayRunner during implementation")
```

### 5.4 Config Schema

Add to `config.yaml` example:

```yaml
gateway:
  platforms:
    a2a:
      enabled: false
      host: 127.0.0.1
      port: 8650
      bearer_token: ""        # optional
      agent_name: hermes-agent
      description: "Hermes A2A agent"
```

Add `Platform.A2A` enum value in `gateway/config.py`.

### 5.5 Runner Bridging Notes

When implementing `_dispatch_to_runner`:
- Build a `MessageEvent` consistent with [gateway/platforms/base.py](../gateway/platforms/base.py).
- Use the same dispatch mechanism that `api_server.py` uses for OpenAI-compatible inbound calls.
- Block on the agent response (await the future) — Phase 2 is synchronous.
- Catch agent exceptions and convert to JSON-RPC error.
- Respect existing approval flow — if the agent requests approval, return a Task with `status.state = "input-required"`.

### 5.6 Tests

`tests/gateway/platforms/test_a2a.py`:
- Spin up adapter on ephemeral port via `aiohttp.test_utils`.
- Assert `GET /.well-known/agent.json` returns valid Agent Card.
- Assert `POST /` with `tasks/send` and a stub runner returns expected JSON-RPC envelope.
- Assert bearer auth: missing header → 401; correct header → 200.
- Assert `tasks/cancel` invokes the runner interrupt path.

### 5.7 Definition of Done — Phase 2
- [ ] Adapter file created with all four endpoints and three RPC methods.
- [ ] `Platform.A2A` enum + config schema added.
- [ ] Adapter registered with the gateway on startup when `enabled: true`.
- [ ] Tests pass under `scripts/run_tests.sh tests/gateway/platforms/test_a2a.py`.
- [ ] Manual verification: start gateway, hit `/.well-known/agent.json` and a sample `tasks/send` with `curl`.

---

## 6. Phase 3 — Polish

### 6.1 SSE Streaming (`tasks/sendSubscribe`)
- Hold the HTTP response open via aiohttp `StreamResponse`.
- Hook into the agent's existing tool-progress callback (same one that feeds the TUI).
- Emit A2A SSE events:
  - `TaskStatusUpdateEvent` — state transitions (`working`, `input-required`, `completed`).
  - `TaskArtifactUpdateEvent` — partial output chunks.
- Send keepalive comments every 30s (mirror `api_server` pattern).
- On client disconnect: cancel agent via `runner.interrupt_session`.

### 6.2 Push Notifications
- Optional A2A feature: caller registers a webhook; Hermes POSTs the final task state when long-running work finishes.
- Reuse the existing background-process notification machinery for delivery.

### 6.3 Multi-Skill Agent Card
- Surface installed Hermes skills as A2A skills in the Agent Card.
- Map A2A `skill.id` → Hermes skill activation flag, so callers can route directly to a specific capability.

### 6.4 Persistent Task ↔ Session Mapping
- Phase 2 holds the mapping in memory. Persist it in the session store so `tasks/get` survives restarts.

### 6.5 Definition of Done — Phase 3
- [ ] `tasks/sendSubscribe` returns valid SSE stream of A2A events.
- [ ] Push-notification webhook delivery on task completion.
- [ ] Agent Card lists installed skills.
- [ ] Task ↔ session mapping persisted in session metadata.

---

## 7. Cross-Cutting Concerns

### 7.1 Security
- Default bind is `127.0.0.1` — never `0.0.0.0` without explicit user opt-in.
- Bearer token comparison must be constant-time (`hmac.compare_digest`).
- Reuse the gateway allowlist/pairing model for stricter caller identity if needed.
- Validate JSON-RPC params before passing to runner — reject oversized messages early.

### 7.2 Prompt Caching
- One Task = one session. Never rebuild system prompt mid-task.
- Multi-turn within a Task continues the same session's conversation history.
- Refer to "Prompt Caching Must Not Break" in [AGENTS.md](../AGENTS.md).

### 7.3 Profile Safety
- Any persistent state (task mapping, audit log) goes under `get_hermes_home()`.
- Per-profile A2A bearer tokens via `.env` per profile.

### 7.4 Observability
- Log every inbound JSON-RPC method + outcome at INFO level.
- Emit metrics if an `observability` plugin is loaded (count by method, latency histogram).

### 7.5 Failure Modes to Handle
- Remote agent times out → outbound tool returns structured error JSON.
- Inbound caller disconnects mid-stream → cancel running agent.
- Duplicate `tasks/send` with same `task.id` → resume, don't fork.
- Malformed `parts` array → JSON-RPC `-32602` invalid params.

---

## 8. Rollout Plan

1. Land Phase 1 behind opt-in toolset; document in user guide.
2. Land Phase 2 behind `enabled: false` default; document in messaging guide.
3. Run interop testing against at least one third-party A2A agent (e.g. the official sample).
4. Land Phase 3 incrementally; SSE first, then push notifications, then multi-skill.
5. Promote `a2a` toolset to a recommended default after one stable release cycle.

---

## 9. Open Questions

- Should the inbound adapter share the OpenAI-compatible `api_server` HTTP port or run standalone? (Recommendation: standalone; different auth surface.)
- How to map A2A `Task.history` `role: "agent"` → multiple Hermes assistant messages with reasoning content? (Likely flatten to text parts, drop reasoning unless caller requests.)
- Federate via A2A as a second delegation backend (`delegate_task` `backend="a2a"`)? (Defer to follow-up plan.)

---

## 10. References

- A2A protocol: https://a2a-protocol.org/
- In-repo analogues to mirror: [gateway/platforms/api_server.py](../gateway/platforms/api_server.py), [gateway/platforms/webhook.py](../gateway/platforms/webhook.py)
- Adapter contract: [gateway/platforms/ADDING_A_PLATFORM.md](../gateway/platforms/ADDING_A_PLATFORM.md)
- Architecture invariants: [AGENTS.md](../AGENTS.md)
- Repo navigation: [REPO_UNDERSTANDING.md](../REPO_UNDERSTANDING.md)
