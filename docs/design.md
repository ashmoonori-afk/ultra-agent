# ultra-agent design

## Purpose and user experience

ultra-agent is one small, locally operated package that keeps an existing omo
agent useful after a chat window closes. The operator supplies an omo login,
an isolated workspace and optional Discord, Telegram or Slack credentials. The package supplies
durable memory, an approval queue, scheduling, monitors, session continuity,
handoff and status delivery. It does not supply another model platform.

The Python distribution and command are `ultra-agent`; the import package is
`ultra_agent`. Python 3.11 or newer is required. Runtime dependencies are
`birkin-mnemosyne==0.4.0` and pure-Python `websocket-client` for presence sockets.
Configuration, SQLite, process control, HTTP and the CLI use the standard
library. No additional JavaScript runtime is introduced. Development checks
are separate. One configuration and a packaged operator skill cover the entire
command surface. Writing style is a user setting, not a hard-coded persona.

The repository is public. Package registry publishing and release tags are not
part of this delivery. Runtime configuration, memory, database files, status
logs, conversations and private research must never enter its Git history.

## Ideal state and gaps

| User need | Required behavior | Gap this package closes |
| --- | --- | --- |
| Short installation | One Python environment and one package install | Full agent platforms require unrelated infrastructure |
| Memory survives restarts | Real Mnemosyne writes, searches and recalls the same vault | Process-local history is not durable memory |
| Reachable personal agent | Configured Discord channels and senders enqueue durable jobs | A live terminal must not be the message queue |
| Explicit authority | Remote work awaits local approval by default; tools disabled by default | Chat text is not permission to run arbitrary tools |
| Honest recovery | Completed work is not replayed; interrupted work is visibly uncertain | A restart cannot establish whether external effects happened |
| Session continuity | Exact persisted omo session ID; fresh ID after a durable handoff | Resuming the wrong session loses or mixes context |
| Compaction-first operation | Normal compaction does not trigger a reset | High usage alone is not a compaction failure |
| Background work | Persistent schedules, change monitors and a supervised daemon | Closing chat should not delete pending work |
| Observable progress | Local status, durable replies and explicit send uncertainty | Network errors must not turn into false success |
| Reproducible delivery | Public-source hygiene, deterministic tests, CLI/HTTP proof and CI | A passing unit test is not installation or operational proof |
| Tracked lanes | Per-lane sessions, states and explicit sync drift | A job queue alone cannot track a longer task |
| Verified closure | Five-minute objection window, live Done checks and pushed work | A completion claim must not close a lane |
| Host protection | can-launch HOLD and one host-wide heavy slot | Separate workspaces must not overload a host |
| Shared messaging | One command for three transports, including untagged Slack replies | Platform-specific scripts fragment operations |
| Host continuity | Authenticated multi-machine relay and reboot service | A local append file cannot cross machines |

## Upstream decisions and provenance

All application code in ultra-agent is original. References below inform
behavior, not a claim that the upstream applications are bundled.

| Reference | Inspected revision | Decision |
| --- | --- | --- |
| [birkin-mnemosyne](https://github.com/ashmoonori-afk/birkin-mnemosyne) | `f69bfef2bc2489301a14c5fcff51c9018565bd7f`, 0.4.0 | Depend on its MIT-licensed, dependency-free core. Use `VaultMemory`, not its optional MCP or embedding stack. |
| Prior private Jarvis setup | Sanitized source snapshot, reviewed locally | Independently implement its listener, scheduler, monitor, keeper, context-guard and relay concepts. Do not distribute the snapshot or deployment settings. |
| [OpenClaw](https://github.com/openclaw/openclaw) | `d9813e3489766284c105907f30065a83ff798a11` | MIT. Reference explicit session ownership and separated transport/runtime boundaries. Exclude the full gateway/plugin stack. |
| [Agent Messenger](https://github.com/agent-messenger/agent-messenger) | `ad06c95382182dfccb218079a2938b295616287a` | No repository-wide license verified. Reference narrow platform boundaries only; no source copied and no dependency. |
| [Hermes Agent](https://github.com/NousResearch/hermes-agent) | `7951ddd7297a284d6da48804241ee05a8a0c621c` | MIT. Reference serialized periodic work and explicit cancellation. Exclude its full application/provider stack. |
| OpenAI Dots / xAI Grok Bot | Closed hosted products | Concepts only: owned workspace, connectors, approvals and work continuing after chat ends. No source or API compatibility claimed. |
| [open-dots](https://github.com/Anil-matcha/open-dots) | `55c9ddd32e49196b93689651e6e5cdef3f5fdd16` | No license file verified. Reference default-ask approval ideas only. |
| [opendots](https://github.com/milisp/opendots) | `6136450e6151e5a8e5d30d070bbf7bd14aab66ab` | No license file verified. Reference separate workspace/memory and persistent automation ideas only. |
| [CopilotKit/OpenBot](https://github.com/CopilotKit/OpenBot) | `17831b2415b3f764cdb07110ca68fdab18fe8b2d` | MIT. Reference explicit approval and adapter boundaries. Exclude its browser studio and database/server stack. |
| [omo-dori-mode-experimental](https://github.com/ashmoonori-afk/omo-dori-mode-experimental) | `742f09df4cca2816cda275ec2de9b2acf95272e2` | MIT. Adapt skill/CLI/config packaging, lane registry/sync, timed objection/live verification, host HOLD/heavy slots and messenger contracts. Retain its MIT notice for the behavioral adaptation. Exclude terminal-tab coupling, personal onboarding and destructive worktree cleanup. |

License files, not repository descriptions, decide code-reuse eligibility.
Vendor-specific licenses do not license an entire application. See NOTICE for
the required dependency attribution. Optional future upstream adapters need
their own documented interface and license review; none is advertised here.

## Architecture

```text
CLI / daemon
    -> configuration + durable SQLite queue
    -> Discord / Telegram / Slack intake and outbox
    -> lane registry / live completion checks / host guard
    -> schedules / file and command monitors / status-file relay
    -> serialized omo JSON process
    -> Mnemosyne vault / compaction guard / handoff
    -> authenticated multi-machine relay / reboot service
```

The core is synchronous and bounded. One process owns the execution lock, and
only one agent job executes at a time. Discord REST polling is deliberate: it
avoids a gateway/async dependency tree and permits catch-up from durable state.
During a turn, new Discord messages remain on Discord and are fetched on the
next tick. This trades subsecond response latency for a smaller runtime.

The workspace contains `ultra-agent.toml`, a working directory, a memory vault
and a private state directory. SQLite and the vault belong on a local filesystem
with working advisory locks, not a Windows-mounted or network share. Status
files watched for relay may be elsewhere. State directories are private.

Modules have narrow ownership:

- `models.py`, `config.py`: immutable typed data and boundary parsing.
- `store.py`: transactions, immutable job input, durable cursor and outbox state.
- `memory.py`: the only Mnemosyne importer; provenance-enabled writes and recall.
- `driver.py`: exact omo argv, JSON events, deadlines and process-group cleanup.
- `discord.py`: authenticated REST requests, parsing, intake and reply transport.
- `http.py`, `telegram.py`, `slack.py`, `presence.py`, `messenger.py`: shared
  wire boundary, platform adapters, sockets and unified dispatch.
- `automation.py`: schedules, file/command probes and complete-line relay.
- `registry.py`, `completion.py`: lane history, session sync, claim deadlines
  and live Done verification.
- `host.py`: resource probes, explicit HOLD and a host-wide heavy lock.
- `relay.py`, `service.py`: authenticated network relay and reboot setup.
- `runtime.py`: lifecycle, approval, execution, recovery and handoff.
- `cli.py`: argparse commands and machine-readable results.

## Durable authority and recovery

Jobs move through `awaiting_approval -> queued -> running` and then
`completed`, `failed` or `uncertain`. Input and source identity are immutable.
Duplicate source keys return the original job. Approval is a local CLI action
bound to that immutable job. An explicitly configured trusted automation may
preapprove its own occurrences; configuration is the operator's authorization,
not permission inferred from a message.

A launch reservation is committed before a subprocess starts. Recovery never
automatically launches a job found in `running`. It becomes `uncertain`, and
new execution is blocked until the operator reconciles it. A retry is a new,
linked job and requires an explicit acknowledgement of possible prior effects.
A single-instance execution lock prevents concurrent daemon/once/recovery
commands. Process-group termination is confirmed before normal timeout recovery.
For crash/reboot supervision, the documented service uses control-group cleanup.
Arbitrary tool programs that escape supervision are not sandboxed by this package.

Reply delivery has its own state: `pending -> sending -> sent`, with
`uncertain` for an interrupted or ambiguous send. Commit `sending` before one
POST. A definite pre-send rate limit may reschedule; a network failure after a
POST must not blindly retry. Operator-requested resend creates a new delivery
without reexecuting the agent. Large replies use deterministic ordered chunks.
If a chunk is uncertain, later chunks wait for reconciliation.

Exactly-once external effects are not promised. The same local state can mean
"reserved but never started" or "action happened but confirmation was lost."
Exposing this distinction is preferable to silently duplicating work.

Following the Dori completion-evidence pattern, a locally configured optional
`verification_command` runs after a successful agent turn. It is fixed argv,
not a command supplied by the model, and uses the same process cleanup and
credential-isolation boundary. A failed check prevents successful completion.
The persisted `verified` field is true only after this check succeeds.
Without a check, `completed` means the agent process completed, not that its
natural-language claim has been independently proven. Neither branch silently
closes an external task or deletes a worktree.

## Lanes and live completion

Each lane has a validated key, role, host, working directory, exact session ID,
optional authoritative session file, worktrees, Done signals and status history.
Statuses are `working`, `done-claimed`, `verified-done`, `not-done`, and `closed`.
Lane state is separate from job state. `lane sync` reports absent/changed
session headers, missing Done criteria, interrupted runs and unregistered
session files. `--write` only adopts an unambiguous authoritative header and
refuses to change an active lane.

`lane claim KEY --evidence TEXT` records a durable deadline exactly 300 seconds
after the claim. An objection records `not-done` with reasons and cancels that
claim. Repeated ticks do not move the deadline. At or after it, re-read all live
Done signals and Git state. Supported signals are merged pull request, closed
issue, exact published npm version and a configured fixed verification argv.
These are read-only checks; a published-version signal never publishes anything.

Each declared worktree must have readable Git state, no tracked or untracked
work, and no commits absent from its remotes. Refresh remote refs; unavailable
remotes or missing evidence refuse closure. Recheck before closing. Only then
record `verified-done` and `closed` with receipts. Failure records `not-done`
and keeps the lane. Closing never deletes a worktree, kills an unrelated
process or merges/pushes anything.

## Host guard and heavy slot

`host can-launch` reports `READY` or `HOLD` with reasons. Defaults are 20 percent
available memory, 1 GiB available disk and 20 active lanes; a zero lane cap
disables that cap. CPU load is reported, not a launch veto. Missing measurements
mean HOLD. Linux memory uses MemAvailable; disk uses the configured workspace.

Heavy launches acquire a host-wide advisory lock in the operator's local state
directory, shared by separate workspaces. Default capacity is one heavy
process. A held slot leaves work queued and reports HOLD. The slot surrounds
the actual process lifetime, including timeout cleanup. Jobs run locally;
other hosts run the same package independently and relay status. The package
does not silently SSH to a host or treat a stale heartbeat as launch authority.

## Unified messenger and capabilities

`messenger send PLATFORM --to TARGET --text TEXT [--thread ID] [--edit ID]`
is one entry point. `messenger inbound PLATFORM --once`, `messenger typing`
and `messenger presence` share configuration and credential isolation.
Every adapter uses durable source keys and the same approval/outbox rules.

| Capability | Discord | Telegram | Slack |
| --- | --- | --- | --- |
| Send/edit | REST messages | sendMessage/editMessageText | chat.postMessage/chat.update |
| Inbound | history cursor | getUpdates offset | tracked/configured thread replies and channel history |
| Typing | channel typing endpoint | sendChatAction | Explicit unsupported-capability error |
| Presence | gateway heartbeat | Explicit unsupported-capability error | user-session client socket; away on release |

Dori has no Slack typing implementation and no Telegram presence. Unsupported
operations fail visibly, never as successful no-ops. Slack reply acceptance
depends on configured channels, allowed authors and tracked/configured threads,
not a bot mention. Polling paginates and persists reply timestamps. User-session
mode may discover subscribed threads through the client API; failures of this
unofficial API are explicit, not replaced by fabricated data.

Slack REST reads a named token environment variable. User-session presence and
subscribed-thread discovery additionally read a named cookie environment
variable. Credentials are operator supplied, never extracted from a browser.
The client WebSocket API is unofficial and can change. Discord presence follows
Hello/Identify/Ready/Heartbeat. Socket shutdown and reconnect are bounded.
Status distinguishes a connected socket from verified visible availability.
Tokens, cookies and Telegram token-bearing URLs are redacted from diagnostics.

## Discord contract

Only configured channels and users are accepted. Bot, webhook, malformed and
unauthorized messages never enter an agent prompt. Tokens come only from a
named environment variable and never appear in configuration examples, status
messages or errors. The token is removed from the environment passed to omo
and monitor commands. omo tools are disabled unless an operator lists them.

Use the official API v10 base with redirects rejected. Test injection can use
a loopback HTTP server, but arbitrary credential destinations are not a public
configuration feature. Validate bot identity and configured channel access in
the live doctor check. Message Content intent, view and history permissions are
documented prerequisites.

First startup establishes a current-message baseline rather than executing old
chat history. Subsequent starts catch up from the durable cursor. Messages are
returned newest-first by Discord; walk backward to the saved boundary, then
persist the accepted inputs in chronological order and advance the cursor in
the same transaction. A bounded backlog limit fails visibly without advancing
the cursor. No silently truncated backlog is accepted.

Outgoing mentions are disabled. Replies reference the triggering message.
429 handling obeys a validated finite retry delay; unauthorized access stops
the connector visibly rather than creating a retry storm.

## omo session and context contract

The supported executable interface is:

```text
omo -p --mode json --session-id UUID --session-dir DIR --no-tools
```

An optional configured model and explicitly selected tools replace those
defaults. The prompt is delivered on standard input, not interpolated into a
shell command. No `shell=True` or command construction from chat text exists.
The authoritative reply is a completed assistant `message_end` with a successful
terminal turn and process exit. Tool output and thinking are not posted.
Malformed event streams, errors, missing terminal completion and timeouts do not
produce a successful job.

Recall a bounded amount of real Mnemosyne memory into each prompt, treating it
as quoted data rather than authority. Store completed turn and handoff records
with `evidence_required=True` and explicit source keys.

Normal `compaction_end` success, high usage, unknown usage and an
`external-owner` rejection do not reset sessions. A terminal compaction error
or explicit operator rotation writes a durable handoff first, then records a
fresh session ID. The handoff contains role, workspace/config references,
current jobs, uncertain outcomes, standing constraints and next actions.
If writing it fails, no readiness marker or new ID is committed.

An uncertain job is not automatically replayed in the new session. The next
authorized turn receives the handoff and recall context. This preserves
continuity without pretending that a context reset proves unfinished actions
are safe to repeat.

## Background automation and relay

Schedules support positive intervals and daily local times with an IANA zone.
Each nominal occurrence has a unique durable source key. Overlapping or missed
intervals coalesce to the latest due occurrence; daily work runs at most once
per local date. Clock rollback does not rerun a recorded occurrence. Trust is
explicit per definition; default occurrences await approval.

File monitors compare durable content fingerprints; command monitors execute
only operator-configured argv with a deadline and no shell. The first observation
establishes a baseline unless explicitly configured otherwise. Changed state
enqueues an event transactionally with its new fingerprint.

Status-file relay tracks file identity and byte offsets. Only complete
`PROGRESS` and `QUESTION` lines are enqueued, so partial writes wait. Duplicate
text at different offsets remains two events. Truncation/rotation resets the
appropriate cursor. Delivery uses the same durable outbox and uncertainty rules.

## Multi-machine relay and reboot recovery

An opt-in HTTP receiver replaces deployment-specific SSH scripts. `relay serve`
accepts bounded `/v1/status` JSON with a configured source, sequence and status
text. A source-specific bearer secret comes from its environment variable;
destination routing is local config, never a remote request parameter.
Authenticate before accepting state; insert source/sequence and outbox together.
Duplicate sends return the existing receipt; stale/out-of-order input fails.
`relay push` sends complete new file lines from a durable cursor; failed pushes
leave the cursor unchanged. Non-loopback clients require HTTPS.

`service render/install` provides a systemd user service with restart-on-failure,
control-group cleanup and explicit workspace/config. Installation and enabling
are operator commands, never import side effects. User-manager boot restarts the
daemon; unattended boot needs the host's user-manager/linger setup documented in
the README. Keeper mode supervises a child without systemd but does not itself
register a boot hook. Recovery after reboot uses the same uncertain-job and
handoff rules.

## Verification and release gate

Tests use temporary workspaces, a real SQLite database, actual Mnemosyne and an
HTTP-level messenger fixtures, not mocks of the integration being asserted.
Subprocess fixtures exercise the exact omo event contract and process cleanup.
Clocks are injected; synchronization uses events or process completion, not
fixed sleeps. Live omo is exercised separately with tools disabled.

Acceptance covers installation, persisted memory, allowlists, approval,
duplicate intake, crash cut points, ambiguous delivery, more than one history
page, Unicode reply chunks, rate limits, timeout cleanup, compaction handling,
schedule recovery, monitor changes, relay partial writes and a second runner.
Additional acceptance covers lane sync, clock-controlled 299/300-second
objection boundaries, false Done claims, dirty/unpushed work, cross-workspace
heavy slots, untagged Slack replies, typing/presence wire behavior, authenticated
two-node relay and service restart configuration. Without live credentials, the
report labels real service verification unavailable; a loopback fixture is not
a live Discord, Slack or Telegram result.

Before every push, scan current/indexed files and every reachable Git object,
including commit metadata, for secrets and private identifiers. Use a public
no-reply author identity. Private scanner rules and source snapshots remain
outside this repository. Commit only verified units. CI installs the package,
runs checks and builds a wheel; it never publishes a package or creates a tag.
