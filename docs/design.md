# ultra-agent design

## Purpose and user experience

ultra-agent is one small, locally operated package that keeps an existing omo
agent useful after a chat window closes. The operator supplies an omo login,
an isolated workspace and, optionally, a Discord bot. The package supplies
durable memory, an approval queue, scheduling, monitors, session continuity,
handoff and status delivery. It does not supply another model platform.

The Python distribution and command are `ultra-agent`; the import package is
`ultra_agent`. Python 3.11 or newer is required. The only mandatory runtime
dependency is `birkin-mnemosyne==0.4.0`. Configuration, SQLite, process control,
HTTP and the CLI use the standard library. Development checks are separate.

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

License files, not repository descriptions, decide code-reuse eligibility.
Vendor-specific licenses do not license an entire application. See NOTICE for
the required dependency attribution. Optional future upstream adapters need
their own documented interface and license review; none is advertised here.

## Architecture

```text
CLI / daemon
    -> configuration + durable SQLite queue
    -> Discord REST intake / outbox
    -> schedules / file and command monitors / status-file relay
    -> serialized omo JSON process
    -> Mnemosyne vault / compaction guard / handoff
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
- `automation.py`: schedules, file/command probes and complete-line relay.
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

## Verification and release gate

Tests use temporary workspaces, a real SQLite database, actual Mnemosyne and an
HTTP-level Discord fixture, not mocks of the integration being asserted.
Subprocess fixtures exercise the exact omo event contract and process cleanup.
Clocks are injected; synchronization uses events or process completion, not
fixed sleeps. Live omo is exercised separately with tools disabled.

Acceptance covers installation, persisted memory, allowlists, approval,
duplicate intake, crash cut points, ambiguous delivery, more than one history
page, Unicode reply chunks, rate limits, timeout cleanup, compaction handling,
schedule recovery, monitor changes, relay partial writes and a second runner.
Without live bot credentials, the report must label real Discord service
verification unavailable; a loopback fixture is not a live Discord result.

Before every push, scan current/indexed files and every reachable Git object,
including commit metadata, for secrets and private identifiers. Use a public
no-reply author identity. Private scanner rules and source snapshots remain
outside this repository. Commit only verified units. CI installs the package,
runs checks and builds a wheel; it never publishes a package or creates a tag.
