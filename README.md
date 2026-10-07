# ultra-agent

A small, locally operated personal-agent runtime that keeps an existing
`omo` login useful after a chat window
closes. It wraps the `omo` CLI with durable memory, a local approval queue,
session continuity and handoff, tracked lanes with verified closure, a host
guard, Discord/Telegram/Slack delivery, schedules, monitors and a multi-machine
status relay. It is not a model platform and does not replace `omo`.

Repository: <https://github.com/ashmoonori-afk/ultra-agent>

This project is not published to any package registry. There is **no PyPI
release and no release tag**. Do not run `pip install ultra-agent`: it will not
install this project. Always install from the GitHub URL below.

## What it does

- **Durable memory.** Writes, searches and recalls a real Mnemosyne vault so
  context survives restarts.
- **Approval queue.** Inbound work is stored in SQLite and waits for a local
  approval by default; tools are disabled by default.
- **Honest recovery.** Interrupted jobs and interrupted sends become visibly
  `uncertain` instead of being silently replayed.
- **Session continuity.** Uses the exact persisted `omo` session ID, and writes
  a durable handoff before ever starting a fresh session.
- **Lanes.** Per-lane sessions, status history, drift sync and a 300-second
  objection window with live Done verification.
- **Host guard.** A `READY`/`HOLD` resource check and one host-wide heavy slot.
- **Messengers.** One command surface for Discord, Telegram and Slack.
- **Automation and relay.** Schedules, file/command monitors, a status-file
  relay and an authenticated multi-machine receiver.

## Requirements

- **Python 3.11 or newer.**
- **An existing, already-authenticated `omo` CLI on `PATH`.** ultra-agent neither
  installs nor configures `omo`; it drives the login you already have.
- **Two runtime dependencies**, installed automatically: `birkin-mnemosyne`
  (the memory vault) and `websocket-client` (pure Python, for the presence
  sockets). Everything else uses the standard library.
- A **local** filesystem for the workspace (SQLite and the vault need working
  advisory locks). Do not put the workspace on a network share or a
  Windows-mounted path such as `/mnt/c`.

## Install

From GitHub into a virtual environment:

```bash
python3 -m venv "$HOME/.venvs/ultra-agent"
. "$HOME/.venvs/ultra-agent/bin/activate"
python -m pip install --upgrade pip
python -m pip install "git+https://github.com/ashmoonori-afk/ultra-agent.git"
ultra-agent --help
```

Editable install with the development extras:

```bash
git clone https://github.com/ashmoonori-afk/ultra-agent.git
cd ultra-agent
python -m pip install -e ".[dev]"
```

The `[dev]` extra adds `pytest`, `ruff`, `basedpyright`, `build` and `websockets`
(test-only). None of these are needed at runtime.

## Quick start

`--workspace` is a global option and comes before the command. Use any isolated
directory you own:

```bash
export UA_WORKSPACE="$HOME/ultra-workspace"

ultra-agent --workspace "$UA_WORKSPACE" init

ultra-agent --workspace "$UA_WORKSPACE" memory write "greeting" "hello from ultra-agent" --source "cli"
ultra-agent --workspace "$UA_WORKSPACE" memory search "hello"
ultra-agent --workspace "$UA_WORKSPACE" memory get "greeting"

ultra-agent --workspace "$UA_WORKSPACE" submit "Reply with the current UTC time." --approve
ultra-agent --workspace "$UA_WORKSPACE" once
ultra-agent --workspace "$UA_WORKSPACE" status
```

- `init` creates the private `work`, `vault`, `state`, `sessions` and `handoffs`
  directories and a default `ultra-agent.toml`. An existing configuration file
  is never overwritten.
- `submit TEXT` enqueues work. Without `--approve` it stays `awaiting_approval`;
  approve it later with `approve ID`. `--lane KEY` routes work through a lane's
  session.
- `once` runs at most one approved job and exits; `run` runs the daemon loop.
- `status` prints JSON. All data results are JSON and failures exit non-zero.

A **real agent turn requires a working `omo` login.** Without it, submissions
still queue, but `once` fails. **Messaging requires real provider credentials**;
see [Configuration](#configuration).

## Configuration

Everything is configured in exactly one file: `<workspace>/ultra-agent.toml`.
Parsing is strict — unknown keys, embedded secrets and malformed values are
rejected. See [`examples/ultra-agent.example.toml`](examples/ultra-agent.example.toml)
for a complete, commented sample.

- **Credentials are separate from configuration.** The TOML names environment
  variables (`token_env`, `cookie_env`); the secret values live only in your
  environment. Tokens are removed from the environment passed to the agent, and
  are never written to config, status output or errors.
- **`writing_style` is configurable and empty by default.** When empty it adds
  nothing to prompts; there is no built-in voice or persona.
- **Manual approval and no tools by default.** `tools = []` means `--no-tools`.
  Inbound messages and messages from schedules/monitors await local approval
  unless a definition is explicitly marked `trusted = true`.
- **`verification_command`** is a fixed argv run after a successful turn; only a
  zero exit marks a job `verified`. A model saying "done" is never proof.
- **Uncertain jobs and deliveries are reconciled explicitly.** An interrupted
  execution becomes `uncertain` and blocks new runs until you run
  `resolve ID --acknowledge-uncertain-effects` or
  `retry ID --acknowledge-uncertain-effects`. An ambiguous reply send is
  reconciled with `resend ID --acknowledge-possible-duplicate`.

## Commands

```text
ultra-agent --workspace PATH init
ultra-agent --workspace PATH doctor [--live]
ultra-agent --workspace PATH submit TEXT [--approve] [--lane KEY]
ultra-agent --workspace PATH approve ID
ultra-agent --workspace PATH status
ultra-agent --workspace PATH once
ultra-agent --workspace PATH run
ultra-agent --workspace PATH resolve ID --acknowledge-uncertain-effects
ultra-agent --workspace PATH retry ID --acknowledge-uncertain-effects
ultra-agent --workspace PATH resend ID --acknowledge-possible-duplicate
ultra-agent --workspace PATH rotate --reason TEXT
ultra-agent --workspace PATH memory write TITLE BODY --source SOURCE
ultra-agent --workspace PATH memory search QUERY
ultra-agent --workspace PATH memory get TITLE
```

`doctor --live` checks live provider identity; it needs real credentials.
It reports channel access as unverified and never sends a test message.

## Lanes

Lanes track longer work with their own persisted session, working directory,
host, worktrees, Done signals and status history. Lane state is separate from
job state.

```text
ultra-agent --workspace PATH lane launch <key>
ultra-agent --workspace PATH lane adopt  <key>
ultra-agent --workspace PATH lane list
ultra-agent --workspace PATH lane sync [--write]
ultra-agent --workspace PATH lane claim  KEY --evidence TEXT
ultra-agent --workspace PATH lane object KEY --reason TEXT
ultra-agent --workspace PATH lane watch  <key>
ultra-agent --workspace PATH lane close  KEY
```

Run `... lane <subcommand> --help` for the exact flags of each subcommand.
For example, `lane launch feature --role worker --done command` uses the fixed
`verification_command` in your configuration. Other Done signals are
`pr:OWNER/REPO#NUMBER`, `issue:OWNER/REPO#NUMBER`, and `npm:PACKAGE@EXACT_VERSION`.
Separate multiple required signals with semicolons; quote that argument.

- `lane sync` reports drift: absent or changed session headers, missing Done
  criteria, interrupted runs and unregistered session files. `--write` only
  adopts an unambiguous authoritative header and refuses to change an active
  lane.
- `lane claim KEY --evidence TEXT` records a durable deadline exactly **300
  seconds** later. An objection records `not-done`, cancels the claim and does
  not move the deadline. Repeated ticks do not extend it.
- At or after the deadline, ultra-agent re-reads all live Done signals and the
  Git state of every declared worktree. Supported signals are a merged pull
  request, a closed issue, an exact published npm version and the configured
  fixed `verification_command`. All are read-only.
- **Unpushed or uncommitted work refuses closure.** A worktree with tracked or
  untracked changes, or commits absent from its remotes, refuses closure, as do
  an unavailable remote or missing evidence. Only then is the lane recorded
  `verified-done` and `closed`. Closing never deletes a worktree, kills an
  unrelated process, merges or publishes anything.
- Each lane keeps its own session and handoff, so continuity is per lane.

## Host guard and heavy slot

```text
ultra-agent --workspace PATH host can-launch   # READY or HOLD, with reasons
ultra-agent --workspace PATH host status
```

Defaults are 20 percent available memory, 1 GiB available disk and 20 active
lanes (0 disables the lane cap). CPU load is reported, not a launch veto.
Missing measurements mean `HOLD`. Heavy launches take one host-wide advisory
lock in your local state directory, shared across separate workspaces; a held
slot leaves work queued and reports `HOLD`. Jobs run locally — ultra-agent does
not SSH to other hosts.

## Messengers

```text
ultra-agent --workspace PATH messenger send PLATFORM --to TARGET --text TEXT [--thread ID] [--edit ID]
ultra-agent --workspace PATH messenger inbound PLATFORM --once
ultra-agent --workspace PATH messenger typing PLATFORM ...
ultra-agent --workspace PATH messenger presence PLATFORM ...
```

`PLATFORM` is `discord`, `telegram` or `slack`. All adapters share durable
source keys and the same approval/outbox rules. Only configured channels and
users are accepted; bot, webhook, malformed and unauthorized messages never
enter a prompt.

| Capability | Discord | Telegram | Slack |
| --- | --- | --- | --- |
| Send / edit | messages | sendMessage / editMessageText | chat.postMessage / chat.update |
| Inbound | history cursor | getUpdates offset | tracked/configured thread replies and channel history |
| Typing | channel typing endpoint | sendChatAction | **unsupported** (explicit error) |
| Presence | gateway heartbeat | **unsupported** (explicit error) | user-session client socket; away on release |

Two honest limitations: **Slack has no typing implementation** and **Telegram
has no presence implementation**. Requesting either fails with an explicit
capability error — never a silent no-op.

`messenger presence PLATFORM` confirms connection and remains online until
interrupted. Add `--seconds N` for a bounded session; failure to connect is an
error, and exit always releases the socket.

Slack replies are accepted from configured channels, allowed authors and
tracked/configured threads **without requiring a bot mention**. Slack
user-session presence and subscribed-thread discovery use an unofficial client
WebSocket API and an operator-supplied cookie environment variable; that API
can change, and failures are reported rather than replaced with fabricated data.

## Schedules, monitors and relay

```text
ultra-agent --workspace PATH relay serve
ultra-agent --workspace PATH relay push PEER --file STATUS_FILE
```

- Schedules support positive intervals or daily local times with an IANA zone.
  Each nominal occurrence has a unique durable source key; missed intervals
  coalesce and clock rollback does not rerun a recorded occurrence.
- File monitors compare durable content fingerprints; command monitors run only
  operator-configured argv with a deadline and no shell.
- The status-file relay tracks file identity and byte offsets and relays only
  complete `PROGRESS`/`QUESTION` lines, so partial writes wait.
- `relay serve` accepts bounded JSON status from configured sources, each with
  its own bearer secret; non-loopback clients must use HTTPS. Destination
  routing is local configuration, never a remote request parameter.

## Service and keeper (reboot recovery)

```text
ultra-agent --workspace PATH service render
ultra-agent --workspace PATH service install --enable
ultra-agent --workspace PATH keeper -- ultra-agent --workspace PATH run
```

`service render` prints plain systemd unit text rather than JSON, with
restart-on-failure and control-group cleanup. `service install` writes the unit;
only `--enable` also enables and starts it
(there is no installation side effect on import). Unattended restart at boot
needs the host's user-manager/linger setup, which is host-specific and out of
scope here. `keeper` supervises a child process without systemd but does not
register a boot hook itself. Recovery after a reboot uses the same
uncertain-job and handoff rules described above.

## Testing and CI

```bash
python -m pytest -q
python -m ruff check ultra_agent tests
python -m basedpyright --pythonpath "$(python -c 'import sys; print(sys.executable)')"
python -m build --wheel
```

CI (`.github/workflows/ci.yml`) runs on pushes and pull requests to `main` on
Ubuntu with Python 3.11 and 3.14, sequentially (`max-parallel: 1`): install
`-e ".[dev]"`, `pytest`, Ruff, basedpyright and a wheel build. It has **no
publish or release steps** and creates no tags.

**Test fixtures are not live-service proof.** The unit and integration tests use
a loopback HTTP server, a loopback WebSocket fixture, a fake `omo` subprocess
and a temporary SQLite database. They prove the local contracts, not a live
Discord, Telegram or Slack deployment. Real provider verification requires real
credentials and an allowed channel, and is reported as **unavailable** when
those are absent. Do not read a green CI run as live-service evidence.

## Security notes

- Never commit your runtime workspace, database, vault, logs or conversation
  files; the provided `.gitignore` excludes them.
- Credentials live only in the environment variables named by your
  configuration. Keep them out of the TOML, shell history and logs.
- The workspace, vault and SQLite state belong on a local filesystem.

## License

MIT. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE) for third-party
attribution.
