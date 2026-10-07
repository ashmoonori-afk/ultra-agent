# One-command setup and interchangeable agent CLIs

This extends `design.md`: the product is no longer limited to omo users.
Codex CLI and Claude Code are first-class execution backends. All backends
share the existing memory, approvals, lanes, completion checks, host guard,
messengers, automation, relay and recovery machinery.

## User outcome

On Linux/WSL, one command downloads and runs the installer:

```sh
curl -fsSL https://raw.githubusercontent.com/ashmoonori-afk/ultra-agent/main/install.sh | bash
```

The installer detects a native `codex`, `claude` or `omo`, installs missing
Python/package prerequisites into user-owned locations, collects messenger
platform, bot token, channel and allowed sender ID, and starts the runtime.
The sender ID is required: an empty allowlist would produce a nonfunctional bot,
while accepting every sender would discard the approval boundary.

Prompts use the controlling terminal, not the script's piped stdin. Tokens
are hidden while entered and written only to a private `state/service.env`.
They never enter process arguments, TOML, stdout, Git or model prompts.
Noninteractive setup uses named environment variables and explicit options.
Native provider login remains with the provider CLI; the installer never reads
or copies authentication files. If login is required, its native login flow
runs interactively. An unattended invocation fails clearly instead of hanging.

Existing configuration and a running workspace are preserved on a repeated
invocation. Setup does not overwrite unrelated executables or shell profiles.
A Windows npm shim discovered through WSL is not treated as a native Linux
runner. It cannot participate in the runtime's POSIX process ownership.

## Bootstrap

`install.sh` is a small bootstrap, not a second runtime. It uses an available
Python 3.11+ with venv support; otherwise it installs uv from its official
installer and uses a managed Python. The package comes from this repository,
not from a package registry named `ultra-agent`. Installation is serialized.
No sudo, system package changes, package publication or release tag is needed.

The installed `setup` command owns detection, configuration and startup.
Existing native CLIs are reused. If no supported CLI is present, setup installs
Codex through its official standalone installer into an owned local location;
explicit Claude selection can use its official native installer. Selecting omo
requires an existing omo installation.

User systemd is preferred where available. `Type=notify` readiness is sent
after a successful first runtime tick, so starting a unit is not mistaken for
application readiness. A process/keeper fallback uses an inherited readiness
pipe and a bounded handshake. The fallback is explicitly not a boot hook.
The service uses the existing restart/uncertainty rules. The installer does
not reboot the host or change system-wide services.

## Runner boundary

Configuration gains `runner = "omo" | "codex" | "claude"`; legacy files default
to omo. `command` remains fixed operator argv. The bounded process primitive
is shared, including stdin prompts, output/deadline limits, token isolation,
owned process groups and inherited workspace/heavy-slot descriptors.

Each backend returns the existing `TurnResult`. Native session bookkeeping
is stored under the workspace's sessions directory in a versioned descriptor:
the stable ultra-agent lane UUID, runner kind and native session ID. This is
explicitly a mapping, not a fabricated native transcript. An absent mapping
starts a session; a valid mapping resumes that exact session. Switching runners
requires explicit rotation and its durable handoff. Malformed or mismatched
mappings fail closed.

- **omo:** preserve the proven `--session-id`/`--session-dir` JSON protocol and
  existing native session files. No tools by default.
- **Codex:** `exec --json` with stdin, explicit sandbox and noninteractive
  approval policy; `exec resume` uses the recorded `thread_id`. Success requires
  exit zero, an authoritative agent message and `turn.completed`; failure/error
  events do not become success. Fresh non-Git workspaces use the documented
  skip-Git-check option. Codex chooses its native thread ID.
- **Claude:** headless JSON output, forced UUID initially and explicit
  `--resume` subsequently. Success requires a valid result/session envelope
  and exit zero; error envelopes fail. Empty built-in tools, strict empty MCP
  configuration and unattended denial preserve the default restriction.

Codex's read-only sandbox is not an exact equivalent of disabling every tool.
Its default additionally disables shell/web and configured MCP tools. Native
MCP names are obtained through the CLI, never by extracting credential files;
per-server disabled overrides are checked. An empty `mcp_servers={}` alone is
NOT sufficient: current Codex merges tables recursively. Arbitrary per-tool
allowlists are not promised; unsupported permission requests fail explicitly.
An explicit Codex sandbox setting can enable workspace writes without using
dangerous bypass flags. The distinction is documented rather than hidden.

A messenger's optional API endpoint override is restricted to its canonical
host or loopback. This allows clean-HOME installer tests to exercise the real
HTTP adapters without contacting a real account.

## Required proof

1. Existing runtime regressions and live omo continuity remain green.
2. Real subprocess fixtures exercise Codex and Claude initial/resume, terminal
   errors, malformed output, cancellation and context-failure handoff.
3. One installer invocation in a clean temporary HOME with fake Codex, and a
   separate one with fake Claude, must create an actual private installation,
   start a real runtime, execute the selected fixture CLI and produce a
   loopback messenger reply. Detecting a command without executing it is not
   evidence of a working backend.
4. Invalid credentials, missing prerequisites, noninteractive input, repeated
   setup, startup failure and interruption have explicit assertions. Tokens
   must not appear in argv/logs/TOML. Readiness and cleanup use events, not sleeps.
5. Final review covers the combined product; all public files and reachable
   history are scanned before each push, and CI must pass for the delivered SHA.

## Primary contracts consulted

- https://developers.openai.com/codex/non-interactive
- https://developers.openai.com/codex/cli/reference/
- https://developers.openai.com/codex/auth
- https://github.com/openai/codex/tree/main/codex-rs/config/src
- https://code.claude.com/docs/en/cli-reference
- https://code.claude.com/docs/en/headless
- https://code.claude.com/docs/en/setup
- https://docs.astral.sh/uv/getting-started/installation/

Installed Claude Code 2.1.292 was probed directly. Its help confirms headless
JSON, session/resume, tool, strict MCP and unattended permission flags.
No provider source is vendored by these adapters.
