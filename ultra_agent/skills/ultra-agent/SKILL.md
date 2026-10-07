---
name: ultra-agent
description: Operate and supervise an ultra-agent workspace from a terminal - one-command setup, init, memory, approval queue, once/status, lanes, live Done checks, host guard, messengers, schedules, relay and service, on a Codex CLI, Claude Code or omo backend. Use when running the ultra-agent CLI or interpreting its durable state.
---

# ultra-agent operator skill

ultra-agent keeps an existing Codex CLI, Claude Code or omo login useful after a
chat window closes (`runner` in the workspace TOML). It adds durable memory, an
approval queue, session continuity, lanes, a host guard, three messengers,
automation and status relay around that CLI. It is not a model platform and
does not replace the CLI or its login.

First-time setup is one command (`install.sh | bash`, or `ultra-agent
--workspace <PATH> setup` when already installed). It never reads provider
credential files; a missing login is reported with the exact login command.
`ultra-agent --workspace <PATH> stop` stops a background agent started by setup.

## When to use

Use this skill when the user asks to run, supervise, debug or explain the
`ultra-agent` CLI: installing it, initializing a workspace, reading or writing
memory, submitting/approving work, running a tick, inspecting lanes, checking
host state, sending or receiving messages, or diagnosing a returned status.

## Use protocol

1. Always pass the workspace explicitly and first:
   `ultra-agent --workspace <PATH> <command>`.
2. Read before you act. Prefer read-only commands (`status`, `doctor`,
   `lane list`, `lane sync`, `host can-launch`, `host status`, `memory search`,
   `memory get`) to understand state before any command with side effects.
3. Treat command output as machine data: data results are JSON. Failures exit
   non-zero. Do not paraphrase a fail as a success.
4. Approval is explicit. A submission without `--approve` waits in
   `awaiting_approval`; run `approve ID` only when the operator has authorized
   that specific job. Tools are disabled unless `tools` is configured.
5. Reconcile openly. `resolve`/`retry` require `--acknowledge-uncertain-effects`;
   `resend` requires `--acknowledge-possible-duplicate`. Never pass those flags
   on the operator's behalf unless the operator asked for that reconciliation.
6. Prefer the operator's configured credentials. Read tokens only from the
   environment variables the configuration names. Never extract, print, log or
   copy a token or cookie from a browser, a file or another process.

## Completion and evidence rules

- A completion claim requires live evidence. Do not report a job, lane or
  delivery as done based on prose. Require the durable state plus a live check:
  a merged pull request, a closed issue, an exact published npm version, or the
  operator-configured `verification_command` exiting zero.
- At or after the 300-second lane objection deadline, re-read every live Done
  signal and the Git state of each declared worktree before closing. A worktree
  with tracked or untracked changes, or commits absent from its remotes, refuses
  closure; an unavailable remote or missing evidence refuses closure too.
- A loopback HTTP/WebSocket fixture, a subprocess fixture or a unit test is not
  live-service proof. Without real provider credentials, say that live Discord,
  Telegram or Slack verification is unavailable rather than implying it passed.
- Unsupported operations fail visibly: Slack has no typing implementation and
  Telegram has no presence implementation. Report the explicit capability error;
  never describe an unsupported operation as a successful no-op.

## Boundaries

- Never delete, prune or `git clean` a worktree automatically. Closing a lane
  never deletes a worktree, kills an unrelated process, merges or pushes.
- Never publish a package, create a release tag, or publish an npm version as
  part of a Done check. Published-version signals are read-only.
- Never invent a writing style or persona. Prompt style comes only from the
  operator's `writing_style` setting; when it is empty, add nothing.
- Never embed, transmit or reconstruct credentials from configuration. Secrets
  live only in the named environment variables.
- Keep the workspace, memory vault and SQLite state on a local filesystem; do
  not move them to a network share or a Windows-mounted path.
