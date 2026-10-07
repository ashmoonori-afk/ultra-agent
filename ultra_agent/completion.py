"""Live completion verification for lanes: objection windows and live Done checks.

A lane's Done criteria are semicolon-separated signals. Parsing is strict and
read-only: only a literal ``command`` (the operator-configured fixed
``verification_command``), ``pr:OWNER/REPO#NUMBER``, ``issue:OWNER/REPO#NUMBER``
and ``npm:PACKAGE@EXACT_VERSION`` are accepted. No argv is ever built from the
Done text, so a model-written claim cannot smuggle a command.

A claim opens a fixed 300-second window owned by :class:`~ultra_agent.registry.Registry`;
this module never bypasses it. When the window has elapsed, ``tick`` and
``close`` re-read every live signal and every declared Git worktree at the
deadline and again immediately before closure, refuse while the lane has
running or uncertain execution, and only then record ``verified-done`` and
``closed`` through compare-and-swap transitions. A racing objection changes the
lane revision, so a stale check cannot close it. Verification never deletes a
worktree, merges, pushes or publishes anything.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypeAlias
from urllib.parse import quote

from ultra_agent.driver import child_environment, run_command
from ultra_agent.http import HttpClient, TransportError
from ultra_agent.models import CheckResult, Config
from ultra_agent.registry import Lane, Registry, RegistryError
from ultra_agent.store import Store

NPM_REGISTRY_URL = "https://registry.npmjs.org"
"""Default npm registry base used for exact published-version checks."""

_ACTIVE_JOB_STATUSES: tuple[str, ...] = (
    "awaiting_approval", "queued", "running", "uncertain",
)

_GH_REF_RE = re.compile(
    r"^(?P<owner>[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)/"
    r"(?P<repo>[A-Za-z0-9._-]+)#(?P<number>[0-9]+)$"
)
_NPM_RE = re.compile(
    r"^(?P<package>(?:@[A-Za-z0-9._-]+/)?[A-Za-z0-9._-]+)@"
    r"(?P<version>[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?(?:\+[A-Za-z0-9.-]+)?)$"
)


class SignalError(ValueError):
    """A lane Done criteria string declared an unknown, malformed or empty check."""


@dataclass(frozen=True, slots=True)
class CommandSignal:
    """Run the operator-configured fixed ``verification_command``."""


@dataclass(frozen=True, slots=True)
class PullRequestSignal:
    """A pull request that must be merged."""

    owner: str
    repo: str
    number: int


@dataclass(frozen=True, slots=True)
class IssueSignal:
    """An issue that must be closed."""

    owner: str
    repo: str
    number: int


@dataclass(frozen=True, slots=True)
class NpmSignal:
    """An exact published npm version that must exist."""

    package: str
    version: str


DoneSignal: TypeAlias = CommandSignal | PullRequestSignal | IssueSignal | NpmSignal
"""One parsed, read-only completion signal."""


@dataclass(frozen=True, slots=True)
class CheckOutcome:
    """The result of evaluating one lane: success and an auditable detail."""

    ok: bool
    detail: str


def _finite(now: float) -> None:
    if not math.isfinite(now):
        raise ValueError("timestamp must be finite")


def parse_signals(done: str, config: Config) -> tuple[DoneSignal, ...]:
    """Parse semicolon-separated Done signals, rejecting anything unsupported.

    A whitespace-only segment (for example a trailing semicolon) is ignored, but
    at least one valid signal is required. Unknown tokens, malformed references
    and an unconfigured ``command`` signal are rejected.
    """
    signals: list[DoneSignal] = []
    for raw in done.split(";"):
        token = raw.strip()
        if not token:
            continue
        if token == "command":
            if not config.verification_command:
                raise SignalError(
                    "Done signal 'command' requires a configured verification_command"
                )
            signals.append(CommandSignal())
            continue
        if token.startswith("pr:"):
            match = _GH_REF_RE.match(token[3:])
            if match is None:
                raise SignalError(f"malformed pull request signal: {token!r}")
            signals.append(
                PullRequestSignal(
                    owner=match.group("owner"),
                    repo=match.group("repo"),
                    number=int(match.group("number")),
                )
            )
            continue
        if token.startswith("issue:"):
            match = _GH_REF_RE.match(token[6:])
            if match is None:
                raise SignalError(f"malformed issue signal: {token!r}")
            signals.append(
                IssueSignal(
                    owner=match.group("owner"),
                    repo=match.group("repo"),
                    number=int(match.group("number")),
                )
            )
            continue
        if token.startswith("npm:"):
            match = _NPM_RE.match(token[4:])
            if match is None:
                raise SignalError(f"malformed npm signal: {token!r}")
            signals.append(
                NpmSignal(package=match.group("package"), version=match.group("version"))
            )
            continue
        raise SignalError(f"unknown Done signal: {token!r}")
    if not signals:
        raise SignalError("lane Done criteria declare no checks")
    return tuple(signals)


class Completion:
    """Durable completion claims, live checks and verified closure for lanes.

    ``config`` supplies the fixed verification argv and the process/HTTP bounds,
    ``store`` is the shared SQLite state (used to refuse closure while a lane has
    running or uncertain jobs) and ``registry`` owns every status transition.
    The injected ``client`` and ``npm_registry`` exist so tests can point the
    exact published-version check at a loopback server; production uses the
    public npm registry.
    """

    def __init__(
        self,
        config: Config,
        store: Store,
        registry: Registry,
        *,
        client: HttpClient | None = None,
        npm_registry: str = NPM_REGISTRY_URL,
    ) -> None:
        self._config = config
        self._store = store
        self._registry = registry
        self._client = client if client is not None else HttpClient()
        self._npm_registry = npm_registry.rstrip("/")

    def claim(self, key: str, evidence: str, now: float) -> Lane:
        """Open the fixed 300-second objection window; never extend on retry."""
        _finite(now)
        return self._registry.claim(key, evidence, now)

    def tick(self, now: float) -> list[Lane]:
        """Report newly emitted claim notifications and settle elapsed claims.

        One pass over open lanes: a ``done-claimed`` lane whose claim has not yet
        been announced is marked emitted once; a ``done-claimed`` lane whose
        deadline has elapsed is verified and settled. The claim deadline is never
        moved by a tick.
        """
        _finite(now)
        reported: list[Lane] = []
        for lane in self._registry.list(open_only=True):
            if lane.status != "done-claimed":
                continue
            emitted_now = False
            if not lane.claim_emitted:
                lane = self._registry.mark_claim_emitted(lane.key)
                emitted_now = True
            if lane.deadline is not None and now >= lane.deadline:
                reported.append(self._settle(lane, now))
            elif emitted_now:
                reported.append(lane)
        return reported

    def close(self, key: str, now: float) -> Lane:
        """Explicitly close a claimed lane after its window and live recheck.

        The objection window is never bypassed: a lane without a claim or with a
        still-open deadline is refused. An already closed lane is returned as-is.
        """
        _finite(now)
        lane = self._registry.get(key)
        if lane.status == "closed":
            return lane
        if lane.status != "done-claimed" or lane.deadline is None:
            raise RegistryError(f"lane {key!r} has no active completion claim")
        if now < lane.deadline:
            raise RegistryError(f"lane {key!r} objection window is still open")
        return self._settle(lane, now)

    def evaluate(self, lane: Lane) -> CheckOutcome:
        """Re-read every live signal and every declared worktree for one lane."""
        active = self._active_jobs(lane.key)
        if active:
            return CheckOutcome(
                False,
                "lane has unfinished execution: " + ", ".join(active),
            )
        try:
            signals = parse_signals(lane.done, self._config)
        except SignalError as exc:
            return CheckOutcome(False, str(exc))
        for signal in signals:
            outcome = self._verify_signal(signal, lane)
            if not outcome.ok:
                return outcome
        for tree in lane.worktrees:
            outcome = self._verify_worktree(lane, tree)
            if not outcome.ok:
                return outcome
        return CheckOutcome(True, "all live signals and declared worktrees verified")

    def _settle(self, lane: Lane, now: float) -> Lane:
        """Verify, re-verify immediately before closure, then transition by CAS."""
        first = self.evaluate(lane)
        if not first.ok:
            return self._fail(lane, first.detail, now)
        second = self.evaluate(lane)
        if not second.ok:
            return self._fail(lane, second.detail, now)
        try:
            return self._registry.settle(lane.key, lane.revision, now, second.detail)
        except RegistryError:
            # A racing objection or re-claim changed the revision: it wins.
            return self._registry.get(lane.key)

    def _fail(self, lane: Lane, detail: str, now: float) -> Lane:
        try:
            return self._registry.transition(
                lane.key, "not-done", detail, now, expected_revision=lane.revision
            )
        except RegistryError:
            return self._registry.get(lane.key)

    def _active_jobs(self, key: str) -> list[str]:
        placeholders = ", ".join("?" for _ in _ACTIVE_JOB_STATUSES)
        with self._store.connection() as connection:
            rows = connection.execute(
                f"SELECT id FROM jobs WHERE lane = ? AND status IN ({placeholders})",
                (key, *_ACTIVE_JOB_STATUSES),
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def _verify_signal(self, signal: DoneSignal, lane: Lane) -> CheckOutcome:
        if isinstance(signal, CommandSignal):
            result = self._run(self._config.verification_command, lane)
            if result.success:
                return CheckOutcome(True, "configured verification command succeeded")
            return CheckOutcome(
                False, f"verification command failed: {result.error or 'failed'}"
            )
        if isinstance(signal, PullRequestSignal):
            endpoint = (
                f"repos/{signal.owner}/{signal.repo}/pulls/{signal.number}"
            )
            merged = self._gh_api(lane, endpoint, ".merged_at")
            if merged is None:
                return CheckOutcome(False, "pull request check could not be read")
            try:
                stamp = datetime.fromisoformat(merged.strip())
            except ValueError:
                return CheckOutcome(
                    False,
                    f"pull request {signal.owner}/{signal.repo}#{signal.number} "
                    "is not merged",
                )
            if stamp.tzinfo is None:
                return CheckOutcome(False, "pull request merge timestamp is invalid")
            return CheckOutcome(
                True,
                f"pull request {signal.owner}/{signal.repo}#{signal.number} is merged",
            )
        if isinstance(signal, IssueSignal):
            endpoint = f"repos/{signal.owner}/{signal.repo}/issues/{signal.number}"
            state = self._gh_api(lane, endpoint, ".state")
            if state is None:
                return CheckOutcome(False, "issue check could not be read")
            if state.strip() != "closed":
                return CheckOutcome(
                    False,
                    f"issue {signal.owner}/{signal.repo}#{signal.number} is not closed",
                )
            return CheckOutcome(
                True, f"issue {signal.owner}/{signal.repo}#{signal.number} is closed"
            )
        # Remaining union member: NpmSignal.
        url = (
            f"{self._npm_registry}/{quote(signal.package, safe='')}/"
            f"{quote(signal.version, safe='')}"
        )
        try:
            record = self._client.request("GET", url, {})
        except TransportError as exc:
            return CheckOutcome(
                False, f"npm {signal.package}@{signal.version} is not published: {exc}"
            )
        if (
            not isinstance(record, dict) or record.get("name") != signal.package
            or record.get("version") != signal.version
        ):
            return CheckOutcome(False, "npm response does not prove the requested version")
        return CheckOutcome(
            True, f"npm {signal.package}@{signal.version} is published"
        )

    def _gh_api(self, lane: Lane, endpoint: str, projection: str) -> str | None:
        result = self._run(("gh", "api", endpoint, "--jq", projection), lane)
        if not result.success:
            return None
        return result.text

    def _verify_worktree(self, lane: Lane, tree: Path) -> CheckOutcome:
        path = tree if tree.is_absolute() else lane.cwd / tree
        if not path.is_dir():
            return CheckOutcome(False, f"declared worktree does not exist: {path}")
        status = self._run(("git", "-C", str(path), "status", "--porcelain"), lane, path)
        if not status.success:
            return CheckOutcome(False, f"worktree has no readable Git state: {path}")
        if status.text.strip():
            return CheckOutcome(
                False, f"worktree has uncommitted or untracked changes: {path}"
            )
        remotes = self._run(("git", "-C", str(path), "remote"), lane, path)
        if not remotes.success or not remotes.text.strip():
            return CheckOutcome(False, f"worktree has no configured remote: {path}")
        fetched = self._run(
            ("git", "-C", str(path), "fetch", "--all", "--prune"), lane, path
        )
        if not fetched.success:
            return CheckOutcome(False, f"worktree remote refs are unavailable: {path}")
        unpushed = self._run(
            ("git", "-C", str(path), "rev-list", "HEAD", "--not", "--remotes"), lane, path
        )
        if not unpushed.success:
            return CheckOutcome(False, f"worktree push state could not be read: {path}")
        if unpushed.text.strip():
            return CheckOutcome(
                False, f"worktree has commits not pushed to its remotes: {path}"
            )
        return CheckOutcome(True, f"worktree is clean and pushed: {path}")

    def _run(
        self, command: tuple[str, ...], lane: Lane, cwd: Path | None = None
    ) -> CheckResult:
        return run_command(
            command,
            lane.cwd if cwd is None else cwd,
            self._config.timeout_seconds,
            child_environment(self._config),
        )


__all__ = [
    "CheckOutcome",
    "CommandSignal",
    "Completion",
    "DoneSignal",
    "IssueSignal",
    "NPM_REGISTRY_URL",
    "NpmSignal",
    "PullRequestSignal",
    "SignalError",
    "parse_signals",
]
