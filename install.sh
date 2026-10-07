#!/usr/bin/env bash
# Minimal one-command bootstrap, invoked as:
#   curl -fsSL https://raw.githubusercontent.com/ashmoonori-afk/ultra-agent/main/install.sh | bash
set -u
set -o pipefail

# An inherited xtrace would echo command lines (and could echo secrets); disable it first.
case $- in
    *x*) set +x ;;
esac
umask 077

DEFAULT_SOURCE="https://github.com/ashmoonori-afk/ultra-agent/archive/refs/heads/main.zip"
UV_INSTALLER_URL="https://astral.sh/uv/install.sh"

fail() {
    printf 'install.sh: %s\n' "$1" >&2
    exit "${2:-1}"
}

INSTALL_DIR="${ULTRA_AGENT_INSTALL_DIR:-$HOME/.local/share/ultra-agent}"
VENV="$INSTALL_DIR/venv"
VENV_PYTHON="$VENV/bin/python"
AGENT_BIN="$VENV/bin/ultra-agent"
SOURCE="${ULTRA_AGENT_SOURCE:-$DEFAULT_SOURCE}"
WORKSPACE="${UA_WORKSPACE:-$INSTALL_DIR/workspace}"

mkdir -p "$INSTALL_DIR" || fail "cannot create $INSTALL_DIR"

# flock releases the lock on process exit, so no stale lock file can block a later run.
if command -v flock >/dev/null 2>&1; then
    exec 9>"$INSTALL_DIR/.install.lock" || fail "cannot open install lock in $INSTALL_DIR"
    flock -w 120 9 || fail "another installation is already in progress"
fi

UV=""

uv_require() {
    if [ -n "$UV" ]; then
        return 0
    fi
    if command -v uv >/dev/null 2>&1; then
        UV="$(command -v uv)"
    else
        command -v curl >/dev/null 2>&1 || fail "curl is required to install uv"
        curl -fsSL "$UV_INSTALLER_URL" |
            env UV_NO_MODIFY_PATH=1 UV_INSTALL_DIR="$INSTALL_DIR/tools" sh ||
            fail "uv installation failed"
        export PATH="$INSTALL_DIR/tools:$PATH"
        UV="$(command -v uv 2>/dev/null)" || fail "uv was not found after installation"
    fi
    printf 'install.sh: using uv at %s\n' "$UV" >&2
}

python_supported() {
    "$1" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' >/dev/null 2>&1
}

find_python() {
    local candidate path
    for candidate in python3.14 python3.13 python3.12 python3.11 python3 python; do
        path="$(command -v "$candidate" 2>/dev/null)" || continue
        [ -n "$path" ] || continue
        if python_supported "$path"; then
            printf '%s\n' "$path"
            return 0
        fi
    done
    return 1
}

if [ ! -x "$VENV_PYTHON" ]; then
    if PY="$(find_python)"; then
        printf 'install.sh: creating environment with %s\n' "$PY" >&2
        if ! "$PY" -m venv "$VENV" </dev/null; then
            uv_require
            "$UV" venv --python "$PY" "$VENV" </dev/null ||
                fail "cannot create a virtual environment at $VENV"
        fi
    else
        uv_require
        printf 'install.sh: creating environment with uv (managed Python 3.11)\n' >&2
        "$UV" venv --python 3.11 "$VENV" </dev/null || fail "uv could not create an environment at $VENV"
    fi
fi

printf 'install.sh: installing ultra-agent from %s\n' "$SOURCE" >&2
if [ -x "$VENV/bin/pip" ]; then
    "$VENV_PYTHON" -m pip install "$SOURCE" </dev/null || fail "package installation failed"
else
    uv_require
    "$UV" pip install --python "$VENV_PYTHON" "$SOURCE" </dev/null || fail "package installation failed"
fi

[ -x "$AGENT_BIN" ] || fail "installed command $AGENT_BIN is missing"

# setup reads prompts from /dev/tty; stdin stays /dev/null so a piped script is never consumed.
if "$AGENT_BIN" --workspace "$WORKSPACE" setup </dev/null; then
    printf 'install.sh: setup complete; ultra-agent installed at %s\n' "$AGENT_BIN"
else
    status=$?
    fail "installed setup command failed (exit $status)" "$status"
fi
