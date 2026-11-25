#!/usr/bin/env python3
"""Shared offline helpers for feature-image / feature-contract / feature-run.

Standard library only; nothing in this module runs docker, ssh, mvn, kubectl or a
cluster. The three feature scripts import it from their own directory, the same way
benchmark.py imports baseline.py.

Environment defaults are documented in each script's docstring:
  FEATURE_REMOTE, FEATURE_KUBECONFIG, FEATURE_JAVA_HOME, FEATURE_HADOOP_HOME,
  FEATURE_SPARK_HOME.
"""
import hashlib
import json
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path

# The only fields a derived feature contract may differ in. Everything else must stay
# semantically identical to the frozen baseline contract (compared as canonical JSON).
ACCOUNTED_FIELDS = (
    "image",
    "image_digest",
    "image_bundle_sha256",
    "feature_commit",
    "parent_image",
    "feature_card",
)

# Frozen baseline contract (scripts/benchmark/baseline-10m.json). Any file read through
# --baseline must hash to this value, otherwise the five-cell comparison is not the same one.
BASELINE_SHA256 = "a29c97557f178e877ff9c9d29c42dfd186e13ffce3fde250ef427520342a19f4"

# The runner files that actually submit and validate cells, plus the frozen text-cell contract
# and query asset (the optional sixth cell is only reviewable when these bytes are pinned). They
# may only be synced to the remote host when their bytes match the reviewed version (the one that
# produced C5/b102cc7 behaviour plus the single-tolerance window fix, the 2026-11-25 window policy
# -- foreign benchmark activity is recorded, coverage/API/marker failures still void a cell -- and
# the text BM25 k=10 cell added by card #3285). Anything else is refused before ssh.
RUNNER_SHA256 = {
    "submit.py": "53bf754e4271f2cc6f7340d2d5a25e1bd055118476f68513b621fec9507b35ea",
    "baseline.py": "5c9f31f5c57c2be3f3b0c30cd0b26eeba268021f9ac3ab0558919583838a0420",
    "pilot.py": "1e9f57b422cf98b98b62c87eb9c28d42b5f1c3931b5a026267389c4cedc1c9d7",
    "benchmark.py": "845231d665829d806b3a8dc8f3c92227da25e19281b342aada0e436c0a76f76d",
    "BaselineBenchmark.java": "258dbcde51da43c59dc57c893a2d0138bef0f4b6b77faf52a5a08ce8a1e7a4d2",
    "verified-stopped-drivers.json": "299287912c4dba978ef58c4333c2a7337444d5b92cffc027a39244e340577db8",
    "text-10m.json": "efba1225617f4497ca8984b2186d4b1c9156c7c9955f79c44c83099a9de62c2f",
    "text-queries-10m.jsonl": "9d04605cc228cb9b7f9a9bc22a73ef9fd917daa0eb4d51bb808800ac660d3bc3",
}

DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
HEX_RE = re.compile(r"[0-9a-f]{64}")
COMMIT_RE = re.compile(r"[0-9a-f]{7,40}")
CARD_RE = re.compile(r"#?[0-9]+")
# Characters allowed in a remote path that is interpolated into an ssh command string.
# No whitespace, quotes, $, ;, |, &, *, ?, brackets or backslashes, so no shell injection.
REMOTE_PATH_RE = re.compile(r"[A-Za-z0-9._~/@:+,=-]+")


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    return sha256_bytes(Path(path).read_bytes())


def is_digest(value):
    return isinstance(value, str) and DIGEST_RE.fullmatch(value) is not None


def is_sha256_hex(value):
    return isinstance(value, str) and HEX_RE.fullmatch(value) is not None


def is_commit(value):
    return isinstance(value, str) and COMMIT_RE.fullmatch(value) is not None


def require_digest_ref(ref, label="image"):
    """Accept only repo@sha256:<64 lowercase hex>; floating tags are refused."""
    require(isinstance(ref, str) and ref and ref == ref.strip() and " " not in ref,
            label + " must be a single token")
    parts = ref.split("@")
    require(len(parts) == 2 and bool(parts[0]),
            label + " must be repo@sha256:<64 hex>, never a floating tag")
    require(is_digest(parts[1]), label + " digest must be sha256:<64 lowercase hex>")
    return ref


def require_remote_path(value, label="remote path"):
    """Validate a path that will be interpolated into a remote shell command."""
    require(isinstance(value, str) and value and value == value.strip(),
            label + " must be a non-empty path without leading/trailing spaces")
    require(REMOTE_PATH_RE.fullmatch(value) is not None,
            label + " may only contain path characters; no spaces, quotes, $, ;, | or &")
    require("~" not in value[1:], label + " may use ~ only as the first character")
    require(".." not in value.split("/"), label + " must not contain .. path segments")
    return value


def require_baseline_pin(path):
    """Refuse a baseline contract whose bytes differ from the frozen one."""
    actual = sha256_file(path)
    require(actual == BASELINE_SHA256,
            "baseline contract is not the frozen file: %s (expected %s)"
            % (actual, BASELINE_SHA256))
    return actual


def verify_runner_hashes(script_dir, expected=None):
    """Verify the bytes that would be synced to the remote host against the runner pins."""
    expected = RUNNER_SHA256 if expected is None else expected
    actual = {}
    for name, want in expected.items():
        path = Path(script_dir) / name
        require(path.is_file(), "runner file missing: " + str(path))
        got = sha256_file(path)
        require(got == want,
                "runner pin mismatch for %s: %s != %s; refusing to sync a different runner"
                % (name, got, want))
        actual[name] = got
    return actual


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def strip_accounted(contract):
    return {key: value for key, value in contract.items() if key not in ACCOUNTED_FIELDS}


def differences(left, right, path=""):
    """Structural differences as readable paths; an empty list means semantically equal."""
    if isinstance(left, dict) and isinstance(right, dict):
        out = []
        for key in sorted(set(left) | set(right)):
            child = path + "/" + str(key)
            if key not in left or key not in right:
                out.append(child + " (present on one side only)")
            else:
                out.extend(differences(left[key], right[key], child))
        return out
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return ["%s (length %d != %d)" % (path, len(left), len(right))]
        out = []
        for index, (a, b) in enumerate(zip(left, right)):
            out.extend(differences(a, b, "%s[%d]" % (path, index)))
        return out
    if type(left) is not type(right) or left != right:
        return ["%s (%r != %r)" % (path or "/", left, right)]
    return []


def utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def utc_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def command_text(argv):
    return " ".join(shlex.quote(str(item)) for item in argv)


def _gitdir(worktree):
    dot = Path(worktree) / ".git"
    if dot.is_dir():
        return dot.resolve()
    if dot.is_file():
        text = dot.read_text().strip()
        if text.startswith("gitdir:"):
            target = Path(text.split("gitdir:", 1)[1].strip())
            if not target.is_absolute():
                target = dot.parent / target
            return target.resolve()
    return None


def _commit(text):
    value = (text or "").strip()
    return value if COMMIT_RE.fullmatch(value) else None


def git_head(worktree):
    """Read HEAD without spawning git, so dry-run stays subprocess-free (worktree aware)."""
    gitdir = _gitdir(worktree)
    if gitdir is None:
        return None
    common = gitdir
    commondir = gitdir / "commondir"
    if commondir.is_file():
        target = Path(commondir.read_text().strip())
        if not target.is_absolute():
            target = gitdir / target
        common = target.resolve()
    head_path = gitdir / "HEAD"
    if not head_path.is_file():
        return None
    head = head_path.read_text().strip()
    if head.startswith("ref:"):
        ref = head.split("ref:", 1)[1].strip()
        for base in (gitdir, common):
            candidate = base / ref
            if candidate.is_file():
                return _commit(candidate.read_text())
        packed = common / "packed-refs"
        if packed.is_file():
            for line in packed.read_text().splitlines():
                if not line or line.startswith("#") or line.startswith("^"):
                    continue
                fields = line.split()
                if len(fields) == 2 and fields[1] == ref:
                    return _commit(fields[0])
        return None
    return _commit(head)
