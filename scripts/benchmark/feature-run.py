#!/usr/bin/env python3
"""Stage scripts/benchmark on the remote host and start the approved five-cell run detached.

Dry-run is the default and only prints the plan (no ssh, no scp, no rsync, no cluster job).
--run --approved performs the plan; the benchmark itself stays the reviewed
benchmark.py --run --approved runner, so the five cells, window policy, UID ledger and
truth/query assets are unchanged.

Guards enforced before anything is started:
  * the six runner files must hash to feature_common.RUNNER_SHA256, both locally (before
    rsync/scp) and again on the remote host after the sync; a different runner is refused
  * --baseline must hash to feature_common.BASELINE_SHA256, and the contract may differ from
    it only in the accounted fields (same-caliber check)
  * --stopped-evidence is required: this cluster still has stale TPCDS drivers that are
    phase=Running but stopped, so without a UID allowlist the window gate spins and fails
  * create-only: an existing tool dir, output root, runner log or pid file is refused
  * no remote deletion and no rsync --delete; this script never stops any pod or job
  * the runner is started with `setsid nohup ... &`, its PID is written next to the output
    root, read from stdout only (ssh stderr carries warnings), and probed with `kill -0`
    right after launch; a runner that dies immediately is reported as an error

Remote defaults (override by flag or environment):
  host          --remote        / FEATURE_REMOTE            shai
  kubeconfig    --kubeconfig    / FEATURE_KUBECONFIG        ~/workspace/k8s-config/clusterA.yaml
  JAVA_HOME     --java-home     / FEATURE_JAVA_HOME         /usr/lib/jvm/java-17-openjdk-amd64
  HADOOP_HOME   --hadoop-home   / FEATURE_HADOOP_HOME       /opt/hadoop-3.3.1
  SPARK_HOME    --spark-home    / FEATURE_SPARK_HOME        /opt/spark
  tool dir      --tool-dir                                  ~/runs/<UTC stamp>-tool-<card>
  window        --window-seconds                            1800
  output root   --output-root   local directory on the remote host, created by benchmark.py
  assets        --queries/--truth  local files on the remote host; benchmark.py uploads them
                                 to HDFS create-only, so they must already exist there
"""
import argparse
import json
import os
from pathlib import Path
import posixpath
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import baseline
import feature_common

CONTRACT_NAME = "contract.json"
STOPPED_NAME = "stopped-evidence.json"


def require(ok, message):
    feature_common.require(ok, message)


def default_remote():
    return os.environ.get("FEATURE_REMOTE", "shai")


def tool_dir_for(card):
    return "~/runs/%s-tool-%s" % (feature_common.utc_stamp(), card)


def validate_feature_contract(contract, base):
    """The feature contract must keep every frozen five-cell parameter identical."""
    baseline.validate_contract(contract)
    for key in ("feature_commit", "parent_image", "feature_card", "image_bundle_sha256"):
        require(key in contract, "not a feature contract (missing %s); derive it with "
                                 "feature-contract.py" % key)
    require("@" not in str(contract["image"]), "contract image must not embed a digest")
    feature_common.require_digest_ref(contract["parent_image"], "parent_image")
    require(feature_common.is_commit(contract["feature_commit"]),
            "feature_commit must be 7-40 lowercase hex")
    require(feature_common.is_sha256_hex(contract["image_bundle_sha256"]),
            "image_bundle_sha256 must be 64 lowercase hex characters")
    require(contract["image_digest"] != base["image_digest"],
            "contract image_digest equals the baseline digest; a rerun would measure the same jar")
    diff = feature_common.differences(feature_common.strip_accounted(base),
                                      feature_common.strip_accounted(contract))
    require(not diff, "feature contract changed frozen five-cell parameters: " + "; ".join(diff[:5]))


def sync_argv(method, script_dir, remote, tool_dir):
    if method == "rsync":
        return ["rsync", "-az", "--exclude", "__pycache__", "--exclude", "*.pyc",
                "--exclude", "target", str(Path(script_dir).resolve()) + "/",
                "%s:%s/" % (remote, tool_dir)]
    files = [str(path) for path in sorted(Path(script_dir).iterdir()) if path.is_file()]
    require(files, "nothing to sync in " + str(script_dir))
    return ["scp", "-p"] + files + ["%s:%s/" % (remote, tool_dir)]


def launch_command(tool_dir, env, bench, runner_log, pid_file):
    """Detached launch; the inner sh writes its own PID and execs the runner.

    Because the shell execs the runner, the recorded PID is the surviving python PID even if
    setsid has to fork. The create-only preconditions already guarantee the log, pid and
    output paths are fresh.
    """
    # `env KUBECONFIG=~/x` would pass a literal "~" (tilde is not expanded after "=" in a
    # plain word), so home-relative environment values are written as $HOME instead.
    env_text = " ".join("%s=%s" % (key, "$HOME/" + value[2:] if value.startswith("~/") else value)
                        for key, value in env.items())
    bench_text = " ".join(bench)
    parent = posixpath.dirname(runner_log) or "."
    inner = ("mkdir -p %s && cd %s && exec setsid nohup env %s "
             "sh -c 'echo $$ > \"$1\"; shift; exec \"$@\"' _ %s %s "
             "> %s 2>&1 < /dev/null"
             % (parent, tool_dir, env_text, pid_file, bench_text, runner_log))
    return ("( %s ) & for _ in 1 2 3 4 5 6 7 8 9 10; do test -s %s && break; sleep 0.2; done; cat %s"
            % (inner, pid_file, pid_file))


def liveness_command(remote, runner_log):
    """Display form; execute() fills in the real PID after launch."""
    return "ssh %s 'kill -0 <pid> || exit 3; echo ALIVE; tail -n 5 %s'" % (remote, runner_log)


def parse_sha256sum(text):
    found = {}
    for line in (text or "").splitlines():
        fields = line.split()
        if len(fields) == 2 and feature_common.HEX_RE.fullmatch(fields[0]):
            found[posixpath.basename(fields[1].lstrip("*"))] = fields[0]
    return found


def verify_sha_output(text, expected, label):
    found = parse_sha256sum(text)
    for name, want in expected.items():
        got = found.get(name)
        require(got is not None,
                "%s: sha256 missing for %s in %r" % (label, name, (text or "")[-300:]))
        require(got == want, "%s: sha256 mismatch for %s (%s != %s)" % (label, name, got, want))


def parse_runner_pid(stdout):
    """Only stdout counts: every ssh invocation on this host writes warnings to stderr."""
    for line in reversed((stdout or "").splitlines()):
        token = line.strip()
        if token.isdigit():
            return int(token)
    return None


def build_plan(args):
    contract_path = Path(args.contract)
    require(contract_path.is_file(), "contract not found: " + str(contract_path))
    contract = json.loads(contract_path.read_text())
    baseline_path = Path(args.baseline)
    require(baseline_path.is_file(), "baseline contract not found: " + str(baseline_path))
    baseline_sha = feature_common.require_baseline_pin(baseline_path)
    base = json.loads(baseline_path.read_text())
    validate_feature_contract(contract, base)
    runner_pins = feature_common.verify_runner_hashes(HERE)

    remote = feature_common.require_remote_path(args.remote, "remote host")
    output_root = feature_common.require_remote_path(args.output_root.rstrip("/"), "output root")
    require("/" in output_root, "output root must be a directory path on the remote host")
    card = str(contract.get("feature_card") or args.card or "unknown")
    tool_dir = feature_common.require_remote_path(args.tool_dir or tool_dir_for(card), "tool dir")
    queries = feature_common.require_remote_path(args.queries, "queries path")
    truth = feature_common.require_remote_path(args.truth, "truth path")
    for label, value in (("--queries", queries), ("--truth", truth)):
        require("://" not in value,
                "%s must be a local path on the remote host (benchmark.py uploads it), "
                "not a URI" % label)
    kubeconfig = feature_common.require_remote_path(args.kubeconfig, "kubeconfig")
    java_home = feature_common.require_remote_path(args.java_home, "JAVA_HOME")
    hadoop_home = feature_common.require_remote_path(args.hadoop_home, "HADOOP_HOME")
    spark_home = feature_common.require_remote_path(args.spark_home, "SPARK_HOME")
    runner_log = feature_common.require_remote_path(output_root + ".runner.log", "runner log")
    pid_file = feature_common.require_remote_path(output_root + ".runner.pid", "pid file")
    stopped = Path(args.stopped_evidence)
    require(stopped.is_file(), "stopped evidence not found: " + str(stopped))

    env = {"KUBECONFIG": kubeconfig, "JAVA_HOME": java_home,
           "HADOOP_HOME": hadoop_home, "SPARK_HOME": spark_home}
    bench = ["python3", "-u", "benchmark.py", "--run", "--approved",
             "--contract", CONTRACT_NAME, "--queries", queries, "--truth", truth,
             "--output-root", output_root, "--window-seconds", str(args.window_seconds)]
    bench += ["--stopped-evidence", STOPPED_NAME]
    launch = launch_command(tool_dir, env, bench, runner_log, pid_file)

    guards = [{"path": tool_dir, "policy": "create-only"},
              {"path": output_root, "policy": "create-only"},
              {"path": runner_log, "policy": "create-only"},
              {"path": pid_file, "policy": "create-only"}]
    steps = []
    # The query/truth files must already exist on the remote host: benchmark.py uploads them
    # with `hdfs dfs -put` and validates their hashes, so a typo would fail after the sync.
    for label, value in (("queries", queries), ("truth", truth)):
        steps.append({"step": "precondition: %s exists %s" % (label, value), "kind": "require_file",
                      "path": value, "timeout": 60,
                      "argv": ["ssh", remote, "test -f " + value]})
    for guard in guards:
        steps.append({"step": "precondition: " + guard["policy"] + " " + guard["path"],
                      "kind": "probe", "path": guard["path"], "timeout": 60,
                      "argv": ["ssh", remote, "test ! -e " + guard["path"]]})
    steps.append({"step": "remote tool dir mkdir", "kind": "run", "timeout": 60,
                  "argv": ["ssh", remote, "mkdir -p " + tool_dir]})
    steps.append({"step": "sync benchmark sources (%s)" % args.sync_method,
                  "kind": "run", "timeout": args.sync_timeout,
                  "argv": sync_argv(args.sync_method, HERE, remote, tool_dir)})
    remote_runner = ["%s/%s" % (tool_dir, name) for name in sorted(runner_pins)]
    steps.append({"step": "verify synced runner hashes", "kind": "verify_remote", "timeout": 120,
                  "expected": dict(runner_pins),
                  "argv": ["ssh", remote, "sha256sum " + " ".join(remote_runner)]})
    steps.append({"step": "upload feature contract", "kind": "run", "timeout": 120,
                  "argv": ["scp", str(contract_path),
                           "%s:%s/%s" % (remote, tool_dir, CONTRACT_NAME)]})
    contract_sha = feature_common.sha256_file(contract_path)
    steps.append({"step": "verify uploaded contract", "kind": "verify_remote", "timeout": 120,
                  "expected": {CONTRACT_NAME: contract_sha},
                  "argv": ["ssh", remote, "sha256sum %s/%s" % (tool_dir, CONTRACT_NAME)]})
    steps.append({"step": "upload stopped-driver evidence", "kind": "run", "timeout": 120,
                  "argv": ["scp", str(stopped),
                           "%s:%s/%s" % (remote, tool_dir, STOPPED_NAME)]})
    steps.append({"step": "launch detached", "kind": "launch", "timeout": args.launch_timeout,
                  "argv": ["ssh", remote, launch]})
    steps.append({"step": "verify runner alive", "kind": "liveness", "timeout": 60,
                  "argv": []})

    plan = {
        "status": "DRY_RUN_ONLY",
        "warnings": ["--stopped-evidence is required: stale TPCDS drivers stay phase=Running "
                     "after stopping, so without the UID allowlist the window gate spins for "
                     "window_seconds and then fails"],
        "remote": remote,
        "tool_dir": tool_dir,
        "output_root": output_root,
        "runner_log": runner_log,
        "pid_file": pid_file,
        "contract": str(contract_path),
        "contract_sha256": contract_sha,
        "baseline": str(baseline_path),
        "baseline_sha256": baseline_sha,
        "runner_sha256": runner_pins,
        "same_caliber_as_baseline": True,
        "feature_card": card,
        "feature_commit": contract["feature_commit"],
        "parent_image": contract["parent_image"],
        "image": contract["image"] + "@" + contract["image_digest"],
        "image_digest": contract["image_digest"],
        "image_bundle_sha256": contract["image_bundle_sha256"],
        "queries": queries,
        "truth": truth,
        "stopped_evidence": str(stopped),
        "stopped_evidence_sha256": feature_common.sha256_file(stopped),
        "window_seconds": args.window_seconds,
        "env": env,
        "guards": guards,
        "launch_command": launch,
        "commands": [{"step": step["step"], "kind": step["kind"],
                      "probe": step["kind"] in ("probe", "require_file"),
                      "command": feature_common.command_text(step["argv"])
                      if step["argv"] else liveness_command(remote, runner_log)}
                     for step in steps],
        "liveness_command": liveness_command(remote, runner_log),
        "follow": "ssh %s tail -f %s" % (remote, runner_log),
        "deletes_remote_files": False,
        "stops_foreign_jobs": False,
        "requires": "--run --approved (root review and user authorization first)",
    }
    return plan, steps


def run_command(argv, timeout):
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)


def execute(plan, steps):
    """Run the planned commands after every create-only precondition passes."""
    outputs = {}
    pid = None
    for step in steps:
        kind = step["kind"]
        argv = step["argv"]
        if kind == "liveness":
            require(pid is not None, "liveness check scheduled before a successful launch")
            argv = ["ssh", plan["remote"],
                    "kill -0 %d || exit 3; echo ALIVE; tail -n 5 %s" % (pid, plan["runner_log"])]
        result = run_command(argv, step["timeout"])
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        text = stdout + stderr
        if kind == "probe":
            require(result.returncode in (0, 1),
                    "cannot verify remote path %s: %s" % (step["path"], tail_text(text)))
            require(result.returncode == 0, "refuse pre-existing remote path: " + step["path"])
            continue
        if kind == "require_file":
            require(result.returncode in (0, 1),
                    "cannot verify remote asset %s: %s" % (step["path"], tail_text(text)))
            require(result.returncode == 0,
                    "asset missing on the remote host: " + step["path"]
                    + " (benchmark.py uploads it with hdfs dfs -put)")
            continue
        if kind == "verify_remote":
            require(result.returncode == 0, "%s failed (exit %d): %s"
                    % (step["step"], result.returncode, tail_text(text)))
            verify_sha_output(stdout, step["expected"], step["step"])
            outputs[step["step"]] = stdout
            continue
        if kind == "liveness":
            require(result.returncode == 0 and "ALIVE" in stdout,
                    "runner PID %d is not alive after launch (exit %d); log tail:\n%s"
                    % (pid, result.returncode, tail_text(stdout, 10)))
            plan["runner_log_tail"] = tail_text(stdout, 5)
            outputs[step["step"]] = stdout
            continue
        require(result.returncode == 0, "%s failed (exit %d): %s"
                % (step["step"], result.returncode, tail_text(text)))
        if kind == "launch":
            pid = parse_runner_pid(stdout)
            require(pid is not None,
                    "could not read the detached runner PID from stdout (%r). The job may "
                    "already be running: inspect %s and %s before deciding what to do."
                    % (tail_text(stdout, 3), plan["pid_file"], plan["runner_log"]))
        outputs[step["step"]] = stdout
    plan["status"] = "STARTED"
    plan["pid"] = pid
    plan["started_at"] = feature_common.utc_iso()
    return plan


def tail_text(text, limit=20):
    lines = [line for line in (text or "").splitlines() if line.strip()]
    return "\n".join(lines[-limit:])[:2000]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--contract", type=Path, required=True,
                        help="feature contract derived by feature-contract.py")
    parser.add_argument("--baseline", type=Path, default=HERE / "baseline-10m.json",
                        help="frozen baseline contract (sha256 pinned) for the same-caliber check")
    parser.add_argument("--queries", required=True,
                        help="local path on the remote host of the 100 query vectors")
    parser.add_argument("--truth", required=True,
                        help="local path on the remote host of the full top-2000 truth")
    parser.add_argument("--output-root", required=True,
                        help="output directory on the remote host (created by benchmark.py)")
    parser.add_argument("--tool-dir", help="remote tool dir (default ~/runs/<UTC>-tool-<card>)")
    parser.add_argument("--stopped-evidence", type=Path, required=True,
                        help="UID-fenced JSON of drivers verified stopped before the run; "
                             "required because stale TPCDS drivers stay phase=Running")
    parser.add_argument("--remote", default=default_remote())
    parser.add_argument("--kubeconfig", default=os.environ.get(
        "FEATURE_KUBECONFIG", "~/workspace/k8s-config/clusterA.yaml"))
    parser.add_argument("--java-home", default=os.environ.get(
        "FEATURE_JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"))
    parser.add_argument("--hadoop-home", default=os.environ.get(
        "FEATURE_HADOOP_HOME", "/opt/hadoop-3.3.1"))
    parser.add_argument("--spark-home", default=os.environ.get(
        "FEATURE_SPARK_HOME", "/opt/spark"))
    parser.add_argument("--card", help="card id used in the default tool dir")
    parser.add_argument("--window-seconds", type=int, default=1800)
    parser.add_argument("--sync-method", choices=["rsync", "scp"], default="rsync")
    parser.add_argument("--sync-timeout", type=int, default=600)
    parser.add_argument("--launch-timeout", type=int, default=120)
    parser.add_argument("--dry-run", action="store_true", help="print the plan only (default)")
    parser.add_argument("--run", action="store_true", help="execute the plan")
    parser.add_argument("--approved", action="store_true",
                        help="required with --run after root review and user authorization")
    args = parser.parse_args(argv)
    if args.run and args.dry_run:
        parser.error("--run and --dry-run are mutually exclusive")
    if args.run and not args.approved:
        parser.error("--run requires --approved after root review and user authorization")

    plan, steps = build_plan(args)
    if not args.run:
        print(json.dumps(plan, indent=2))
        return plan
    result = execute(plan, steps)
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        print("REFUSED:", error, file=sys.stderr)
        sys.exit(2)
