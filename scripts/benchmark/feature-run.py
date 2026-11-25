#!/usr/bin/env python3
"""Stage scripts/benchmark on the remote host and start the approved five-cell run detached.

Dry-run is the default and only prints the plan (no ssh, no scp, no rsync, no cluster
job). --run --approved performs the plan; the benchmark itself stays the reviewed
benchmark.py --run --approved runner, so the five cells, window policy, UID ledger and
truth/query assets are unchanged.

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

Guarantees:
  * create-only: an existing tool dir, output root, runner log or pid file is refused
  * no remote deletion and no rsync --delete; this script never stops any pod or job
  * the runner is started with `setsid nohup ... &`, so it survives the ssh session; its
    PID is written next to the output root and echoed back
  * paths interpolated into ssh command strings are restricted to safe path characters
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

    Because the shell execs the runner, the recorded PID is the surviving python PID even
    if setsid has to fork. The create-only preconditions already guarantee the log, pid
    and output paths are fresh.
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


def build_plan(args):
    contract_path = Path(args.contract)
    require(contract_path.is_file(), "contract not found: " + str(contract_path))
    contract = json.loads(contract_path.read_text())
    baseline_path = Path(args.baseline)
    require(baseline_path.is_file(), "baseline contract not found: " + str(baseline_path))
    base = json.loads(baseline_path.read_text())
    validate_feature_contract(contract, base)

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
    stopped = None
    if args.stopped_evidence:
        stopped = Path(args.stopped_evidence)
        require(stopped.is_file(), "stopped evidence not found: " + str(stopped))

    env = {"KUBECONFIG": kubeconfig, "JAVA_HOME": java_home,
           "HADOOP_HOME": hadoop_home, "SPARK_HOME": spark_home}
    bench = ["python3", "-u", "benchmark.py", "--run", "--approved",
             "--contract", CONTRACT_NAME, "--queries", queries, "--truth", truth,
             "--output-root", output_root, "--window-seconds", str(args.window_seconds)]
    if stopped is not None:
        bench += ["--stopped-evidence", STOPPED_NAME]
    launch = launch_command(tool_dir, env, bench, runner_log, pid_file)

    guards = [{"path": tool_dir, "policy": "create-only"},
              {"path": output_root, "policy": "create-only"},
              {"path": runner_log, "policy": "create-only"},
              {"path": pid_file, "policy": "create-only"}]
    steps = []
    for guard in guards:
        steps.append({"step": "precondition: create-only " + guard["path"], "probe": True,
                      "path": guard["path"], "timeout": 60,
                      "argv": ["ssh", remote, "test ! -e " + guard["path"]]})
    steps.append({"step": "remote tool dir mkdir", "timeout": 60,
                  "argv": ["ssh", remote, "mkdir -p " + tool_dir]})
    steps.append({"step": "sync benchmark sources (%s)" % args.sync_method,
                  "timeout": args.sync_timeout,
                  "argv": sync_argv(args.sync_method, HERE, remote, tool_dir)})
    steps.append({"step": "upload feature contract", "timeout": 120,
                  "argv": ["scp", str(contract_path),
                           "%s:%s/%s" % (remote, tool_dir, CONTRACT_NAME)]})
    if stopped is not None:
        steps.append({"step": "upload stopped-driver evidence", "timeout": 120,
                      "argv": ["scp", str(stopped),
                               "%s:%s/%s" % (remote, tool_dir, STOPPED_NAME)]})
    steps.append({"step": "launch detached", "timeout": args.launch_timeout,
                  "argv": ["ssh", remote, launch]})

    plan = {
        "status": "DRY_RUN_ONLY",
        "remote": remote,
        "tool_dir": tool_dir,
        "output_root": output_root,
        "runner_log": runner_log,
        "pid_file": pid_file,
        "contract": str(contract_path),
        "contract_sha256": feature_common.sha256_file(contract_path),
        "baseline": str(baseline_path),
        "baseline_sha256": feature_common.sha256_file(baseline_path),
        "same_caliber_as_baseline": True,
        "feature_card": card,
        "feature_commit": contract["feature_commit"],
        "parent_image": contract["parent_image"],
        "image": contract["image"] + "@" + contract["image_digest"],
        "image_digest": contract["image_digest"],
        "image_bundle_sha256": contract["image_bundle_sha256"],
        "queries": queries,
        "truth": truth,
        "stopped_evidence": str(stopped) if stopped is not None else None,
        "window_seconds": args.window_seconds,
        "env": env,
        "guards": guards,
        "commands": [{"step": step["step"], "probe": bool(step.get("probe")),
                      "command": feature_common.command_text(step["argv"])} for step in steps],
        "launch_command": launch,
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
    for step in steps:
        result = run_command(step["argv"], step["timeout"])
        text = (result.stdout or "") + (result.stderr or "")
        if step.get("probe"):
            require(result.returncode in (0, 1),
                    "cannot verify remote path %s: %s" % (step["path"], text.strip()[:300]))
            require(result.returncode == 0,
                    "refuse pre-existing remote path: " + step["path"])
            continue
        require(result.returncode == 0, "%s failed (exit %d): %s"
                % (step["step"], result.returncode, text.strip()[-1500:]))
        outputs[step["step"]] = text
    pid_lines = [line for line in outputs.get("launch detached", "").splitlines() if line.strip()]
    require(pid_lines and pid_lines[-1].strip().isdigit(),
            "could not read the detached runner PID from: %r" % (pid_lines[-1:] or ""))
    plan["status"] = "STARTED"
    plan["pid"] = int(pid_lines[-1].strip())
    plan["started_at"] = feature_common.utc_iso()
    return plan


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--contract", type=Path, required=True,
                        help="feature contract derived by feature-contract.py")
    parser.add_argument("--baseline", type=Path, default=HERE / "baseline-10m.json",
                        help="frozen baseline contract used for the same-caliber check")
    parser.add_argument("--queries", required=True,
                        help="local path on the remote host of the 100 query vectors")
    parser.add_argument("--truth", required=True,
                        help="local path on the remote host of the full top-2000 truth")
    parser.add_argument("--output-root", required=True,
                        help="output directory on the remote host (created by benchmark.py)")
    parser.add_argument("--tool-dir", help="remote tool dir (default ~/runs/<UTC>-tool-<card>)")
    parser.add_argument("--stopped-evidence", type=Path,
                        help="UID-fenced JSON of drivers verified stopped before the run")
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
