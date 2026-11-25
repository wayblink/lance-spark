#!/usr/bin/env python3
"""Approved B-only exact truth execution. Reuses the reviewed baseline pilot lifecycle helpers."""
import argparse
import datetime
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approved", action="store_true")
    parser.add_argument("--helpers", required=True, type=Path)
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--driver-node", default="cpu-1122.host.platform.shaipower.com")
    args = parser.parse_args()
    if not args.approved:
        parser.error("B-stage root authorization required")
    sys.path.insert(0, str(args.helpers))
    pilot = importlib.import_module("pilot")
    baseline = importlib.import_module("baseline")
    contract = json.loads(args.contract.read_text())
    baseline.validate_contract(contract)
    args.output.mkdir(parents=True, exist_ok=False)
    runid = "b3271-truth-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
    namespace = contract["cluster"]["namespace"]
    started = time.time(); deadline = started + 7200
    image = contract["image"] + "@" + contract["image_digest"]
    output = args.output; ledger = output / "created.json"
    (output / "contract.json").write_text(args.contract.read_text())
    (output / "run.json").write_text(json.dumps({"run_id": runid, "started_ms": int(started * 1000),
        "stage": "B-exact-truth", "image": image, "deadline_seconds": 7200}, indent=2))
    if args.driver_node:
        node = pilot.kube(["get", "node", args.driver_node, "-o", "json"])
        baseline.require(node.returncode == 0, "cannot inspect driver node")
        info = json.loads(node.stdout)
        conditions = {c["type"]: c["status"] for c in info["status"]["conditions"]}
        baseline.require(not info["spec"].get("unschedulable", False) and not info["spec"].get("taints", [])
            and conditions.get("Ready") == "True" and conditions.get("DiskPressure") == "False"
            and conditions.get("MemoryPressure") == "False" and conditions.get("PIDPressure") == "False",
            "driver node unhealthy or tainted")
        (output / "node-before.json").write_text(json.dumps(info, indent=2))
    existing, api_ok = pilot.get_pod(namespace, runid)
    baseline.require(api_ok and existing is None, "refuse existing pod or unavailable API")
    before = pilot.kube(["get", "pods", "-n", namespace, "-o", "json"])
    baseline.require(before.returncode == 0, "pod before snapshot unavailable")
    (output / "pods-before.json").write_text(json.dumps(pilot.redact_json(json.loads(before.stdout)), indent=2))
    build = output / "build"; (build / "classes").mkdir(parents=True)
    compiled = pilot.call(["docker", "run", "--rm", "--user", "0", "--entrypoint", "/bin/bash",
        "-v", str(HERE / "TruthGenerator.java") + ":/src/TruthGenerator.java:ro", "-v", str(build) + ":/run",
        image, "-lc", 'set -e; "$JAVA_HOME/bin/javac" -proc:none -cp "/opt/spark/jars/*" -d /run/classes /src/TruthGenerator.java; "$JAVA_HOME/bin/jar" cf /run/TruthGenerator.jar -C /run/classes .'], timeout=300)
    (build / "compile.log").write_text(compiled.stdout + compiled.stderr)
    baseline.require(compiled.returncode == 0, "truth JAR compile failed")
    defaults = Path(os.environ.get("SPARK_DEFAULTS", "/opt/apache-kyuubi-1.11.0-bin/conf/spark-defaults.conf"))
    platform = {}
    for line in defaults.read_text().splitlines():
        if line.startswith("spark.hadoop.fs.s3a.") and "=" in line:
            key, value = line.split("=", 1); platform[key] = value
    access = os.environ.get("AWS_ACCESS_KEY_ID", platform.get("spark.hadoop.fs.s3a.access.key"))
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", platform.get("spark.hadoop.fs.s3a.secret.key"))
    endpoint = os.environ.get("AWS_ENDPOINT_URL", platform.get("spark.hadoop.fs.s3a.endpoint"))
    baseline.require(access and secret and endpoint, "object-store credentials unavailable")
    secrets = (access, secret)
    conf = baseline.spark_conf(contract)
    evidence = contract["cluster"]["evidence_root"] + "/" + runid
    truth_output = evidence + "/truth"
    contract_uri = evidence + "/contract.json"
    conf.update({"spark.kubernetes.driver.pod.name": runid, "spark.kubernetes.driver.deleteOnTermination": "false",
        "spark.kubernetes.submission.waitAppCompletion": "false", "spark.eventLog.dir": evidence + "/spark-events",
        "spark.driver.maxResultSize": "256m", "spark.sql.catalog.lance.storage.endpoint": endpoint,
        "spark.sql.catalog.lance.storage.access_key_id": access, "spark.sql.catalog.lance.storage.secret_access_key": secret,
        "spark.sql.catalog.lance.storage.region": "us-east-1", "spark.sql.catalog.lance.storage.aws_allow_http": "true",
        "spark.sql.catalog.lance.storage.virtual_hosted_style_request": "false"})
    if args.driver_node:
        conf["spark.kubernetes.driver.node.selector.kubernetes.io/hostname"] = args.driver_node
    for scope in ("spark.kubernetes.driverEnv.", "spark.executorEnv."):
        for key, value in {"AWS_ACCESS_KEY_ID": access, "AWS_SECRET_ACCESS_KEY": secret, "AWS_ENDPOINT_URL": endpoint,
                "AWS_ENDPOINT": endpoint, "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true"}.items():
            conf[scope + key] = value
    safe = {k: "<redacted>" if any(x in k.lower() for x in ("secret", "access_key")) else v for k, v in conf.items()}
    (output / "spark-conf-redacted.json").write_text(json.dumps(safe, indent=2))
    hdfs = os.environ.get("HADOOP_HOME", "/opt/hadoop-3.3.1") + "/bin/hdfs"
    for command in ([hdfs, "dfs", "-mkdir", "-p", evidence + "/spark-events"],
                    [hdfs, "dfs", "-put", str(args.contract), contract_uri]):
        result = pilot.call(command, timeout=45)
        baseline.require(result.returncode == 0, "truth staging create failed")
    (output / "locations.json").write_text(json.dumps({"evidence": evidence, "truth": truth_output, "contract": contract_uri}, indent=2))
    command = [os.environ.get("SPARK_HOME", "/opt/spark") + "/bin/spark-submit", "--master", contract["cluster"]["master"],
        "--deploy-mode", "cluster", "--name", runid, "--class", "TruthGenerator"]
    for key, value in conf.items(): command.extend(["--conf", key + "=" + value])
    command.extend([str(build / "TruthGenerator.jar"), contract_uri, truth_output, "10"])
    uid = None; state = "SUBMITTING"; watch = None; markers = []; passed = False
    errors = {}
    try:
        submitted = pilot.call(command, timeout=contract["timeouts"]["submit_seconds"])
        (output / "submit.log").write_text(pilot.redact(submitted.stdout + submitted.stderr, secrets))
        baseline.require(submitted.returncode == 0, "spark-submit failed")
        discovery_deadline = min(deadline, time.time() + 120)
        while time.time() < discovery_deadline:
            pod, api_ok = pilot.get_pod(namespace, runid)
            if pod:
                uid = pod["metadata"]["uid"]; baseline.record_created(ledger, runid, namespace, runid, uid); break
            baseline.require(api_ok, "API unavailable finding new pod")
            time.sleep(2)
        baseline.require(uid is not None, "new driver not found")
        print(json.dumps({"status": "SUBMITTED", "run_id": runid, "uid": uid, "output": str(output), "truth": truth_output}), flush=True)
        with (output / "pod-watch.jsonl").open("w") as out:
            while time.time() < deadline:
                pod, api_ok = pilot.get_pod(namespace, runid); state = baseline.pod_state(pod, api_ok)
                if pod:
                    baseline.require(pod["metadata"]["uid"] == uid, "driver name reused")
                    (output / "pod-latest.json").write_text(json.dumps(pilot.redact_json(pod), indent=2))
                if state in ("SUCCEEDED", "FAILED", "POD_GONE"): break
                if watch is None or watch.poll() is not None:
                    watch = subprocess.Popen(["kubectl", "--request-timeout=0", "get", "pod", "-n", namespace, runid,
                        "--watch", "-o", "custom-columns=NAME:.metadata.name,UID:.metadata.uid,PHASE:.status.phase,NODE:.spec.nodeName",
                        "--no-headers"], stdout=out, stderr=subprocess.STDOUT)
                try:
                    watch.wait(timeout=30)
                    if watch.returncode is not None: time.sleep(5)
                except subprocess.TimeoutExpired: pass
            else:
                state = "DEADLINE_EXCEEDED"
                if baseline.cleanup_allowed(ledger, runid, namespace, runid, uid):
                    options = json.dumps({"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid}})
                    cancelled = pilot.call(["kubectl", "--request-timeout=20s", "delete", "--raw",
                        f"/api/v1/namespaces/{namespace}/pods/{runid}", "-f", "-"], input=options)
                    (output / "deadline-cancel.log").write_text(cancelled.stdout + cancelled.stderr)
    finally:
        if watch and watch.poll() is None:
            watch.terminate()
            try: watch.wait(timeout=5)
            except subprocess.TimeoutExpired: watch.kill(); watch.wait()
        log_text = ""
        for name, operation in {
            "driver.log": lambda: pilot.kube(["logs", "-n", namespace, runid], timeout=30),
            "describe.txt": lambda: pilot.kube(["describe", "pod", "-n", namespace, runid]),
        }.items():
            try:
                result = operation()
                (output / name).write_text(pilot.redact(result.stdout + result.stderr, secrets))
                if name == "driver.log" and result.returncode == 0: log_text = result.stdout
                if result.returncode != 0: errors[name] = "nonzero_exit"
            except Exception as exc: errors[name] = type(exc).__name__
        try: pilot.record_pods_and_events(output, namespace, runid, uid, secrets)
        except Exception as exc: errors["pod_events"] = type(exc).__name__
        try:
            event_result = pilot.call([hdfs, "dfs", "-get", evidence + "/spark-events", str(output / "spark-events")], timeout=90)
            if event_result.returncode != 0: errors["eventlog"] = "nonzero_exit"
        except Exception as exc: errors["eventlog"] = type(exc).__name__
        markers = [line for line in log_text.splitlines() if line.startswith("TRUTH3271 ")]
        (output / "markers.jsonl").write_text("\n".join(markers) + "\n")
        generation_passed = state == "SUCCEEDED" and any(line.startswith("TRUTH3271 done ") and '"ok":true' in line for line in markers)
        if generation_passed:
            try:
                fetched = pilot.call([hdfs, "dfs", "-get", truth_output, str(output / "truth")], timeout=120)
                baseline.require(fetched.returncode == 0, "truth download failed")
                assembled = pilot.call([sys.executable, str(HERE / "truth.py"), "assemble", "--contract", str(args.contract),
                    "--directory", str(output / "truth"), "--block-size", "10", "--output", str(output / "truth.json")], timeout=60)
                (output / "assemble.log").write_text(assembled.stdout + assembled.stderr)
                baseline.require(assembled.returncode == 0, "truth assembly refused")
                passed = True
            except Exception as exc: errors["assembly"] = type(exc).__name__
        (output / "verdict.json").write_text(json.dumps({"state": state, "ok": passed, "pod": runid, "uid": uid,
            "truth": truth_output, "generation_passed": generation_passed, "evidence_errors": errors,
            "elapsed_seconds": time.time() - started, "cleanup": "retained evidence; no pre-delete"}, indent=2))
        print(json.dumps({"status": state, "ok": passed, "output": str(output), "markers": len(markers)}), flush=True)
    baseline.require(passed, "truth generation failed: inspect evidence")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Never print command arguments: Spark conf can contain short-lived storage credentials.
        print("TRUTH_SUBMIT_FAILED:", type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
