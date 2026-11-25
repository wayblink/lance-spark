#!/usr/bin/env python3
"""Submit the explicitly approved correctness pilot; create-only unique run, UID-fenced evidence."""
import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import baseline


def call(args, timeout=30, **kw):
    return subprocess.run(args, timeout=timeout, capture_output=True, text=True, **kw)


def kube(args, timeout=30):
    return call(["kubectl", "--request-timeout=20s"] + args, timeout)


def get_pod(namespace, name):
    p = kube(["get", "pod", "-n", namespace, name, "-o", "json"])
    if p.returncode == 0:
        return json.loads(p.stdout), True
    return None, "NotFound" in p.stderr or "not found" in p.stderr


def redact(text, secrets=()):
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def record_pods_and_events(output, ns, run_id, uid, secrets):
    pods = kube(["get", "pods", "-n", ns, "-o", "json"])
    if pods.returncode == 0:
        items = json.loads(pods.stdout).get("items", [])
        related = [p for p in items if p["metadata"]["name"] == run_id or
                   any(o.get("uid") == uid for o in p["metadata"].get("ownerReferences", []))]
        (output / "pods-after.json").write_text(redact(json.dumps(related, indent=2), secrets))
    if uid:
        events = kube(["get", "events", "-n", ns, "--field-selector", "involvedObject.uid="+uid, "-o", "json"])
        (output / "kube-events.json").write_text(redact(events.stdout+events.stderr, secrets))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--approved", action="store_true")
    p.add_argument("--contract", type=Path, default=HERE / "baseline-10m.json")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--driver-node", help="correctness pilot only; record any explicit scheduling override")
    a = p.parse_args()
    if not a.approved:
        p.error("pilot requires explicit --approved after lead authorization")
    c = json.loads(a.contract.read_text()); baseline.validate_contract(c)
    a.output.mkdir(parents=True, exist_ok=False)
    runid = "b3255-pilot-" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
    ns = c["cluster"]["namespace"]; podname = runid
    image = c["image"] + "@" + c["image_digest"]
    start = time.time(); deadline = start + 1800
    (a.output / "contract.json").write_text(a.contract.read_text())
    (a.output / "run.json").write_text(json.dumps({"run_id":runid,"started_ms":int(start*1000),"correctness_only":True,"image":image},indent=2))
    existing, ok = get_pod(ns, podname)
    baseline.require(ok and existing is None, "refuse existing pod or unavailable API")
    snapshot = kube(["get", "pods", "-n", ns, "-o", "json"])
    (a.output / "pods-before.json").write_text(snapshot.stdout)
    baseline.require(snapshot.returncode == 0, "cannot record initial cluster snapshot")
    source = HERE / "BaselinePilot.java"
    build = a.output / "build"; build.mkdir(); (build / "classes").mkdir()
    compile_result = call(["docker","run","--rm","--user","0","--entrypoint","/bin/bash",
                          "-v",str(source)+":/src/BaselinePilot.java:ro","-v",str(build)+":/run",image,"-lc",
                          'set -e; "$JAVA_HOME/bin/javac" -cp "/opt/spark/jars/*" -d /run/classes /src/BaselinePilot.java; "$JAVA_HOME/bin/jar" cf /run/BaselinePilot.jar -C /run/classes .'],timeout=300)
    (build / "compile.log").write_text(compile_result.stdout + compile_result.stderr)
    baseline.require(compile_result.returncode == 0, "pilot compile failed; see compile.log")
    defaults = Path(os.environ.get("SPARK_DEFAULTS", "/opt/apache-kyuubi-1.11.0-bin/conf/spark-defaults.conf"))
    platform = {}
    for line in defaults.read_text().splitlines():
        if line.startswith("spark.hadoop.fs.s3a.") and "=" in line:
            k,v=line.split("=",1);platform[k]=v
    key = os.environ.get("AWS_ACCESS_KEY_ID",platform.get("spark.hadoop.fs.s3a.access.key"))
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY",platform.get("spark.hadoop.fs.s3a.secret.key"))
    endpoint = os.environ.get("AWS_ENDPOINT_URL",platform.get("spark.hadoop.fs.s3a.endpoint"))
    baseline.require(key and secret and endpoint, "object store configuration missing")
    secrets = (key, secret)
    conf = baseline.spark_conf(c)
    if a.driver_node:
        conf["spark.kubernetes.driver.node.selector.kubernetes.io/hostname"] = a.driver_node
    evidence = c["cluster"]["evidence_root"] + "/" + runid
    conf.update({"spark.eventLog.dir":evidence+"/spark-events", "spark.kubernetes.driver.pod.name":podname,
                 "spark.kubernetes.driver.deleteOnTermination":"false","spark.kubernetes.submission.waitAppCompletion":"false",
                 "spark.driver.maxResultSize":"256m", "spark.sql.catalog.lance.storage.endpoint":endpoint,
                 "spark.sql.catalog.lance.storage.access_key_id":key,"spark.sql.catalog.lance.storage.secret_access_key":secret,
                 "spark.sql.catalog.lance.storage.region":"us-east-1","spark.sql.catalog.lance.storage.aws_allow_http":"true",
                 "spark.sql.catalog.lance.storage.virtual_hosted_style_request":"false"})
    for scope in ["spark.kubernetes.driverEnv.","spark.executorEnv."]:
        for env,value in {"AWS_ACCESS_KEY_ID":key,"AWS_SECRET_ACCESS_KEY":secret,"AWS_ENDPOINT_URL":endpoint,
                          "AWS_ENDPOINT":endpoint,"AWS_REGION":"us-east-1","AWS_ALLOW_HTTP":"true"}.items():
            conf[scope+env]=value
    hdfs=os.environ.get("HADOOP_HOME","/opt/hadoop-3.3.1")+"/bin/hdfs"
    mkdir = call([hdfs,"dfs","-mkdir","-p",evidence+"/spark-events"],timeout=45)
    baseline.require(mkdir.returncode == 0, "HDFS event directory creation failed")
    command = [os.environ.get("SPARK_HOME","/opt/spark")+"/bin/spark-submit", "--master",c["cluster"]["master"],
               "--deploy-mode","cluster","--name",runid,"--class","BaselinePilot"]
    for k,v in conf.items(): command += ["--conf",k+"="+v]
    command += [str(build/"BaselinePilot.jar"),c["query_part"]]
    safe = {k:("<redacted>" if any(x in k.lower() for x in ["secret","access_key"]) else v) for k,v in conf.items()}
    (a.output / "spark-conf-redacted.json").write_text(json.dumps(safe,indent=2))
    try:
        submitted = call(command, timeout=c["timeouts"]["submit_seconds"])
    except subprocess.TimeoutExpired as exc:
        (a.output/"submit.log").write_text(redact(str(exc.stdout or "")+str(exc.stderr or ""), secrets))
        (a.output/"verdict.json").write_text(json.dumps({"state":"SUBMIT_TIMEOUT","run_id":runid,"pod":podname}))
        raise
    (a.output / "submit.log").write_text(redact(submitted.stdout+submitted.stderr, secrets))
    ledger=a.output/"created.json"; uid=None; state="UNKNOWN"; watch_handle=None
    try:
        baseline.require(submitted.returncode == 0,"spark-submit failed")
        while time.time() < deadline:
            pod,api_ok=get_pod(ns,podname)
            if pod:
                uid=pod["metadata"]["uid"];baseline.record_created(ledger,runid,ns,podname,uid)
                break
            if not api_ok: raise ValueError("API unavailable while finding created pod")
            time.sleep(2)
        baseline.require(uid is not None,"created driver never appeared")
        with (a.output/"pod-watch.jsonl").open("w") as out:
            watch_handle=subprocess.Popen(["kubectl","--request-timeout=0","get","pod","-n",ns,podname,"--watch","-o","json"],stdout=out,stderr=subprocess.STDOUT)
            while time.time()<deadline:
                pod,api_ok=get_pod(ns,podname);state=baseline.pod_state(pod,api_ok)
                if pod:
                    baseline.require(pod["metadata"]["uid"]==uid,"pod name was reused; do not act on replacement")
                    (a.output/"pod-latest.json").write_text(json.dumps(pod,indent=2))
                if state in ["SUCCEEDED","FAILED","POD_GONE"]:break
                # kubectl watch, not a fast busy loop; reconcile after a bounded watch interval.
                if watch_handle.poll() is not None:
                    watch_handle = subprocess.Popen(["kubectl", "--request-timeout=0", "get", "pod", "-n", ns,
                                                     podname, "--watch", "-o", "json"], stdout=out, stderr=subprocess.STDOUT)
                try:
                    watch_handle.wait(timeout=30)
                    if watch_handle.returncode is not None:
                        time.sleep(5)  # bounded reconnect backoff, never a busy loop on a dead watch
                except subprocess.TimeoutExpired:
                    pass
            else:
                state="DEADLINE_EXCEEDED"
                if baseline.cleanup_allowed(ledger,runid,ns,podname,uid):
                    options=json.dumps({"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":uid}})
                    cancelled=call(["kubectl","--request-timeout=20s","delete","--raw",
                                    f"/api/v1/namespaces/{ns}/pods/{podname}","-f","-"],input=options)
                    (a.output/"deadline-cancel.log").write_text(cancelled.stdout+cancelled.stderr)
    finally:
        if watch_handle and watch_handle.poll() is None:
            watch_handle.terminate()
            try:watch_handle.wait(timeout=5)
            except subprocess.TimeoutExpired:watch_handle.kill();watch_handle.wait()
        logs=kube(["logs","-n",ns,podname],timeout=30)
        (a.output/"driver.log").write_text(redact(logs.stdout+logs.stderr, secrets))
        describe=kube(["describe","pod","-n",ns,podname])
        (a.output/"describe.txt").write_text(redact(describe.stdout+describe.stderr, secrets))
        record_pods_and_events(a.output, ns, runid, uid, secrets)
        call([hdfs,"dfs","-get",evidence+"/spark-events",str(a.output/"spark-events")],timeout=60)
        markers=[line for line in logs.stdout.splitlines() if line.startswith("BASEPILOT ")]
        (a.output/"markers.jsonl").write_text("\n".join(markers)+"\n")
        (a.output/"verdict.json").write_text(json.dumps({"state":state,"pod":podname,"uid":uid,"run_id":runid,
                                                     "evidence":evidence,"elapsed_seconds":time.time()-start,
                                                     "cleanup":"not performed; evidence and UID ledger retained"},indent=2))
        print(json.dumps({"output":str(a.output),"state":state,"pod":podname,"uid":uid,"markers":len(markers)}))
    baseline.require(state=="SUCCEEDED" and any(x.startswith("BASEPILOT done ") and '"ok":true' in x for x in markers),"pilot did not pass; inspect evidence")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Compilation/submission can fail before the pod lifecycle finally block exists.
        if "--output" in sys.argv:
            output = Path(sys.argv[sys.argv.index("--output")+1])
            if output.is_dir() and not (output/"verdict.json").exists():
                (output/"verdict.json").write_text(json.dumps({"state":"PRE_SUBMIT_FAILURE", "exception_type":type(exc).__name__}))
        print("PILOT_FAILED:", type(exc).__name__, file=sys.stderr)
        sys.exit(1)
