#!/usr/bin/env python3
"""Reviewed formal runner: one unique matrix, five sequential cells, no implicit cluster work.

Window policy (user decision 2026-11-25T16:40+08:00): a cell is accepted when every measured
sample has bounded, API-healthy polling evidence around it, the job finished SUCCEEDED and its own
BASELINE_DONE marker is in the driver log. A foreign benchmark/unknown pod that overlaps the
sample window is recorded in ``foreign_observed``/``contamination`` and only clears
``exclusive_window_valid``; it never voids the cell and never stops the matrix. Nothing here
deletes, stops, patches or reschedules another team's job.
"""
import datetime
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid

import baseline
import pilot

POLL_SECONDS = 20
# Maximum tolerated distance from any measured instant to the nearest window observation.
COVERAGE_SECONDS = POLL_SECONDS + 15
# Bounded foreign summary kept on the verdict; ``foreign_observed_total`` carries the real count.
FOREIGN_SUMMARY_LIMIT = 20

# Driver UIDs whose own evidence proves SparkContext stopped and no executor remains.
# This is an explicit, UID-fenced allowlist; a replacement pod or any executor revokes it.
VERIFIED_STOPPED_DRIVERS = {}

# Names/arguments used by long-lived Spark services must not block a benchmark window.
BACKGROUND_TOKENS = (
    "kyuubi", "thrift", "thriftserver", "connect", "spark-connect-server",
    "long-lived", "long_lived",
)
# These are evidence tokens, not pod-name prefixes: driver evidence classifies its app's executors too.
BENCHMARK_TOKENS = (
    "tpcds", "tpc", "fmt", "format", "bench", "topk", "oss", "pure", "lance-iceberg",
)

# Object-store settings every driver and executor needs; values come from the platform at run time
# and are redacted by ``placeholder_storage`` when a dry-run prints a cell command.
STORAGE_ENV_ROLES = {"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret",
                     "AWS_ENDPOINT_URL": "endpoint", "AWS_ENDPOINT": "endpoint",
                     "AWS_REGION": "region", "AWS_ALLOW_HTTP": "allow_http"}
STORAGE_ENV_SCOPES = ("spark.kubernetes.driverEnv.", "spark.executorEnv.")
STORAGE_LITERALS = {"spark.sql.catalog.lance.storage.region": "us-east-1",
                    "spark.sql.catalog.lance.storage.aws_allow_http": "true",
                    "spark.sql.catalog.lance.storage.virtual_hosted_style_request": "false"}


def _pod_text(pod):
    """Return bounded identifying text from labels, names and container args."""
    meta = pod.get("metadata", {})
    labels = meta.get("labels", {})
    annotations = meta.get("annotations", {})
    spec = pod.get("spec", {})
    parts = [meta.get("name", ""), *[str(v) for v in labels.values()], *[str(v) for v in annotations.values()]]
    for container in spec.get("containers", []) or []:
        parts.extend(str(v) for v in (container.get("name"), container.get("command", []), container.get("args", [])))
        for env in container.get("env", []) or []:
            parts.append(str(env.get("name", "")))
    return " ".join(parts).lower()[:12000]


def activity_details(pods, own_app=None, own_name=None):
    """Classify live Spark pods as benchmark, background, or unknown by app evidence."""
    live = []
    app_evidence = {}
    for pod in pods:
        meta, status = pod.get("metadata", {}), pod.get("status", {})
        if status.get("phase") in ("Succeeded", "Failed", "Error"):
            continue
        labels = meta.get("labels", {})
        role = labels.get("spark-role")
        app = labels.get("spark-app-selector")
        if role not in ("driver", "executor") and not app:
            continue
        text = _pod_text(pod)
        key = app or f"pod:{meta.get('uid') or meta.get('name')}"
        category = (
            "stopped"
            if role == "driver"
            and VERIFIED_STOPPED_DRIVERS.get(meta.get("uid"), {}).get("app") == app
            else None
        )
        app_evidence.setdefault(key, []).append(text)
        live.append((pod, role, app, key, category))

    app_category = {}
    for key, texts in app_evidence.items():
        joined = " ".join(texts)
        if any(token in joined for token in BACKGROUND_TOKENS):
            app_category[key] = "background"
        elif any(token in joined for token in BENCHMARK_TOKENS):
            app_category[key] = "benchmark"
        else:
            app_category[key] = "unknown"

    revoked_apps = {
        app
        for pod, role, app, key, verified_category in live
        if role == "executor"
        and app
        and any(
            p_role == "driver"
            and p_app == app
            and verified == "stopped"
            for _pod, p_role, p_app, _key, verified in live
        )
    }
    out = []
    for pod, role, app, key, verified_category in live:
        meta, status = pod.get("metadata", {}), pod.get("status", {})
        if (own_app and app == own_app) or meta.get("name") == own_name:
            continue
        category = verified_category or app_category[key]
        # Any live executor under an allowlisted old application invalidates its exemption.
        if app in revoked_apps:
            category = "benchmark"
        out.append({"uid": meta.get("uid"), "name": meta.get("name"), "app": app,
                    "role": role, "phase": status.get("phase"),
                    "node": pod.get("spec", {}).get("nodeName"),
                    "category": category})
    return out


def activity(pods, own_app=None, own_name=None):
    """Return active benchmark/unknown pods; background services only affect evidence."""
    return [item for item in activity_details(pods, own_app, own_name)
            if item["category"] in ("benchmark", "unknown")]


def schedule():
    # Freeze this setting for the whole matrix; executors retain normal scheduler placement.
    return {"spark.kubernetes.driver.node.selector.kubernetes.io/hostname": "cpu-1122.host.platform.shaipower.com"}


def delete_options(uid):
    return {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid}}


def measured(samples):
    return [sample for sample in samples if not sample.get("warmup")]


def relevant(observations, start, end):
    """Observations that can attest to one sample.

    One tolerance governs both "which observations are relevant" and "how large a hole is
    acceptable"; a tighter filter than the tolerance would reject clean windows whenever a short
    sample fits between two polls.
    """
    tolerance = COVERAGE_SECONDS * 1000
    return [o for o in observations if start - tolerance <= o["at_ms"] <= end + tolerance]


def coverage_ok(samples, observations):
    """Strict evidence gate: API-healthy polls before and after every measured sample.

    Foreign activity is deliberately NOT part of this gate; it is reported by
    ``foreign_summary`` and never relaxes the coverage rule below.
    """
    samples = measured(samples)
    if not samples or not observations:
        return False
    tolerance = COVERAGE_SECONDS * 1000
    for sample in samples:
        start, end = sample["started_ms"], sample["started_ms"] + sample["ms"]
        covering = relevant(observations, start, end)
        if not covering or any(not o["api_ok"] for o in covering):
            return False
        if not any(o["at_ms"] <= start for o in covering):
            return False
        if not any(o["at_ms"] >= end for o in covering):
            return False
        times = sorted(o["at_ms"] for o in covering)
        if max([times[0]-start, end-times[-1]]+[b-a for a,b in zip(times,times[1:])]) > tolerance:
            return False
    return True


def foreign_summary(samples, observations):
    """Foreign benchmark/unknown pods seen inside a measured sample's window.

    Deduplicated per pod UID (falls back to app/name), ordered by first sighting and capped at
    ``FOREIGN_SUMMARY_LIMIT``; ``total`` keeps the true count so truncation is never silent.
    """
    seen = {}
    for sample in measured(samples):
        start, end = sample["started_ms"], sample["started_ms"] + sample["ms"]
        for observed in relevant(observations, start, end):
            for pod in observed.get("foreign") or []:
                key = pod.get("uid") or (pod.get("app"), pod.get("name"))
                entry = seen.get(key)
                if entry is None:
                    seen[key] = {"app":pod.get("app"), "name":pod.get("name"), "role":pod.get("role"),
                                 "node":pod.get("node"), "category":pod.get("category"),
                                 "first_at_ms":observed["at_ms"], "last_at_ms":observed["at_ms"]}
                else:
                    entry["first_at_ms"] = min(entry["first_at_ms"], observed["at_ms"])
                    entry["last_at_ms"] = max(entry["last_at_ms"], observed["at_ms"])
    ordered = sorted(seen.values(), key=lambda item: item["first_at_ms"])
    return {"observed": ordered[:FOREIGN_SUMMARY_LIMIT], "total": len(ordered)}


def api_unhealthy(samples, observations):
    """True when a poll covering a measured sample reported api_ok=false (not a coverage hole)."""
    for sample in measured(samples):
        start, end = sample["started_ms"], sample["started_ms"] + sample["ms"]
        if any(not o["api_ok"] for o in relevant(observations, start, end)):
            return True
    return False


def cell_window(samples, observations, state, done_marker_present, api_unknown=False):
    """The single decision point for one cell.

    usable             = SUCCEEDED and BASELINE_DONE and coverage_ok and not api_unknown
    exclusive_window_  = usable and no foreign benchmark/unknown overlap (the old "clean" bit)
    contamination      = api_failure / foreign_benchmark / None; a coverage hole alone is
                         neither, it is simply not usable
    """
    foreign = foreign_summary(samples, observations)
    coverage = coverage_ok(samples, observations)
    api_failure = api_unknown or api_unhealthy(samples, observations)
    usable = bool(state == "SUCCEEDED" and done_marker_present and coverage and not api_unknown)
    clean = usable and not foreign["observed"]
    return {"coverage_ok": coverage, "window_usable": usable,
            "exclusive_window_valid": clean, "baseline_valid": clean,
            "foreign_observed": foreign["observed"],
            "foreign_observed_total": foreign["total"],
            "contamination": ("api_failure" if api_failure
                              else "foreign_benchmark" if foreign["observed"] else None)}


def window_valid(samples, observations):
    """Legacy meaning kept for reports/tests: strict coverage and no foreign overlap at all."""
    return coverage_ok(samples, observations) and not foreign_summary(samples, observations)["observed"]


def snapshot(ns, own_app=None, own_name=None):
    try:
        p = pilot.kube(["get", "pods", "-n", ns, "-o", "json"])
        ok = p.returncode == 0
        items = json.loads(p.stdout).get("items", []) if ok else []
    except (subprocess.TimeoutExpired, ValueError):
        ok, items = False, []
    details = activity_details(items, own_app, own_name)
    return {"at_ms":int(time.time()*1000), "api_ok":ok,
            "foreign":[x for x in details if x["category"] in ("benchmark", "unknown")],
            "background":[x for x in details if x["category"] == "background"],
            "classification": {category: sum(x["category"] == category for x in details)
                               for category in ("benchmark", "background", "unknown")},
            "owned":[{"uid":x["metadata"]["uid"], "name":x["metadata"]["name"],
                       "app":x["metadata"].get("labels", {}).get("spark-app-selector"),
                       "phase":x.get("status", {}).get("phase"), "node":x.get("spec", {}).get("nodeName"),
                       "role":x["metadata"].get("labels", {}).get("spark-role")}
                      for x in items if (own_app and x["metadata"].get("labels", {}).get("spark-app-selector")==own_app)
                      or x["metadata"]["name"]==own_name]}


def wait_window(ns, seconds, evidence):
    """Poll for a clean window; when it stays busy, record the exhaustion and continue.

    Foreign jobs are only observed here: nothing is deleted, stopped, patched or rescheduled.
    Every poll is appended to ``evidence`` so an exhausted wait still has proof.
    """
    deadline=time.monotonic()+seconds
    polls=0
    while True:
        observed=snapshot(ns)
        polls+=1
        with evidence.open("a") as out:out.write(json.dumps(observed)+"\n")
        if observed["api_ok"] and not observed["foreign"]:
            return {"exhausted":False,"polls":polls}
        if time.monotonic()>=deadline:
            marker={"at_ms":int(time.time()*1000),"api_ok":observed["api_ok"],
                    "window_wait_exhausted":True,"foreign_count":len(observed["foreign"])}
            with evidence.open("a") as out:out.write(json.dumps(marker)+"\n")
            return {"exhausted":True,"polls":polls}
        threading.Event().wait(POLL_SECONDS)


def store(output, name, data, secrets):
    if not isinstance(data, str):data=json.dumps(pilot.redact_json(data), indent=2)
    (output/name).write_text(pilot.redact(data, secrets))


def validate_expected_cell(records, kind, k, eventlog, text_contract=None):
    baseline.require(records and all(x.get("kind")==kind and x.get("k")==k for x in records), "samples belong to a different cell")
    if kind=="text":
        baseline.require(text_contract is not None,"text cell has no frozen contract")
        expected=baseline.text_expected_samples(text_contract)
    else:
        expected=40 if kind=="single" else 4
    baseline.require(len(records)==expected,"wrong cell sample/warmup count")
    return baseline.validate_run(records,eventlog)


def capture(name, operation, errors):
    try:
        result=operation()
        if hasattr(result,"returncode") and result.returncode!=0:
            errors[name]="exit_"+str(result.returncode)
        return result
    except Exception as exc:
        errors[name]=type(exc).__name__
        return None


def write_verdict(output, verdict, errors, secrets):
    verdict["evidence_errors"]=errors
    if errors:
        # Unfaithful evidence can never be silently accepted, whatever the window looked like.
        for key in ("window_usable","exclusive_window_valid","baseline_valid"):verdict[key]=False
    store(output,"verdict.json",verdict,secrets)


def cell_name(verdict):
    return "%s-k%d" % (verdict["kind"], verdict["k"])


def platform_config():
    p=Path(os.environ.get("SPARK_DEFAULTS", "/opt/apache-kyuubi-1.11.0-bin/conf/spark-defaults.conf"))
    values={}
    for line in p.read_text().splitlines():
        if line.startswith("spark.hadoop.fs.s3a.") and "=" in line:
            k,v=line.split("=",1);values[k]=v
    key=os.environ.get("AWS_ACCESS_KEY_ID",values.get("spark.hadoop.fs.s3a.access.key"))
    secret=os.environ.get("AWS_SECRET_ACCESS_KEY",values.get("spark.hadoop.fs.s3a.secret.key"))
    endpoint=os.environ.get("AWS_ENDPOINT_URL",values.get("spark.hadoop.fs.s3a.endpoint"))
    baseline.require(key and secret and endpoint, "object store configuration missing")
    conf={"spark.sql.catalog.lance.storage.endpoint":endpoint,
          "spark.sql.catalog.lance.storage.access_key_id":key,
          "spark.sql.catalog.lance.storage.secret_access_key":secret}
    conf.update(STORAGE_LITERALS)
    roles={"key":key,"secret":secret,"endpoint":endpoint,"region":"us-east-1","allow_http":"true"}
    for scope in STORAGE_ENV_SCOPES:
        for name,role in STORAGE_ENV_ROLES.items():
            conf[scope+name]=roles[role]
    return conf,(key,secret)


def placeholder_storage():
    """Exactly the platform_config keys with redacted values; dry-run only, never submits."""
    conf={"spark.sql.catalog.lance.storage.endpoint":"<object-store endpoint>",
          "spark.sql.catalog.lance.storage.access_key_id":"<redacted>",
          "spark.sql.catalog.lance.storage.secret_access_key":"<redacted>"}
    conf.update(STORAGE_LITERALS)
    for scope in STORAGE_ENV_SCOPES:
        for name in STORAGE_ENV_ROLES:
            conf[scope+name]="<redacted>"
    return conf


def _cell_conf(c, name, remote, storage):
    """Shared driver/executor conf for one cell; identical for the five vector cells and text."""
    conf=baseline.spark_conf(c);conf.update(schedule());conf.update(storage)
    conf.update({"spark.kubernetes.driver.pod.name":name,"spark.kubernetes.driver.deleteOnTermination":"false",
                 "spark.kubernetes.submission.waitAppCompletion":"false","spark.eventLog.dir":remote+"/spark-events",
                 "spark.driver.maxResultSize":"1g"})
    return conf


def _submit_command(c, conf, jar, payload, name):
    command=[os.environ.get("SPARK_HOME","/opt/spark")+"/bin/spark-submit","--master",c["cluster"]["master"],
             "--deploy-mode","cluster","--name",name,"--class","BaselineBenchmark"]
    for key,value in conf.items():command += ["--conf",key+"="+value]
    command += [str(jar)]+[str(item) for item in payload]
    return command


def cell_command(c, kind, k, jar, remote_contract, remote_truth, remote, name, storage):
    """Build the reviewed spark-submit invocation for one vector cell (pure: no cluster access)."""
    conf=_cell_conf(c,name,remote,storage)
    return conf,_submit_command(c,conf,jar,[remote_contract,c["query_part"],remote_truth,
                                            remote+"/samples.jsonl",kind,str(k)],name)


def text_cell_command(c, text, jar, remote, name, storage):
    """The optional sixth cell: same conf/scheduling, only the JVM payload differs."""
    conf=_cell_conf(c,name,remote,storage)
    return conf,_submit_command(c,conf,jar,[text["remote_contract"],text["remote_asset"],
                                            remote+"/samples.jsonl","text",str(text["k"])],name)


def text_cell_plan(c, text_contract_path, remote_root="<hdfs-run-root>"):
    """Pure dry-run plan for the text cell: validates frozen bytes, prints a redacted command.

    No ssh, kubectl, docker or subprocess: the only work is reading two local files and building
    the same spark-submit argv ``run_one_cell`` would use, with storage values placeheld.
    """
    text_contract_path=Path(text_contract_path).resolve()
    baseline.require(text_contract_path.is_file(),"text contract not found: "+str(text_contract_path))
    text_contract=json.loads(text_contract_path.read_text());baseline.validate_text_contract(text_contract)
    rows=baseline.load_text_asset(text_contract,text_contract_path)
    text={"contract":text_contract,"k":text_contract["k"],"queries":[row["query"] for row in rows],
          "queries_sha256":text_contract["queries_sha256"],
          "remote_contract":remote_root+"/text-contract.json","remote_asset":remote_root+"/text-queries.jsonl"}
    conf,command=text_cell_command(c,text,"<compiled>/BaselineBenchmark.jar",remote_root+"/text-k10",
                                   "b<card>-text-k10-<run>",placeholder_storage())
    return {"kind":"text","k":text_contract["k"],"class":"BaselineBenchmark","cell_dir":"text-k10",
            "queries":len(rows),"queries_sha256":text_contract["queries_sha256"],
            "contract_sha256":baseline.sha256(text_contract_path),
            "warmup_passes":text_contract["warmup_passes"],"repeats":text_contract["repeats"],
            "samples":baseline.text_expected_samples(text_contract),
            "result_contract":"doc_uuid,_score; ORDER BY `_score` DESC LIMIT 10",
            "sql_template":text_contract["sql_template"],
            "sql_examples":[baseline.text_sql(text_contract,rows[0]["query"]),
                            baseline.text_sql(text_contract,rows[-1]["query"])],
            "spark_conf":conf,"spark_submit_command":command}


def cancel_own(ns, name, uid, ledger, run_id, output, secrets):
    pod,ok=pilot.get_pod(ns,name)
    if ok and pod and pod["metadata"]["uid"]==uid and baseline.cleanup_allowed(ledger,run_id,ns,name,uid):
        p=pilot.call(["kubectl","--request-timeout=20s","delete","--raw",f"/api/v1/namespaces/{ns}/pods/{name}","-f","-"],
                     input=json.dumps(delete_options(uid)))
        store(output,"cancel.json",{"code":p.returncode,"output":p.stdout+p.stderr},secrets)


def prepare_matrix(c, contract_path, queries_path, truth_path, output, stopped_evidence=None,
                   text_contract_path=None):
    """Validate frozen assets, fence the UID ledger, compile the JVM source, create the run root.

    ``text_contract_path`` enables the optional sixth cell: its frozen contract and query asset
    are validated by sha256 here (and again by the JVM), copied into the run root for later
    read-only reporting, and uploaded create-only next to contract.json/truth.json.

    Returns the context ``run_one_cell`` needs. ``pair-run.py`` calls this once per (arm, round)
    so every replicate keeps its own manifest, compiled jar and HDFS evidence root.
    """
    os.umask(0o077)
    VERIFIED_STOPPED_DRIVERS.clear()
    if stopped_evidence is not None:
        VERIFIED_STOPPED_DRIVERS.update(load_stopped_evidence(stopped_evidence))
    baseline.validate_contract(c);baseline.validate_asset_files(c,queries_path,truth_path)
    baseline.require(baseline.query_float32_sha256([json.loads(l) for l in Path(queries_path).read_text().splitlines() if l.strip()])==c["query_float32_sha256"],"frozen queries changed")
    text=None
    if text_contract_path is not None:
        text_contract_path=Path(text_contract_path).resolve()
        baseline.require(text_contract_path.is_file(),"text contract not found: "+str(text_contract_path))
        text_contract=json.loads(text_contract_path.read_text());baseline.validate_text_contract(text_contract)
        rows=baseline.load_text_asset(text_contract,text_contract_path)
        text={"contract":text_contract,"contract_path":text_contract_path,"queries":[row["query"] for row in rows],
              "k":text_contract["k"],"asset_path":text_contract_path.with_name(text_contract["queries_asset"]),
              "queries_sha256":text_contract["queries_sha256"]}
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=False)
    runid="matrix-"+datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")+"-"+uuid.uuid4().hex[:8]
    remote=c["cluster"]["evidence_root"]+"/"+runid
    storage,secrets=platform_config();hdfs=os.environ.get("HADOOP_HOME","/opt/hadoop-3.3.1")+"/bin/hdfs"
    manifest={"run_id":runid,"contract_sha256":baseline.sha256(contract_path),"truth_sha256":baseline.sha256(truth_path),
              "query_json_sha256":baseline.sha256(queries_path),"schedule":schedule(),"executor_schedule":"default, all nodes observed",
              "cells":baseline.cells(c),"remote":remote,"status":"STARTING","text_cell":text is not None,
              "window_policy":"record overlapping foreign benchmark/unknown activity and keep the cell; only coverage/API/marker failures void a cell",
              "verified_stopped_drivers":VERIFIED_STOPPED_DRIVERS}
    if text is not None:
        # Frozen text-cell identity: the report re-checks these bytes against the run root copies.
        text["remote_contract"]=remote+"/text-contract.json";text["remote_asset"]=remote+"/text-queries.jsonl"
        (output/"text-contract.json").write_text(text["contract_path"].read_text())
        (output/"text-queries.jsonl").write_text(text["asset_path"].read_text())
        manifest.update({"text_contract_sha256":baseline.sha256(text["contract_path"]),
                         "text_queries_sha256":text["queries_sha256"],
                         "text_cell_detail":{"kind":"text","k":text["k"],"queries":len(text["queries"]),
                                             "warmup_passes":text["contract"]["warmup_passes"],
                                             "repeats":text["contract"]["repeats"],
                                             "sql_template":text["contract"]["sql_template"]}})
    store(output,"manifest.json",manifest,secrets)
    src=Path(__file__).with_name("BaselineBenchmark.java");build=output/"build";build.mkdir();(build/"classes").mkdir()
    image=c["image"]+"@"+c["image_digest"]
    compiled=pilot.call(["docker","run","--rm","--user","0","--entrypoint","/bin/bash","-v",str(src)+":/src/BaselineBenchmark.java:ro",
                         "-v",str(build)+":/run",image,"-lc",'set -e; "$JAVA_HOME/bin/javac" -cp "/opt/spark/jars/*" -d /run/classes /src/BaselineBenchmark.java; "$JAVA_HOME/bin/jar" cf /run/BaselineBenchmark.jar -C /run/classes .'],timeout=300)
    store(build,"compile.log",compiled.stdout+compiled.stderr,secrets);baseline.require(compiled.returncode==0,"matrix source compile failed")
    p=pilot.call([hdfs,"dfs","-mkdir","-p",remote],timeout=45);baseline.require(p.returncode==0,"HDFS evidence creation failed")
    uploads=[(contract_path,"contract.json"),(truth_path,"truth.json")]
    if text is not None: uploads += [(text["contract_path"],"text-contract.json"),(text["asset_path"],"text-queries.jsonl")]
    for local,name in uploads:
        p=pilot.call([hdfs,"dfs","-put",str(local),remote+"/"+name],timeout=60);baseline.require(p.returncode==0,"create-only asset upload failed: "+name)
    return {"contract":c,"output":output,"runid":runid,"remote":remote,"storage":storage,
            "secrets":secrets,"hdfs":hdfs,"jar":build/"BaselineBenchmark.jar","text":text}


def run_one_cell(ctx, kind, k, window_seconds):
    """Run one cell through the reviewed lifecycle; return (records, verdict).

    A foreign benchmark in the window is recorded in the verdict and the cell stays usable; a
    coverage/API/marker failure makes it unusable, which stops the matrix after evidence is kept.
    """
    c=ctx["contract"];root=ctx["output"];remote_root=ctx["remote"]
    storage=ctx["storage"];secrets=ctx["secrets"];hdfs=ctx["hdfs"];jar=ctx["jar"]
    ns=c["cluster"]["namespace"]
    output=root/f"{kind}-k{k}";output.mkdir()
    window=wait_window(ns,window_seconds,output/"window-before.jsonl")
    name="b3255-"+kind+"-k"+str(k)+"-"+uuid.uuid4().hex[:12]
    pod,ok=pilot.get_pod(ns,name);baseline.require(ok and pod is None,"unique pod precondition failed")
    remote=remote_root+f"/{kind}-k{k}"
    if kind=="text":
        baseline.require(ctx.get("text") is not None,"text cell was not prepared with a frozen contract")
        conf,command=text_cell_command(c,ctx["text"],jar,remote,name,storage)
    else:
        conf,command=cell_command(c,kind,k,jar,remote_root+"/contract.json",remote_root+"/truth.json",
                                  remote,name,storage)
    store(output,"spark-conf.json",conf,secrets)
    p=pilot.call([hdfs,"dfs","-mkdir","-p",remote+"/spark-events"],timeout=45)
    baseline.require(p.returncode==0,"cannot create event directory")
    state="PRE_SUBMIT";uid=None;app=None;observations=[];stop=threading.Event();monitor=None
    ledger=output/"created.json";start=time.monotonic();records=[];api_unknown=False
    deadline=start+c["timeouts"]["cell_seconds"]
    def observe():
        while not stop.is_set():
            o=snapshot(ns,app,name);observations.append(o)
            with (output/"activity.jsonl").open("a") as log:log.write(json.dumps(o)+"\n")
            stop.wait(POLL_SECONDS)
    try:
        monitor=threading.Thread(target=observe,daemon=True);monitor.start()
        try:
            submitted=pilot.call(command,timeout=c["timeouts"]["submit_seconds"])
            store(output,"submit.log",submitted.stdout+submitted.stderr,secrets)
            state="SUBMITTED" if submitted.returncode==0 else "SUBMIT_FAILED"
        except subprocess.TimeoutExpired as exc:
            store(output,"submit.log",str(exc.stdout or "")+str(exc.stderr or ""),secrets);state="SUBMIT_TIMEOUT"
        # Even a submit timeout can create a driver. Discover it, fence UID, and preserve evidence.
        discovery=time.monotonic()+180
        while time.monotonic()<discovery:
            pod,ok=pilot.get_pod(ns,name)
            if pod:
                uid=pod["metadata"]["uid"];app=pod["metadata"].get("labels",{}).get("spark-app-selector")
                baseline.record_created(ledger,name,ns,name,uid);break
            if not ok:api_unknown=True
            if state in ("SUBMIT_FAILED","SUBMIT_TIMEOUT"):break
            stop.wait(5)
        baseline.require(uid is not None,"submitted driver not found; inspect submit evidence")
        if state in ("SUBMIT_FAILED","SUBMIT_TIMEOUT"):
            cancel_own(ns,name,uid,ledger,name,output,secrets)
        else:
            while time.monotonic()<deadline:
                pod,ok=pilot.get_pod(ns,name);state=baseline.pod_state(pod,ok)
                if not ok:api_unknown=True
                if pod:
                    baseline.require(pod["metadata"]["uid"]==uid,"pod name reused")
                    store(output,"pod-latest.json",pod,secrets)
                if state in ("SUCCEEDED","FAILED","POD_GONE"):break
                stop.wait(POLL_SECONDS)
            else:
                state="CELL_DEADLINE_EXCEEDED";cancel_own(ns,name,uid,ledger,name,output,secrets)
    finally:
        stop.set()
        if monitor:monitor.join(timeout=30)
        observations.append(snapshot(ns,app,name))
        store(output,"activity-final.json",observations[-1],secrets)
        errors={}
        logs=capture("logs",lambda:pilot.kube(["logs","-n",ns,name],timeout=45),errors)
        if logs is not None: store(output,"driver.log",logs.stdout+logs.stderr,secrets)
        d=capture("describe",lambda:pilot.kube(["describe","pod","-n",ns,name]),errors)
        if d is not None: store(output,"describe.txt",d.stdout+d.stderr,secrets)
        capture("pods_events",lambda:pilot.record_pods_and_events(output,ns,name,uid,secrets),errors)
        capture("eventlog_fetch",lambda:pilot.call([hdfs,"dfs","-get",remote+"/spark-events",str(output/"spark-events")],timeout=120),errors)
        fetched=capture("samples_fetch",lambda:pilot.call([hdfs,"dfs","-cat",remote+"/samples.jsonl"],timeout=60),errors)
        if fetched is not None and fetched.returncode==0:
            store(output,"samples.jsonl",fetched.stdout,secrets)
            records=[json.loads(line) for line in fetched.stdout.splitlines() if line.strip()]
        else: records=[]
        eventlog=any(p.is_file() for p in (output/"spark-events").rglob("*")) if (output/"spark-events").exists() else False
        expected_marker='BASELINE_DONE {"ok":true,"kind":"'+kind+'","k":'+str(k)+'}'
        marker_present=logs is not None and expected_marker in logs.stdout
        window_state=cell_window(records,observations,state=state,done_marker_present=marker_present,
                                 api_unknown=api_unknown)
        verdict={"kind":kind,"k":k,"pod":name,"uid":uid,"app":app,"state":state,
                 "done_marker_present":marker_present,"eventlog_present":eventlog,
                 "elapsed_seconds":time.monotonic()-start,"cache_residency":"unknown",
                 "window_wait_exhausted":window["exhausted"],
                 "cleanup":"own terminal pod retained as evidence"}
        verdict.update(window_state)
        try:
            validate_expected_cell(records,kind,k,eventlog,(ctx.get("text") or {}).get("contract"))
        except (ValueError,KeyError) as exc:
            # Samples that fail validation are never acceptable, whatever the window looked like.
            verdict["validation_error"]=str(exc)
            for key in ("window_usable","exclusive_window_valid","baseline_valid"):verdict[key]=False
        if not verdict["window_usable"]:
            verdict["window_action"]="cell not usable; explicit new-attempt rerun required, never silently accepted"
        elif not verdict["exclusive_window_valid"]:
            verdict["window_action"]="foreign activity recorded as contamination; accepted, never clean"
        write_verdict(output,verdict,errors,secrets)
    baseline.require(verdict["window_usable"],f"cell {kind} k{k} not usable; matrix stopped, preserve failure")
    return records,verdict


def load_stopped_evidence(path):
    """Load the UID-fenced evidence map for drivers proven to have stopped."""
    evidence=json.loads(Path(path).read_text())
    baseline.require(isinstance(evidence,dict),"stopped-driver evidence must be a UID map")
    for uid,record in evidence.items():
        baseline.require(uid and isinstance(record,dict) and record.get("app")
                         and record.get("spark_context_stopped") is True
                         and record.get("ui_connection_refused") is True
                         and record.get("executors_at_verification")==0
                         and record.get("verified_at_ms"),"incomplete stopped-driver evidence")
    return evidence


def run_matrix(c,contract_path,queries_path,truth_path,output,window_seconds=1800,stopped_evidence=None,
               text_cell=False,text_contract_path=None):
    """Run the five frozen vector cells, then the optional text cell, under one lifecycle.

    ``text_cell=False`` keeps the historical five-cell matrix byte-for-byte; enabling it only
    appends text-k10 after batch-k2000 and counts it in matrix-verdict.cells/samples.
    """
    if text_cell:
        baseline.require(text_contract_path is not None,"--text-cell requires a frozen text contract")
        ctx=prepare_matrix(c,contract_path,queries_path,truth_path,output,stopped_evidence,
                           text_contract_path=text_contract_path)
    else:
        ctx=prepare_matrix(c,contract_path,queries_path,truth_path,output,stopped_evidence)
    out=ctx["output"];secrets=ctx["secrets"];records=[];verdicts=[]
    cells=baseline.cells(c)+(baseline.text_cells() if text_cell else [])
    try:
        for kind,k in cells:
            cell_records,verdict=run_one_cell(ctx,kind,k,window_seconds)
            records += cell_records;verdicts.append(verdict)
        summary=baseline.validate_matrix(records,True,text=text_cell)
        usable=[verdict for verdict in verdicts if verdict.get("window_usable") is True]
        clean=[verdict for verdict in verdicts if verdict.get("exclusive_window_valid") is True]
        result={"complete":len(usable)==len(verdicts)==len(cells),
            "cells":len(cells),"cells_usable":len(usable),"cells_clean":len(clean),
            "contaminated_cells":[cell_name(v) for v in verdicts if v.get("contamination")],
            "window_wait_exhausted_cells":[cell_name(v) for v in verdicts if v.get("window_wait_exhausted") is True],
            "samples":summary["samples"],"cache_residency":"unknown","text_cell":text_cell}
        if text_cell:
            result["text_queries_sha256"]=ctx["text"]["queries_sha256"]
        store(out,"matrix-verdict.json",result,secrets)
    except Exception as exc:
        store(out,"matrix-verdict.json",{"complete":False,"failure_type":type(exc).__name__,
            "reason":str(exc),"accepted_samples":len(records),"cells_completed":len(verdicts),
            "cells_planned":len(cells),"text_cell":text_cell,
            "cells_usable":sum(v.get("window_usable") is True for v in verdicts),
            "cells_clean":sum(v.get("exclusive_window_valid") is True for v in verdicts),
            "contaminated_cells":[cell_name(v) for v in verdicts if v.get("contamination")],
            "window_wait_exhausted_cells":[cell_name(v) for v in verdicts if v.get("window_wait_exhausted") is True]},secrets)
        raise
