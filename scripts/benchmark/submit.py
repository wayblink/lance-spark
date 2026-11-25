#!/usr/bin/env python3
"""Reviewed formal runner: one unique matrix, five sequential cells, no implicit cluster work."""
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


def schedule():
    # Freeze this setting for the whole matrix; executors retain normal scheduler placement.
    return {"spark.kubernetes.driver.node.selector.kubernetes.io/hostname": "cpu-1122.host.platform.shaipower.com"}


def delete_options(uid):
    return {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid}}


def activity(pods, own_app=None, own_name=None):
    out = []
    for p in pods:
        meta, status = p.get("metadata", {}), p.get("status", {})
        labels = meta.get("labels", {})
        if status.get("phase") in ("Succeeded", "Failed", "Error"):
            continue
        role = labels.get("spark-role")
        app = labels.get("spark-app-selector")
        if role not in ("driver", "executor") and not app:
            continue
        if (own_app and app == own_app) or meta.get("name") == own_name:
            continue
        out.append({"uid":meta.get("uid"), "name":meta.get("name"), "app":app,
                    "role":role, "phase":status.get("phase"), "node":p.get("spec", {}).get("nodeName")})
    return out


def window_valid(samples, observations):
    samples = [x for x in samples if not x.get("warmup")]
    if not samples or not observations:
        return False
    for sample in samples:
        start, end = sample["started_ms"], sample["started_ms"] + sample["ms"]
        relevant = [o for o in observations if start-POLL_SECONDS*1000 <= o["at_ms"] <= end+POLL_SECONDS*1000]
        if not relevant or any(not o["api_ok"] or o["foreign"] for o in relevant):
            return False
        if not any(o["at_ms"] <= start and start-o["at_ms"] <= (POLL_SECONDS+15)*1000 for o in relevant):
            return False
        if not any(o["at_ms"] >= end and o["at_ms"]-end <= (POLL_SECONDS+15)*1000 for o in relevant):
            return False
        times = sorted(o["at_ms"] for o in relevant)
        if max([times[0]-start, end-times[-1]]+[b-a for a,b in zip(times,times[1:])]) > (POLL_SECONDS+15)*1000:
            return False
    return True


def snapshot(ns, own_app=None, own_name=None):
    try:
        p = pilot.kube(["get", "pods", "-n", ns, "-o", "json"])
        ok = p.returncode == 0
        items = json.loads(p.stdout).get("items", []) if ok else []
    except (subprocess.TimeoutExpired, ValueError):
        ok, items = False, []
    return {"at_ms":int(time.time()*1000), "api_ok":ok,
            "foreign":activity(items, own_app, own_name),
            "owned":[{"uid":x["metadata"]["uid"], "name":x["metadata"]["name"],
                       "app":x["metadata"].get("labels", {}).get("spark-app-selector"),
                       "phase":x.get("status", {}).get("phase"), "node":x.get("spec", {}).get("nodeName"),
                       "role":x["metadata"].get("labels", {}).get("spark-role")}
                      for x in items if (own_app and x["metadata"].get("labels", {}).get("spark-app-selector")==own_app)
                      or x["metadata"]["name"]==own_name]}


def wait_window(ns, seconds, evidence):
    deadline=time.monotonic()+seconds
    while True:
        observed=snapshot(ns)
        with evidence.open("a") as out:out.write(json.dumps(observed)+"\n")
        if observed["api_ok"] and not observed["foreign"]:
            return
        baseline.require(time.monotonic()<deadline, "exclusive window not ready or API unknown")
        threading.Event().wait(POLL_SECONDS)


def store(output, name, data, secrets):
    if not isinstance(data, str):data=json.dumps(pilot.redact_json(data), indent=2)
    (output/name).write_text(pilot.redact(data, secrets))


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
          "spark.sql.catalog.lance.storage.access_key_id":key,"spark.sql.catalog.lance.storage.secret_access_key":secret,
          "spark.sql.catalog.lance.storage.region":"us-east-1","spark.sql.catalog.lance.storage.aws_allow_http":"true",
          "spark.sql.catalog.lance.storage.virtual_hosted_style_request":"false"}
    for scope in ("spark.kubernetes.driverEnv.","spark.executorEnv."):
        for name,value in {"AWS_ACCESS_KEY_ID":key,"AWS_SECRET_ACCESS_KEY":secret,"AWS_ENDPOINT_URL":endpoint,
                           "AWS_ENDPOINT":endpoint,"AWS_REGION":"us-east-1","AWS_ALLOW_HTTP":"true"}.items():
            conf[scope+name]=value
    return conf,(key,secret)


def cancel_own(ns, name, uid, ledger, run_id, output, secrets):
    pod,ok=pilot.get_pod(ns,name)
    if ok and pod and pod["metadata"]["uid"]==uid and baseline.cleanup_allowed(ledger,run_id,ns,name,uid):
        p=pilot.call(["kubectl","--request-timeout=20s","delete","--raw",f"/api/v1/namespaces/{ns}/pods/{name}","-f","-"],
                     input=json.dumps(delete_options(uid)))
        store(output,"cancel.json",{"code":p.returncode,"output":p.stdout+p.stderr},secrets)


def run_cell(c, kind, k, jar, remote_contract, remote_truth, root, remote_root, storage, secrets, window_seconds):
    ns=c["cluster"]["namespace"]
    output=root/f"{kind}-k{k}";output.mkdir()
    wait_window(ns,window_seconds,output/"window-before.jsonl")
    name="b3255-"+kind+"-k"+str(k)+"-"+uuid.uuid4().hex[:12]
    pod,ok=pilot.get_pod(ns,name);baseline.require(ok and pod is None,"unique pod precondition failed")
    conf=baseline.spark_conf(c);conf.update(schedule());conf.update(storage)
    remote=remote_root+f"/{kind}-k{k}"
    conf.update({"spark.kubernetes.driver.pod.name":name,"spark.kubernetes.driver.deleteOnTermination":"false",
                 "spark.kubernetes.submission.waitAppCompletion":"false","spark.eventLog.dir":remote+"/spark-events",
                 "spark.driver.maxResultSize":"1g"})
    store(output,"spark-conf.json",conf,secrets)
    hdfs=os.environ.get("HADOOP_HOME","/opt/hadoop-3.3.1")+"/bin/hdfs"
    p=pilot.call([hdfs,"dfs","-mkdir","-p",remote+"/spark-events"],timeout=45)
    baseline.require(p.returncode==0,"cannot create event directory")
    command=[os.environ.get("SPARK_HOME","/opt/spark")+"/bin/spark-submit","--master",c["cluster"]["master"],
             "--deploy-mode","cluster","--name",name,"--class","BaselineBenchmark"]
    for key,value in conf.items():command += ["--conf",key+"="+value]
    command += [str(jar),remote_contract,c["query_part"],remote_truth,remote+"/samples.jsonl",kind,str(k)]
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
        logs=pilot.kube(["logs","-n",ns,name],timeout=45);store(output,"driver.log",logs.stdout+logs.stderr,secrets)
        d=pilot.kube(["describe","pod","-n",ns,name]);store(output,"describe.txt",d.stdout+d.stderr,secrets)
        pilot.record_pods_and_events(output,ns,name,uid,secrets)
        pilot.call([hdfs,"dfs","-get",remote+"/spark-events",str(output/"spark-events")],timeout=120)
        fetched=pilot.call([hdfs,"dfs","-cat",remote+"/samples.jsonl"],timeout=60)
        if fetched.returncode==0:
            store(output,"samples.jsonl",fetched.stdout,secrets)
            records=[json.loads(line) for line in fetched.stdout.splitlines() if line.strip()]
        eventlog=any(p.is_file() for p in (output/"spark-events").rglob("*")) if (output/"spark-events").exists() else False
        clean=window_valid(records,observations) and not api_unknown
        verdict={"kind":kind,"k":k,"pod":name,"uid":uid,"app":app,"state":state,"exclusive_window_valid":clean,
                 "eventlog_present":eventlog,"elapsed_seconds":time.monotonic()-start,
                 "baseline_valid":False,"cache_residency":"unknown","cleanup":"own terminal pod retained as evidence"}
        try:
            baseline.validate_run(records,eventlog)
            expected_marker='BASELINE_DONE {"ok":true,"kind":"'+kind+'","k":'+str(k)+'}'
            verdict["done_marker_present"] = expected_marker in logs.stdout
            verdict["baseline_valid"]=state=="SUCCEEDED" and clean and verdict["done_marker_present"]
        except (ValueError,KeyError) as exc:verdict["validation_error"]=str(exc)
        if not clean:verdict["window_action"]="whole cell invalid; explicit new-attempt rerun required, never silently accepted"
        store(output,"verdict.json",verdict,secrets)
    baseline.require(verdict["baseline_valid"],f"cell {kind} k{k} invalid; matrix stopped, preserve failure")
    return records


def run_matrix(c,contract_path,queries_path,truth_path,output,window_seconds=1800):
    os.umask(0o077);baseline.validate_contract(c);baseline.validate_asset_files(c,queries_path,truth_path)
    baseline.require(baseline.query_float32_sha256([json.loads(l) for l in Path(queries_path).read_text().splitlines() if l.strip()])==c["query_float32_sha256"],"frozen queries changed")
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=False)
    runid="matrix-"+datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")+"-"+uuid.uuid4().hex[:8]
    remote=c["cluster"]["evidence_root"]+"/"+runid
    storage,secrets=platform_config();hdfs=os.environ.get("HADOOP_HOME","/opt/hadoop-3.3.1")+"/bin/hdfs"
    store(output,"manifest.json",{"run_id":runid,"contract_sha256":baseline.sha256(contract_path),"truth_sha256":baseline.sha256(truth_path),
                                 "query_json_sha256":baseline.sha256(queries_path),"schedule":schedule(),"executor_schedule":"default, all nodes observed",
                                 "cells":baseline.cells(c),"remote":remote,"status":"STARTING"},secrets)
    src=Path(__file__).with_name("BaselineBenchmark.java");build=output/"build";build.mkdir();(build/"classes").mkdir()
    image=c["image"]+"@"+c["image_digest"]
    compiled=pilot.call(["docker","run","--rm","--user","0","--entrypoint","/bin/bash","-v",str(src)+":/src/BaselineBenchmark.java:ro",
                         "-v",str(build)+":/run",image,"-lc",'set -e; "$JAVA_HOME/bin/javac" -cp "/opt/spark/jars/*" -d /run/classes /src/BaselineBenchmark.java; "$JAVA_HOME/bin/jar" cf /run/BaselineBenchmark.jar -C /run/classes .'],timeout=300)
    store(build,"compile.log",compiled.stdout+compiled.stderr,secrets);baseline.require(compiled.returncode==0,"matrix source compile failed")
    p=pilot.call([hdfs,"dfs","-mkdir","-p",remote],timeout=45);baseline.require(p.returncode==0,"HDFS evidence creation failed")
    for local,name in [(contract_path,"contract.json"),(truth_path,"truth.json")]:
        p=pilot.call([hdfs,"dfs","-put",str(local),remote+"/"+name],timeout=60);baseline.require(p.returncode==0,"create-only asset upload failed")
    all_records=[]
    try:
        for kind,k in baseline.cells(c):
            all_records += run_cell(c,kind,k,build/"BaselineBenchmark.jar",remote+"/contract.json",remote+"/truth.json",
                                    output,remote,storage,secrets,window_seconds)
        baseline.validate_matrix(all_records,True)
        store(output,"matrix-verdict.json",{"complete":True,"cells":5,"samples":96,"cache_residency":"unknown"},secrets)
    except Exception as exc:
        store(output,"matrix-verdict.json",{"complete":False,"failure_type":type(exc).__name__,"reason":str(exc),"accepted_samples":len(all_records)},secrets)
        raise
