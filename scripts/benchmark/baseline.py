#!/usr/bin/env python3
"""Lance dev benchmark contract and offline evidence checks; no cluster work by default."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import struct
import sys

HERE = Path(__file__).resolve().parent

# Optional sixth cell: text BM25 k=10 (frozen by scripts/benchmark/text-10m.json). The five vector
# cells above stay untouched; this cell is appended only when the runner is asked for it.
TEXT_CELLS = (("text", 10),)
TEXT_QUERIES = 20
TEXT_K = 10
TEXT_CONTRACT_NAME = "text-10m.json"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_contract(c):
    require(c.get("schema_version") == 1, "unsupported contract version")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", c["image_digest"]), "immutable image digest required")
    require(c["version"] == 56 and c["rows"] == 10784907, "10M snapshot changed")
    require(c["query_count"] == 100 and c["dimension"] == 2560, "100 queries x 2560 required")
    require(c["single_ks"] == [10, 100, 2000] and c["batch_ks"] == [100, 2000], "five fixed cells required")
    require(c["single_query_ids"] == list(range(10)), "ten fixed distinct query ids required")
    require(c["index"] == "r3-ivf-4b" and c["nprobes"] == 8 and c["metric"] == "l2", "search axis changed")
    require(c["refine_factor"] is None, "baseline refine must be absent, never translate 0 to 1")
    require(c["id_column"] == "doc_uuid" and c["vector_column"] == "qwen_3_4b_embedding", "columns changed")
    require(c["with_row_id"] is True and c["result_columns"] == ["query_id", "doc_uuid", "_rowid", "_distance"], "result rowid contract changed")
    require(c["warmup_passes"] >= 1 and c["repeats"] >= 3, "warmup and >=3 repeats required")
    uuids = sorted(c["index_uuids"])
    require(len(set(uuids)) == c["index_segments"] == 32, "32 distinct index segments required")
    require(hashlib.sha256("\n".join(uuids).encode()).hexdigest() == c["index_uuid_sha256"], "index segment fingerprint changed")
    r = c["resources"]
    require((r["executors"], r["executor_cores"], r["executor_heap"], r["executor_overhead"],
             r["driver_cores"], r["driver_heap"], r["task_cpus"]) == (12, 4, "30g", "12g", 4, "12g", 1), "resource profile changed")
    require(r["index_cache_bytes"] == 6 * 1024**3 and r["metadata_cache_bytes"] == 1024**3,
            "native cache budget must stay 6GiB + 1GiB; do not use iceberg's 22GiB")
    require(r["aqe"] is True and r["shuffle_partitions"] == 48, "AQE/shuffle settings changed")


def cells(c):
    return [("single", k) for k in c["single_ks"]] + [("batch", k) for k in c["batch_ks"]]


def sql(c, kind, k, vector=None, diagnostic=False):
    require((kind, k) in cells(c), "unknown main cell")
    parts = [f"table => '{c['table']}'"]
    if kind == "batch":
        parts += [f"query_table => '{c['query_view']}'", "query_id_column => 'qid'", "query_vector_column => 'vector'"]
        columns = "query_id, doc_uuid, _rowid, _distance"
    else:
        require(vector is not None and len(vector) == c["dimension"], "single query needs one vector")
        require(all(math.isfinite(float(x)) for x in vector), "non-finite query")
        values = ",".join(str(float(x)) for x in vector)
        parts += [f"query_vector => array({values})"]
        columns = "doc_uuid, _rowid, _distance"
    parts += [f"vector_column => '{c['vector_column']}'", f"index => '{c['index']}'",
              f"version => {c['version']}", f"k => {k}", f"nprobes => {c['nprobes']}",
              f"distance_type => '{c['metric']}'", "with_row_id => true"]
    if diagnostic:
        parts.append("columns => array('doc_uuid')")
    return f"SELECT {columns} FROM VECTOR_SEARCH(" + ", ".join(parts) + ")"


def expected_text_sql_template(c):
    """The one SQL shape the text cell is allowed to execute (``{query}`` is substituted)."""
    require(isinstance(c, dict), "text contract must be a JSON object")
    return ("SELECT %s, `%s` FROM %s WHERE lance_match(%s, '{query}') "
            "ORDER BY `%s` DESC LIMIT %d" % (
                c.get("id_column"), c.get("score_column"), c.get("table"),
                c.get("column"), c.get("score_column"), int(c.get("k", 0))))


def validate_text_contract(c):
    """Validate the optional text-cell contract; the five vector cells are not involved."""
    require(c.get("schema_version") == 1, "unsupported text contract version")
    require(c.get("kind") == "text" and c.get("k") == TEXT_K, "text cell is k=%d" % TEXT_K)
    require(c.get("table") == "lance.smoke.mm_fixed" and c.get("column") == "content",
            "text table/column changed")
    require(c.get("id_column") == "doc_uuid" and c.get("score_column") == "_score",
            "text result columns changed")
    require(c.get("index") == "fmt_fts_content" and c.get("index_segments") == 48,
            "text FTS index identity changed")
    require(c.get("version") == 56 and c.get("rows") == 10784907, "10M snapshot changed")
    require(c.get("queries") == TEXT_QUERIES and c.get("warmup_passes") == 1
            and c.get("repeats") == 3, "text cell needs 20 queries, 1 warmup and 3 repeats")
    asset = c.get("queries_asset")
    require(isinstance(asset, str) and asset == Path(asset).name and asset.endswith(".jsonl")
            and "/" not in asset and "\\" not in asset, "queries_asset must be a bare .jsonl name")
    require(re.fullmatch(r"[0-9a-f]{64}", str(c.get("queries_sha256"))) is not None,
            "queries_sha256 must be 64 lowercase hex")
    require(c.get("sql_template") == expected_text_sql_template(c), "sql template shape changed")
    require(re.findall(r"\{[A-Za-z_]+\}", str(c.get("sql_template"))) == ["{query}"],
            "sql template must contain exactly one {query} placeholder")
    require(c.get("result_columns") == [c.get("id_column"), c.get("score_column")],
            "text result contract changed")
    timeouts = c.get("timeouts") or {}
    require(isinstance(timeouts.get("action_seconds"), int) and timeouts["action_seconds"] > 0,
            "text action timeout missing")
    return c


def load_text_asset(c, contract_path):
    """Read the frozen query asset next to the text contract and check its bytes."""
    path = Path(contract_path).resolve().parent / c["queries_asset"]
    require(path.is_file(), "text query asset not found: %s" % path)
    data = path.read_bytes()
    require(hashlib.sha256(data).hexdigest() == c["queries_sha256"],
            "text query asset changed: %s" % path)
    rows = []
    for number, line in enumerate(data.decode("utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError as exc:
            raise ValueError("%s:%d: invalid JSON line (%s)" % (path, number, exc))
        require(isinstance(row, dict) and set(row) == {"qid", "query"},
                "%s:%d: expected exactly qid+query" % (path, number))
        require(isinstance(row["qid"], int) and not isinstance(row["qid"], bool),
                "%s:%d: qid must be an int" % (path, number))
        require(isinstance(row["query"], str) and row["query"] == row["query"].strip()
                and row["query"], "%s:%d: query must be a non-empty trimmed string" % (path, number))
        rows.append(row)
    require(len(rows) == c["queries"], "%s: expected %d query lines" % (path, c["queries"]))
    require([row["qid"] for row in rows] == list(range(c["queries"])),
            "%s: qids must appear in file order as 0..%d" % (path, c["queries"] - 1))
    require(len({row["query"] for row in rows}) == c["queries"], "%s: duplicate query" % path)
    return rows


def text_sql(c, query):
    """The executed SQL for one frozen query: only ``{query}`` is substituted, quotes doubled."""
    require(isinstance(query, str) and query and query == query.strip() and "\n" not in query,
            "invalid text query")
    return c["sql_template"].replace("{query}", query.replace("'", "''"))


def text_expected_samples(c):
    return c["queries"] * (c["warmup_passes"] + c["repeats"])


def text_cells():
    return list(TEXT_CELLS)


def query_float32_sha256(queries):
    """Hash qid-sorted little-endian int64 id, int32 dimension, IEEE754 float32 payload."""
    h = hashlib.sha256()
    for q in sorted(queries, key=lambda q: q["qid"]):
        h.update(struct.pack("<qi", q["qid"], len(q["vector"])))
        for x in q["vector"]:
            h.update(struct.pack("<f", float(x)))
    return h.hexdigest()


def truth_identity(c, query_hash):
    return {"dataset_uri": c["dataset_uri"], "version": c["version"], "rows": c["rows"],
            "vector_column": c["vector_column"], "dimension": c["dimension"], "metric": c["metric"],
            "query_sha256": query_hash, "query_part_sha256": c["query_part_sha256"],
            "topk": 2000, "query_count": 100, "algorithm": "exact-bypass"}


def validate_assets(c, queries, truth, query_hash):
    require(len(queries) == 100, "exactly 100 query vectors required")
    require(sorted(q["qid"] for q in queries) == list(range(100)), "unique qid 0..99 required")
    for q in queries:
        require(len(q["vector"]) == c["dimension"], "query dimension mismatch")
        require(all(x is not None and math.isfinite(float(x)) for x in q["vector"]), "invalid query vector")
    require(truth.get("metadata") == truth_identity(c, query_hash), "truth snapshot/query/algorithm identity mismatch")
    rows = truth.get("queries", [])
    require(len(rows) == 100 and sorted(q["qid"] for q in rows) == list(range(100)), "truth must cover all 100 queries")
    for q in rows:
        require(len(q["ids"]) == len(q["rowids"]) == len(q["distances"]) == 2000, "truth must contain full top2000 per query")
        require(len(set(q["rowids"])) == 2000, "truth contains duplicate row ids")
        distances = q["distances"]
        require(all(math.isfinite(float(x)) for x in distances), "invalid truth distance")
        require(list(zip(distances, q["rowids"])) == sorted(zip(distances, q["rowids"])),
                "truth is not ordered by distance,rowid")


def validate_rows(rows, queries, k):
    require(len(rows) == queries * k, "result cardinality mismatch")
    grouped = {i: [] for i in range(queries)}
    for row in rows:
        q = int(row.get("query_id", 0))
        require(q in grouped, "unknown query id")
        require(row.get("doc_uuid") is not None, "NULL doc_uuid")
        require(math.isfinite(float(row["_distance"])), "invalid distance")
        require(isinstance(row.get("_rowid"), int), "rowid entity missing")
        grouped[q].append(row["_rowid"])
    for q, ids in grouped.items():
        require(len(ids) == k and len(set(ids)) == k, f"query {q} needs k distinct rowids")


def validate_run(records, eventlog_present):
    """Validate warmup/repeats per cell/query; unknown native residency stays unknown."""
    require(eventlog_present, "Spark event log missing")
    allowed = [("single", 10), ("single", 100), ("single", 2000), ("batch", 100),
               ("batch", 2000)] + text_cells()
    warmed = set(); observed = {}; diagnostics = []
    for rec in records:
        require(rec.get("complete") is True, "failed/cancelled sample")
        kind, k, qid = rec.get("kind"), rec.get("k"), rec.get("qid", "batch")
        require((kind, k) in allowed, "invalid cell identity")
        identity = (kind, k, qid)
        if rec.get("warmup"):
            warmed.add(identity)
        else:
            require(identity in warmed, "measured query precedes its own warmup")
            require(rec.get("started_ms") and rec.get("ms", -1) >= 0, "sample clock missing")
            repeats = observed.setdefault(identity, set())
            require(rec.get("repeat") not in repeats and rec.get("repeat") in (0, 1, 2), "duplicate/unknown repeat")
            repeats.add(rec["repeat"])
            evidence = rec.get("cache_evidence", {})
            diagnostics.append(evidence)
    require(observed and all(x == {0, 1, 2} for x in observed.values()), "three fixed repeats per query required")
    for kind, k in {(x[0], x[1]) for x in observed}:
        expected = (set(range(10)) if kind == "single"
                    else set(range(TEXT_QUERIES)) if kind == "text" else {"batch"})
        require({x[2] for x in observed if x[:2] == (kind, k)} == expected, "missing measured queries")
    # ScanStats currently lacks index-cache fields in b102cc7's Spark mapping. A driver SDK probe
    # is not evidence for executor SQL sessions. Never fabricate full-residency entries/coverage.
    verified = all(x.get("scope") == "sql-executor-session" and x.get("residency_verified") is True
                   for x in diagnostics)
    return {"warmup_valid": True, "cache_residency": "verified" if verified else "unknown",
            "samples": sum(map(len, observed.values()))}


def validate_matrix(records, eventlog_present, text=False, text_only=False):
    required = {("single", 10), ("single", 100), ("single", 2000), ("batch", 100), ("batch", 2000)}
    if text_only:
        require(text, "text-only matrix requires the text cell")
        required = set(text_cells())
    elif text:
        required |= set(text_cells())
    require({(x.get("kind"), x.get("k")) for x in records} == required,
            "full baseline requires exactly five fixed cells" if not text
            else ("text-only matrix requires exactly text-k10" if text_only
                  else "text matrix requires the five fixed cells plus text-k10"))
    return validate_run(records, eventlog_present)


def validate_asset_files(c, queries_path, truth_path):
    queries = [json.loads(l) for l in Path(queries_path).read_text().splitlines() if l.strip()]
    validate_assets(c, queries, json.loads(Path(truth_path).read_text()), query_float32_sha256(queries))


def foreign_active(pods, owned_uids):
    return [p for p in pods if p.get("phase") not in ("Succeeded", "Failed", "Error")
            and p.get("uid") not in owned_uids and p.get("role") in ("driver", "executor")]


def pod_state(pod, api_ok):
    if not api_ok:
        return "API_UNAVAILABLE"
    if pod is None:
        return "POD_GONE"
    return pod.get("status", {}).get("phase", "Pending").upper()


def record_created(path, run_id, namespace, name, uid):
    path = Path(path)
    rows = json.loads(path.read_text()) if path.exists() else []
    require(not any(x["namespace"] == namespace and x["name"] == name for x in rows), "refuse existing object or reused name")
    rows.append({"run_id": run_id, "namespace": namespace, "name": name, "uid": uid})
    path.write_text(json.dumps(rows, indent=2) + "\n")


def cleanup_allowed(path, run_id, namespace, name, uid):
    return any(x == {"run_id": run_id, "namespace": namespace, "name": name, "uid": uid}
               for x in json.loads(Path(path).read_text()))


def spark_conf(c):
    r, cluster = c["resources"], c["cluster"]
    return {
        "spark.kubernetes.namespace": cluster["namespace"],
        "spark.kubernetes.authenticate.driver.serviceAccountName": cluster["service_account"],
        "spark.kubernetes.authenticate.executor.serviceAccountName": cluster["service_account"],
        "spark.kubernetes.container.image": c["image"] + "@" + c["image_digest"],
        "spark.kubernetes.file.upload.path": cluster["staging"],
        "spark.driver.cores": str(r["driver_cores"]), "spark.driver.memory": r["driver_heap"],
        "spark.driver.memoryOverhead": r["driver_overhead"],
        "spark.executor.instances": str(r["executors"]), "spark.executor.cores": str(r["executor_cores"]),
        "spark.executor.memory": r["executor_heap"], "spark.executor.memoryOverhead": r["executor_overhead"],
        "spark.task.cpus": "1", "spark.dynamicAllocation.enabled": "false",
        "spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "48",
        "spark.eventLog.enabled": "true", "spark.eventLog.compress": "false",
        "spark.sql.extensions": "org.lance.spark.extensions.LanceSparkSessionExtensions",
        "spark.sql.catalog.lance": "org.lance.spark.LanceNamespaceSparkCatalog",
        "spark.sql.catalog.lance.impl": "dir", "spark.sql.catalog.lance.root": c["catalog_root"],
        "spark.sql.catalog.lance.index_cache_backend": f"moka://?capacity={r['index_cache_bytes']}",
        "spark.sql.catalog.lance.metadata_cache_backend": f"moka://?capacity={r['metadata_cache_bytes']}",
        "spark.sql.lance.search.distributed.enabled": "true",
        "spark.kubernetes.driverEnv.LANCE_INCLUDE_VECTOR_CENTROIDS": "false",
        "spark.executorEnv.LANCE_INCLUDE_VECTOR_CENTROIDS": "false",
        "spark.driver.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "spark.executor.extraJavaOptions": "--add-opens=java.base/java.nio=ALL-UNNAMED",
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["dry-run", "validate-assets", "validate-text-assets"])
    p.add_argument("--contract", type=Path, default=HERE / "baseline-10m.json")
    p.add_argument("--text-contract", type=Path, default=HERE / TEXT_CONTRACT_NAME)
    p.add_argument("--queries", type=Path)
    p.add_argument("--truth", type=Path)
    a = p.parse_args(); c = json.loads(a.contract.read_text()); validate_contract(c)
    if a.command == "validate-assets":
        require(a.queries and a.truth, "--queries and --truth required")
        validate_asset_files(c, a.queries, a.truth)
        print("ASSETS_OK")
        return
    if a.command == "validate-text-assets":
        require(a.text_contract.is_file(), "text contract not found: %s" % a.text_contract)
        text = validate_text_contract(json.loads(a.text_contract.read_text()))
        rows = load_text_asset(text, a.text_contract)
        print(json.dumps({"status": "TEXT_ASSETS_OK", "contract_sha256": sha256(a.text_contract),
                          "queries_sha256": text["queries_sha256"], "queries": len(rows),
                          "k": text["k"], "sql_template": text["sql_template"]}, indent=2))
        return
    placeholder = [0.0] * c["dimension"]
    print(json.dumps({"status": "DRY_RUN_ONLY", "requires_approval": ["compile/smoke", "truth", "five cells"],
                      "contract_sha256": sha256(a.contract), "spark_conf": spark_conf(c),
                      "cells": [{"kind": kind, "k": k, "sql": sql(c, kind, k, placeholder)} for kind, k in cells(c)]}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except ValueError as e:
        print("REFUSED:", e, file=sys.stderr)
        sys.exit(2)
