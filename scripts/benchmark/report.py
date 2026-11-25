#!/usr/bin/env python3
"""Aggregate one or two benchmark matrix directories into the standard report table.

This module is read-only post-processing: it never submits work, never rewrites
``samples.jsonl`` / ``verdict.json`` / ``activity*.json``, and it does not change the
measurement semantics of ``submit.py`` / ``BaselineBenchmark.java``.

Statistics contract (do not change a number without changing this docstring):

* A record with ``warmup: true`` (``repeat: -1``) is a warmup pass and never enters
  any statistic. Every measured record must appear after its own identity's warmup
  in ``samples.jsonl`` (``baseline.validate_run`` enforces the same rule).
* identity = (kind, k, qid): a ``single`` cell has 10 identities (qid 0..9), a
  ``batch`` cell has one identity (qid="batch"); each identity has 3 measured
  repeats, ``repeat`` in {0, 1, 2}.
* per-identity value = median of that identity's 3 measured ``ms``.
* cell p50 = median of the per-identity values: 10 values for ``single`` (mean of
  the 5th and 6th), one value for ``batch`` (median of the 3 whole-batch totals).
* cell p95 = quantile over **all** measured samples of the cell (30 for ``single``,
  3 for ``batch``) with linear interpolation between closest ranks:
  ``h = (n - 1) * p``, then floor/ceil neighbours weighted by the fractional part.
  This is ``numpy.quantile(method="linear")`` / Excel ``PERCENTILE.INC``, not
  nearest-rank.
* batch ``总耗时`` = median of the 3 whole-batch ``ms``; batch ``每查询`` =
  arithmetic mean of ``ms / N`` over the repeats (N = distinct qid count found in
  the sample's ``results``; contract ``query_count`` is only a fallback). Because
  every repeat uses the same N this equals ``mean(ms) / N``.
* ``recall_min`` = minimum over every value of every measured sample's ``recall``
  map (each sample's map is {qid: hits/k}). A sample without ``recall`` makes the
  cell report ``n/a``; it is never turned into 0.
* ``outliers`` = measured ``ms`` > 5 x that identity's measured median
  (``--outlier-factor``). Outliers stay inside p50/p95 -- they are listed with
  qid/repeat/ms/share of the cell's measured total, and never dropped.
* Anything missing is rendered ``n/a`` with a reason: absent cell directory,
  absent ``samples.jsonl`` / ``verdict.json``, zero measured samples,
  ``complete=false`` records, or ``verdict.baseline_valid=false`` (the runner
  itself rejected the cell). Numbers are never fabricated as 0.

Window wording: only identifiable *newly appeared foreign* Spark pods close the
window (``submit.activity()`` classes benchmark/unknown); long-lived background
services are allowed. Cache wording: warm-session only; native index-cache
residency stays unverified unless ``cache_evidence.residency_verified`` is true.
"""

import argparse
import datetime
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import sys

HERE = Path(__file__).resolve().parent
DEFAULT_CONTRACT = HERE / "baseline-10m.json"
CELL_RE = re.compile(r"^(?P<kind>[a-z][a-z0-9_-]*)-k(?P<k>[0-9]+)$")
CAPACITY_RE = re.compile(r"capacity=([0-9]+)")
SIZE_RE = re.compile(r"^([0-9]+(?:\.[0-9]+)?)\s*([kmgt]?)(?:i?b)?$", re.IGNORECASE)
OUTLIER_FACTOR = 5.0

KIND_LABELS = {"single": "向量单查询", "batch": "批量", "fts": "文本 BM25",
               "text": "文本 BM25", "bm25": "文本 BM25", "text-bm25": "文本 BM25"}
KIND_ORDER = {"single": 0, "batch": 1}

# label, spark-conf key, group used to explain a mismatch direction.
RESOURCE_ROWS = (
    ("executor 数", "spark.executor.instances", "resource"),
    ("executor cores", "spark.executor.cores", "resource"),
    ("executor heap", "spark.executor.memory", "resource"),
    ("executor overhead", "spark.executor.memoryOverhead", "resource"),
    ("driver cores", "spark.driver.cores", "resource"),
    ("driver heap", "spark.driver.memory", "resource"),
    ("driver overhead", "spark.driver.memoryOverhead", "resource"),
    ("task.cpus", "spark.task.cpus", "resource"),
    ("AQE", "spark.sql.adaptive.enabled", "plan"),
    ("shuffle partitions", "spark.sql.shuffle.partitions", "plan"),
    ("index cache 预算", "spark.sql.catalog.lance.index_cache_backend", "resource"),
    ("metadata cache 预算", "spark.sql.catalog.lance.metadata_cache_backend", "resource"),
    ("distributed search", "spark.sql.lance.search.distributed.enabled", "plan"),
    ("镜像", "spark.kubernetes.container.image", "image"),
)

CONTRACT_CHECKS = (
    ("executor 数", "executors"),
    ("executor cores", "executor_cores"),
    ("executor heap", "executor_heap"),
    ("executor overhead", "executor_overhead"),
    ("driver cores", "driver_cores"),
    ("driver heap", "driver_heap"),
    ("driver overhead", "driver_overhead"),
    ("task.cpus", "task_cpus"),
    ("index cache 预算", "index_cache_bytes"),
    ("metadata cache 预算", "metadata_cache_bytes"),
    ("AQE", "aqe"),
    ("shuffle partitions", "shuffle_partitions"),
)

SCALE_ROWS = (
    ("行数", "rows"),
    ("fragments", "fragments"),
    ("版本", "version"),
    ("表名", "table"),
    ("dataset_uri", "dataset_uri"),
    ("向量列", "vector_column"),
    ("id 列", "id_column"),
    ("维度", "dimension"),
    ("距离口径", "metric"),
    ("索引名", "index"),
    ("索引段数", "index_segments"),
    ("nprobes", "nprobes"),
    ("refine_factor", "refine_factor"),
)


class ReportError(ValueError):
    """Raised for any input that must not silently become a number."""


def fail(message):
    raise ReportError(message)


# --------------------------------------------------------------------------- io

def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path, what):
    try:
        return json.loads(Path(path).read_text())
    except OSError as exc:
        fail("%s: cannot read %s (%s)" % (what, path, type(exc).__name__))
    except ValueError as exc:
        fail("%s: invalid JSON in %s (%s)" % (what, path, exc))


def iter_jsonl(path):
    with Path(path).open() as handle:
        for number, line in enumerate(handle, 1):
            if line.strip():
                try:
                    yield json.loads(line)
                except ValueError:
                    fail("%s:%d: invalid JSON line" % (path, number))


# ------------------------------------------------------------------ statistics

def quantile_linear(values, probability):
    """Quantile with h = (n-1) * p; same as numpy's default and Excel PERCENTILE.INC."""
    samples = sorted(float(value) for value in values)
    if not samples:
        fail("cannot take a quantile of zero samples")
    if not 0.0 <= probability <= 1.0:
        fail("quantile probability outside [0, 1]: %r" % (probability,))
    position = (len(samples) - 1) * probability
    lower = int(math.floor(position))
    fraction = position - lower
    if lower + 1 >= len(samples):
        return samples[-1]
    return samples[lower] + (samples[lower + 1] - samples[lower]) * fraction


def cell_identity(name):
    match = CELL_RE.match(name)
    if not match:
        return None
    return match.group("kind"), int(match.group("k"))


def empty_cell(kind, k, directory):
    return {
        "kind": kind, "k": k, "dir": directory,
        "measured": 0, "warmup": 0, "failed": 0,
        "medians": {}, "p50": None, "p95": None, "per_query": None, "queries": None,
        "recall_min": None, "recall_missing": 0, "recall_values": 0,
        "outliers": [], "cache_verified": 0, "cache_total": 0, "total_ms": 0.0,
        "problems": [], "unavailable": None, "missing": [],
        "verdict": {}, "activity": {}, "spark_conf": {},
        "sql_checked": 0, "sql_mismatch": 0, "complete": False,
    }


def verdict_reason(verdict):
    if verdict.get("validation_error"):
        return str(verdict["validation_error"])
    if verdict.get("exclusive_window_valid") is False:
        return "exclusive_window_valid=false，窗口内有外来 pod 或 API 不可用"
    if verdict.get("state") and verdict["state"] != "SUCCEEDED":
        return "state=%s" % verdict["state"]
    if verdict.get("done_marker_present") is False:
        return "缺 BASELINE_DONE 标记"
    return "见 verdict.json"


def activity_summary(directory):
    summary = {"polls": 0, "api_fail": 0, "foreign_polls": 0, "unknown_polls": 0,
               "categories": {}}
    path = Path(directory) / "activity.jsonl"
    if path.is_file():
        for record in iter_jsonl(path):
            summary["polls"] += 1
            if not record.get("api_ok"):
                summary["api_fail"] += 1
            foreign = record.get("foreign") or []
            if foreign:
                summary["foreign_polls"] += 1
                if any(item.get("category") == "unknown" for item in foreign):
                    summary["unknown_polls"] += 1
            for category, count in (record.get("classification") or {}).items():
                summary["categories"][category] = summary["categories"].get(category, 0) + int(count)
    final = Path(directory) / "activity-final.json"
    if final.is_file():
        record = read_json(final, "activity-final")
        summary["final_api_ok"] = record.get("api_ok")
        summary["final_foreign"] = len(record.get("foreign") or [])
    return summary


def sql_evidence(measured, contract):
    """Count measured VECTOR_SEARCH samples whose inline SQL matches the contract."""
    if not contract or not isinstance(contract.get("index"), str):
        return 0, 0
    patterns = [
        re.compile(r"index => '%s'" % re.escape(contract["index"])),
        re.compile(r"nprobes => %d(?!\d)" % int(contract["nprobes"])),
        re.compile(r"version => %d(?!\d)" % int(contract["version"])),
    ]
    checked = mismatch = 0
    for record in measured:
        statement = str(record.get("sql", ""))
        if "VECTOR_SEARCH" not in statement:
            continue
        checked += 1
        if not all(pattern.search(statement) for pattern in patterns):
            mismatch += 1
    return checked, mismatch


def compute_cell(kind, k, directory, run_name, contract, skip_missing, outlier_factor):
    """Stream one cell's samples; keep only the fields the report needs.

    Batch k=2000 samples carry ~200k result rows each, so records are never retained
    whole: the memory high-water mark is one JSON line, not the whole file.
    """
    cell = empty_cell(kind, k, directory)
    samples_path = directory / "samples.jsonl"
    verdict_path = directory / "verdict.json"
    conf_path = directory / "spark-conf.json"
    missing = [path.name for path in (samples_path, verdict_path) if not path.is_file()]
    if missing:
        message = "run %s: cell %s-k%d is missing %s under %s" % (
            run_name, kind, k, "、".join(missing), directory)
        if skip_missing:
            cell["missing"] = missing
            cell["unavailable"] = "缺 " + "、".join(missing)
            return cell
        fail(message)
    verdict = read_json(verdict_path, "verdict of %s-k%d" % (kind, k))
    if verdict.get("kind") not in (None, kind) or verdict.get("k") not in (None, k):
        fail("run %s: %s declares kind/k %r/%r but the directory is %s-k%d"
             % (run_name, verdict_path, verdict.get("kind"), verdict.get("k"), kind, k))
    if conf_path.is_file():
        cell["spark_conf"] = read_json(conf_path, "spark-conf of %s-k%d" % (kind, k))

    measured, repeat_sets, warmed = [], {}, set()
    warmup = 0
    records_seen = 0
    for index, record in enumerate(iter_jsonl(samples_path), 1):
        records_seen += 1
        where = "%s:%d" % (samples_path, index)
        if (record.get("kind"), record.get("k")) != (kind, k):
            fail("%s: sample kind/k %r/%r does not match directory name %s-k%d"
                 % (where, record.get("kind"), record.get("k"), kind, k))
        identity = str(record.get("qid"))
        if record.get("warmup") is True:
            warmup += 1
            warmed.add(identity)
            continue
        if identity not in warmed:
            fail("%s: measured qid=%s has no preceding warmup sample" % (where, identity))
        repeat = record.get("repeat")
        if repeat not in (0, 1, 2):
            fail("%s: repeat=%r is outside {0, 1, 2}" % (where, repeat))
        seen = repeat_sets.setdefault(identity, set())
        if repeat in seen:
            fail("%s: duplicate repeat=%d for qid=%s" % (where, repeat, identity))
        seen.add(repeat)
        if record.get("complete") is not True:
            cell["failed"] += 1
            continue
        ms = record.get("ms")
        if isinstance(ms, bool) or not isinstance(ms, (int, float)) or not math.isfinite(float(ms)) or ms < 0:
            fail("%s: measured sample has invalid ms=%r" % (where, ms))
        results = record.get("results")
        measured.append({
            "qid": identity, "repeat": repeat, "ms": float(ms),
            "results_count": len(results) if isinstance(results, dict) else None,
            "recall": record.get("recall"), "sql": record.get("sql", ""),
            "cache_verified": (record.get("cache_evidence") or {}).get("residency_verified") is True,
        })
    if not records_seen:
        if skip_missing:
            cell["unavailable"] = "samples.jsonl 无记录"
            return cell
        fail("run %s: cell %s-k%d: samples.jsonl has no records (%s)" % (run_name, kind, k, samples_path))

    cell["warmup"] = warmup
    cell["measured"] = len(measured)
    cell["cache_total"] = len(measured)
    cell["cache_verified"] = sum(1 for record in measured if record["cache_verified"])
    cell["verdict"] = verdict
    cell["activity"] = activity_summary(directory)

    by_identity = {}
    for record in measured:
        by_identity.setdefault(record["qid"], []).append(record["ms"])
    cell["medians"] = {qid: statistics.median(values) for qid, values in by_identity.items()}
    cell["total_ms"] = sum(record["ms"] for record in measured)
    cell["queries"] = len(by_identity)

    if cell["failed"]:
        cell["unavailable"] = "%d 条 complete=false" % cell["failed"]
        cell["problems"].append(cell["unavailable"])
    elif not measured:
        cell["unavailable"] = "无 measured 样本"
    else:
        cell["p50"] = statistics.median(cell["medians"].values())
        cell["p95"] = quantile_linear([record["ms"] for record in measured], 0.95)

    if kind == "batch":
        counts = [record["results_count"] for record in measured if record["results_count"]]
        if counts and len(set(counts)) == 1:
            cell["per_query"] = (sum(record["ms"] for record in measured) / len(measured)) / counts[0]
            cell["queries"] = counts[0]
        elif measured:
            fallback = (contract or {}).get("query_count")
            if counts:
                cell["problems"].append("各 repeat 的 results 查询数不同：%s" % sorted(set(counts)))
            elif isinstance(fallback, int) and fallback > 0:
                cell["problems"].append("样本缺 results，每查询按合同 query_count=%d 折算" % fallback)
                cell["queries"] = fallback
                cell["per_query"] = (sum(record["ms"] for record in measured) / len(measured)) / fallback
            else:
                cell["problems"].append("样本缺 results 且无合同 query_count，每查询为 n/a")

    recall_values = []
    for record in measured:
        recall = record["recall"]
        if not isinstance(recall, dict) or not recall:
            cell["recall_missing"] += 1
            continue
        for value in recall.values():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                fail("cell %s-k%d: non-numeric recall value %r" % (kind, k, value))
            recall_values.append(float(value))
    cell["recall_values"] = len(recall_values)
    if recall_values:
        cell["recall_min"] = min(recall_values)
    if cell["recall_missing"]:
        cell["problems"].append("%d 条 measured 样本无 recall" % cell["recall_missing"])

    for record in measured:
        median = cell["medians"][record["qid"]]
        if median <= 0:
            cell["problems"].append("qid=%s 的 measured 中位数为 0，跳过离群判定" % record["qid"])
            continue
        if record["ms"] > outlier_factor * median:
            share = (record["ms"] / cell["total_ms"] * 100.0) if cell["total_ms"] else None
            cell["outliers"].append({"qid": record["qid"], "repeat": record["repeat"],
                                     "ms": record["ms"], "share": share,
                                     "ratio": record["ms"] / median})

    incomplete = sorted(identity for identity, repeats in repeat_sets.items() if repeats != {0, 1, 2})
    if incomplete:
        cell["problems"].append("repeat 不足 3 个的 identity：%s" % "、".join(incomplete))
    cell["complete"] = bool(measured) and not cell["failed"] and not incomplete

    if verdict.get("baseline_valid") is False:
        cell["unavailable"] = "verdict.baseline_valid=false：" + verdict_reason(verdict)

    checked, mismatch = sql_evidence(measured, contract)
    cell["sql_checked"], cell["sql_mismatch"] = checked, mismatch
    if mismatch:
        cell["problems"].append("SQL 与合同索引参数不一致（%d/%d 条）" % (mismatch, checked))
    return cell


# ------------------------------------------------------------------- run level

def resolve_contract(directory, explicit):
    if explicit is not None:
        path = Path(explicit)
        if not path.is_file():
            fail("--contract not found: %s" % path)
        return path, "命令行 --contract"
    local = Path(directory) / "contract.json"
    if local.is_file():
        return local, "run 目录 contract.json"
    if DEFAULT_CONTRACT.is_file():
        return DEFAULT_CONTRACT, "仓库 scripts/benchmark/baseline-10m.json"
    return None, "无合同"


def harness_commit(run, override):
    if override:
        return override, "--harness-commit"
    manifest = run.get("manifest") or {}
    for key in ("harness_commit", "git_commit", "commit"):
        value = manifest.get(key)
        if isinstance(value, str) and value:
            return value, "manifest.%s" % key
    for name in ("harness-commit.txt", "git-commit.txt"):
        path = run["dir"] / name
        if path.is_file():
            text = path.read_text().strip()
            if text:
                return text, name
    return None, "missing"


def load_run(name, directory, args):
    directory = Path(directory).resolve()
    if not directory.is_dir():
        fail("run %s: not a directory: %s" % (name, directory))
    manifest_path = directory / "manifest.json"
    manifest = read_json(manifest_path, "manifest of run %s" % name) if manifest_path.is_file() else None
    contract_path, contract_source = resolve_contract(directory, args.contract)
    contract = read_json(contract_path, "contract") if contract_path else None
    run = {"name": name, "dir": directory, "manifest": manifest, "cells": {},
           "contract": contract, "contract_source": contract_source,
           "contract_path": contract_path,
           "contract_sha256": sha256_file(contract_path) if contract_path else None,
           "manifest_contract_sha256": (manifest or {}).get("contract_sha256")}
    run["harness_commit"] = harness_commit(run, args.harness_commit)
    run["contract_matches_manifest"] = (
        None if run["manifest_contract_sha256"] is None else
        run["manifest_contract_sha256"] == run["contract_sha256"])
    for child in sorted(directory.iterdir()):
        if not child.is_dir():
            continue
        identity = cell_identity(child.name)
        if identity is None:
            continue
        kind, k = identity
        run["cells"][(kind, k)] = compute_cell(kind, k, child, name, contract,
                                               args.skip_missing_cells, args.outlier_factor)
    if not run["cells"]:
        fail("run %s: no cell directories (expected names like single-k10) under %s"
             % (name, directory))
    return run


def split_run_spec(spec):
    if "=" in spec:
        name, directory = spec.split("=", 1)
        if name and directory and "/" not in name and "\\" not in name:
            return name, directory
    path = Path(spec)
    return path.resolve().name, spec


# ------------------------------------------------------------------ formatting

def md_cell(value):
    text = "n/a" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def fmt_ms(value):
    return "n/a" if value is None else "%.1f" % value


def size_bytes(text):
    if isinstance(text, bool):
        return None
    if isinstance(text, (int, float)):
        return float(text)
    match = SIZE_RE.match(str(text).strip())
    if not match:
        return None
    unit = match.group(2).lower()
    return float(match.group(1)) * {"": 1, "k": 1024, "m": 1024 ** 2,
                                    "g": 1024 ** 3, "t": 1024 ** 4}[unit]


def conf_scalar(label, raw):
    if raw is None:
        return None
    text = str(raw)
    match = CAPACITY_RE.search(text)
    if match:
        return float(match.group(1))
    if label in ("AQE", "distributed search"):
        return text.lower()
    if label in ("镜像", "shuffle partitions"):
        return text
    return size_bytes(text)


def display_conf(label, raw):
    if raw is None:
        return None
    match = CAPACITY_RE.search(str(raw))
    if match:
        return "%.1f GiB（capacity=%s）" % (int(match.group(1)) / 1024 ** 3, match.group(1))
    return str(raw)


def scene_label(kind, cell):
    n = (cell or {}).get("queries")
    if kind == "single":
        return KIND_LABELS["single"]
    if kind == "batch":
        return "批量 %d 查询" % n if n else KIND_LABELS["batch"]
    if kind in KIND_LABELS:
        return KIND_LABELS[kind]
    return kind


def scene_sort_key(identity):
    kind, k = identity
    return (KIND_ORDER.get(kind, 2), kind, k)


def cell_value(cell):
    if cell is None:
        return "n/a（该 run 无此格目录）"
    if cell["unavailable"]:
        return "n/a（%s）" % cell["unavailable"]
    if cell["p50"] is None:
        return "n/a（无可用 measured 样本）"
    if cell["kind"] == "batch":
        value = "%s（%s）" % (fmt_ms(cell["p50"]), fmt_ms(cell["per_query"]))
    else:
        value = "%s（%s）" % (fmt_ms(cell["p50"]), fmt_ms(cell["p95"]))
    if not cell["complete"]:
        value += "*"
    return value


def ratio_text(control, treatment):
    if control is None or treatment is None:
        return "n/a"
    if control["unavailable"] or treatment["unavailable"]:
        return "n/a"
    if control["p50"] is None or treatment["p50"] is None:
        return "n/a"
    baseline, other = control["p50"], treatment["p50"]
    if abs(baseline - other) < 1e-9:
        text = "持平"
    elif baseline < other:
        text = "对照组快 %.2f×（本方慢）" % (other / baseline)
    else:
        text = "本方快 %.2f×（对照组慢）" % (baseline / other)
    if not (control["complete"] and treatment["complete"]):
        text += "*"
    return text


def scale_value(run, contract_key):
    contract = (run or {}).get("contract")
    if not contract or contract_key not in contract:
        return None
    value = contract[contract_key]
    if contract_key == "refine_factor":
        return "absent" if value is None else value
    return value


def scale_alignment(label, control_value, treatment_value):
    if control_value is None or treatment_value is None:
        return "一侧无合同"
    if str(control_value) == str(treatment_value):
        return "对齐"
    if label == "nprobes":
        return "不一致：nprobes 不同，单查询扫描面不同"
    if label == "refine_factor":
        return "不一致：refine 口径不同"
    if label == "索引名":
        return "不一致：索引不同，需在召回列上一起看"
    return "不一致：数据/索引口径不同，比值不可跨口径解释"


def run_conf(run):
    out = {}
    for label, key, group in RESOURCE_ROWS:
        raw_by_cell = {}
        for (kind, k), cell in sorted(run["cells"].items()):
            raw = (cell.get("spark_conf") or {}).get(key)
            if raw is not None:
                raw_by_cell.setdefault(str(raw), []).append("%s-k%d" % (kind, k))
        if not raw_by_cell:
            continue
        if len(raw_by_cell) == 1:
            raw = next(iter(raw_by_cell))
            out[label] = {"raw": raw, "group": group, "inconsistent": False,
                          "scalar": conf_scalar(label, raw), "cells": raw_by_cell[raw]}
        else:
            out[label] = {"raw": "、".join(sorted(raw_by_cell)), "group": group,
                          "inconsistent": True, "scalar": None,
                          "detail": "；".join("%s=%s" % (raw, ",".join(cells))
                                              for raw, cells in sorted(raw_by_cell.items()))}
    return out


def alignment_text(control_detail, treatment_detail):
    if control_detail is None or treatment_detail is None:
        return "一侧无 spark-conf 证据"
    if control_detail["inconsistent"] or treatment_detail["inconsistent"]:
        return "该 run 内 spark-conf 不一致，需人工核对"
    if control_detail["raw"] == treatment_detail["raw"]:
        return "对齐"
    if control_detail["group"] == "image":
        return "镜像不同（feature 对照的预期差异）"
    if control_detail["group"] == "plan":
        return "执行计划口径不同，比值仅作量级参照"
    control_bytes, treatment_bytes = control_detail["scalar"], treatment_detail["scalar"]
    if treatment_bytes is None or control_bytes is None:
        return "不同（差值无法自动解析，需人工核对）"
    if treatment_bytes > control_bytes:
        return "本方更多 → 本方指标偏乐观"
    if treatment_bytes < control_bytes:
        return "本方更少 → 本方数字是保守下界"
    return "不同"


def contract_mismatches(run):
    contract = (run or {}).get("contract") or {}
    resources = contract.get("resources") or {}
    conf = run_conf(run)
    problems = []
    for label, key in CONTRACT_CHECKS:
        if key not in resources:
            continue
        detail = conf.get(label)
        if detail is None or detail["inconsistent"] or detail["scalar"] is None:
            problems.append("%s：实际 spark-conf 缺失或不一致" % label)
            continue
        expected = conf_scalar(label, resources[key])
        if expected is None:
            problems.append("%s：合同值无法解析" % label)
        elif detail["scalar"] != expected:
            problems.append("%s：合同 %s / 实际 %s" % (label, resources[key], detail["raw"]))
    return problems


def contract_resource_line(run):
    contract = (run or {}).get("contract") or {}
    resources = contract.get("resources") or {}
    if not resources:
        return None
    return ("%d × (%dc, %s+%s)，driver %dc/%s+%s，task.cpus=%s，index_cache %s GiB，"
            "metadata %s GiB，AQE %s，shuffle %s" % (
                resources.get("executors", 0), resources.get("executor_cores", 0),
                resources.get("executor_heap"), resources.get("executor_overhead"),
                resources.get("driver_cores", 0), resources.get("driver_heap"),
                resources.get("driver_overhead"), resources.get("task_cpus"),
                "%.1f" % (resources.get("index_cache_bytes", 0) / 1024 ** 3),
                "%.1f" % (resources.get("metadata_cache_bytes", 0) / 1024 ** 3),
                "true" if resources.get("aqe") is True else
                "false" if resources.get("aqe") is False else resources.get("aqe"),
                resources.get("shuffle_partitions")))


def activity_totals(run):
    totals = {"polls": 0, "api_fail": 0, "foreign_polls": 0, "unknown_polls": 0, "cells": 0}
    for cell in run["cells"].values():
        activity = cell.get("activity") or {}
        for key in ("polls", "api_fail", "foreign_polls", "unknown_polls"):
            totals[key] += activity.get(key, 0)
        totals["cells"] += 1
    return totals


def union_cells(control, treatment):
    keys = set()
    for run in (control, treatment):
        if run:
            keys.update(run["cells"])
    return [identity for identity in sorted(keys, key=scene_sort_key)]


# -------------------------------------------------------------------- render

def render_table(control, treatment, rows):
    lines = ["| 场景 | k | 对照组数值 | 本方数值 | 比值 |",
             "| --- | --- | --- | --- | --- |"]
    for kind, k in rows:
        control_cell = control["cells"].get((kind, k)) if control else None
        treatment_cell = treatment["cells"].get((kind, k)) if treatment else None
        label = scene_label(kind, control_cell or treatment_cell)
        control_text = "n/a（未指定对照组）" if control is None else cell_value(control_cell)
        treatment_text = "n/a（未指定本方）" if treatment is None else cell_value(treatment_cell)
        lines.append("| %s | %d | %s | %s | %s |" % (
            md_cell(label), k, md_cell(control_text), md_cell(treatment_text),
            md_cell(ratio_text(control_cell, treatment_cell))))
    return lines


def render_scale(control, treatment):
    lines = ["## 数据规模与索引参数", "",
             "| 项目 | 对照组 | 本方 | 对齐/偏差 |",
             "| --- | --- | --- | --- |"]
    for label, key in SCALE_ROWS:
        control_value = scale_value(control, key)
        treatment_value = scale_value(treatment, key)
        alignment = ("—（仅一侧有数据）" if control is None or treatment is None
                     else scale_alignment(label, control_value, treatment_value))
        lines.append("| %s | %s | %s | %s |" % (
            md_cell(label), md_cell(control_value), md_cell(treatment_value),
            md_cell(alignment)))
    lines.append("")
    for run, role in ((control, "对照组"), (treatment, "本方")):
        if not run:
            continue
        if run["contract"] is None:
            lines.append("- %s 合同：n/a（未找到 contract.json 或 baseline-10m.json）" % role)
            continue
        if run["contract_matches_manifest"] is True:
            fingerprint = "sha256 与 manifest.contract_sha256 一致"
        elif run["contract_matches_manifest"] is False:
            fingerprint = "⚠ sha256 与 manifest.contract_sha256 不一致（manifest=%s）" % run["manifest_contract_sha256"]
        else:
            fingerprint = "manifest 无 contract_sha256，指纹未核对"
        lines.append("- %s 合同：%s（来源：%s；%s）" % (
            role, Path(run["contract_path"]).name, run["contract_source"], fingerprint))
        checked = sum(cell["sql_checked"] for cell in run["cells"].values())
        mismatch = sum(cell["sql_mismatch"] for cell in run["cells"].values())
        if checked:
            lines.append("- %s 样本内嵌 SQL 与合同索引参数：%d/%d 条一致%s" % (
                role, checked - mismatch, checked,
                "" if not mismatch else "，⚠ %d 条不一致" % mismatch))
    return lines


def render_resources(control, treatment):
    lines = ["## 资源、镜像与 harness", ""]
    reference = contract_resource_line(control) or contract_resource_line(treatment)
    if reference:
        lines += ["合同要求（baseline-10m.json）：%s。" % reference, ""]
    lines += ["| 项目 | 对照组 | 本方 | 对齐/偏差方向 |",
              "| --- | --- | --- | --- |"]
    control_conf = run_conf(control) if control else {}
    treatment_conf = run_conf(treatment) if treatment else {}
    labels, alignments = [], {}
    for label, _key, _group in RESOURCE_ROWS:
        if label in control_conf or label in treatment_conf:
            labels.append(label)
    for label in labels:
        control_detail = control_conf.get(label)
        treatment_detail = treatment_conf.get(label)
        control_text = display_conf(label, control_detail["raw"]) if control_detail else None
        treatment_text = display_conf(label, treatment_detail["raw"]) if treatment_detail else None
        if control_detail and control_detail["inconsistent"]:
            control_text = "%s（%s）" % (control_text, control_detail["detail"])
        if treatment_detail and treatment_detail["inconsistent"]:
            treatment_text = "%s（%s）" % (treatment_text, treatment_detail["detail"])
        alignment = ("—（仅一侧有数据）" if control is None or treatment is None
                     else alignment_text(control_detail, treatment_detail))
        alignments[label] = alignment
        lines.append("| %s | %s | %s | %s |" % (
            md_cell(label), md_cell(control_text), md_cell(treatment_text),
            md_cell(alignment)))
    lines.append("")
    mismatched = [label for label in labels if alignments.get(label) != "对齐"]
    if control and treatment:
        if mismatched:
            lines.append("- 不对齐项：%s（偏差方向见上表最后一列）。" % "、".join(mismatched))
        else:
            lines.append("- 不对齐项：无（资源/计划项逐项一致）。")
    for run, role in ((control, "对照组"), (treatment, "本方")):
        if not run:
            continue
        mismatches = contract_mismatches(run)
        if mismatches:
            lines.append("- %s 实际 spark-conf 与合同不一致：%s" % (role, "；".join(mismatches)))
        else:
            lines.append("- %s 实际 spark-conf 与合同逐项一致（%d 项）" % (role, len(CONTRACT_CHECKS)))
        contract = run["contract"] or {}
        image = conf_scalar("镜像", (run_conf(run).get("镜像") or {}).get("raw"))
        declared = contract.get("image_digest")
        if declared:
            matched = image is not None and str(declared) in str(image)
            lines.append("- %s 镜像 digest：%s；合同 image_digest 匹配：%s" % (
                role, declared, "是" if matched else "否/无法确认"))
        commit, source = run["harness_commit"]
        if commit:
            lines.append("- %s harness commit：`%s`（来源：%s）" % (role, commit, source))
        else:
            lines.append("- %s harness commit：未记录（run 产物无 harness_commit/git_commit 字段，"
                         "本工具不推测）" % role)
    return lines


def render_window(control, treatment):
    lines = ["## 窗口口径", "",
             "- 只排除「可识别的、新增的外来 Spark pod」（submit.activity() 归为 benchmark/unknown "
             "的活 driver/executor）；Kyuubi/ThriftServer/Spark Connect 等常驻 background 允许作为共享背景，"
             "但每格必须在样本前后都有轮询证据。",
             "- 判定依据：每格 verdict.exclusive_window_valid，以及 activity.jsonl 的轮询记录。"]
    for run, role in ((control, "对照组"), (treatment, "本方")):
        if not run:
            continue
        totals = activity_totals(run)
        with_verdict = [cell for cell in run["cells"].values() if cell.get("verdict")]
        valid = sum(1 for cell in with_verdict
                    if cell["verdict"].get("exclusive_window_valid") is True)
        lines.append("- %s `%s`：窗口有效 %d/%d 格（有 verdict 的格）；activity 轮询 %d 次，"
                     "API 失败 %d 次，含外来 pod 的轮询 %d 次（其中 unknown %d 次）。" % (
                         role, run["name"], valid, len(with_verdict), totals["polls"],
                         totals["api_fail"], totals["foreign_polls"], totals["unknown_polls"]))
    return lines


def render_cache(control, treatment):
    lines = ["## 缓存口径", "",
             "- 采用 warm-session：每个 identity 先执行 warmup（repeat=-1），measured 只取预热后的 3 个 repeat；"
             "本表不包含任何冷启动数字。",
             "- native index-cache residency **unverified**：只有 samples 里 "
             "cache_evidence.residency_verified=true 才算证据。"]
    for run, role in ((control, "对照组"), (treatment, "本方")):
        if not run:
            continue
        verified = sum(cell["cache_verified"] for cell in run["cells"].values())
        total = sum(cell["cache_total"] for cell in run["cells"].values())
        states = sorted({cell["verdict"].get("cache_residency") for cell in run["cells"].values()
                         if cell.get("verdict") and cell["verdict"].get("cache_residency")})
        lines.append("- %s `%s`：residency_verified=true 的 measured 样本 %d/%d；"
                     "verdict.cache_residency=%s。因此只能声称「同 session 预热后」，不能声称索引常驻内存。" % (
                         role, run["name"], verified, total,
                         "、".join(states) if states else "未记录"))
    return lines


def render_outliers(control, treatment, rows, outlier_factor):
    lines = ["## 离群样本（不剔除，仍计入 p50/p95）", ""]
    entries = []
    for run, role in ((control, "对照组"), (treatment, "本方")):
        if not run:
            continue
        for kind, k in rows:
            cell = run["cells"].get((kind, k))
            if not cell:
                continue
            for outlier in cell["outliers"]:
                entries.append((kind, k, role, run, outlier))
    if not entries:
        lines.append("无（阈值：单样本 ms > %g × 该 query 的 measured 中位数）。" % outlier_factor)
        return lines
    lines += ["| 场景 | k | 运行 | qid | repeat | ms | 占该格 measured 总耗时 | ×该 query 中位数 |",
              "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for kind, k, role, run, outlier in entries:
        share = "n/a" if outlier["share"] is None else "%.1f%%" % outlier["share"]
        cell = run["cells"].get((kind, k))
        lines.append("| %s | %d | %s（%s） | %s | %s | %s | %s | %.1f× |" % (
            md_cell(scene_label(kind, cell)), k, md_cell(role), md_cell(run["name"]),
            md_cell(outlier["qid"]), md_cell(outlier["repeat"]), fmt_ms(outlier["ms"]),
            md_cell(share), outlier["ratio"]))
    lines.append("")
    lines.append("这些样本不是测量误差诊断结论：p50 用中位数，不受单点影响；p95 是否被抬高取决于该样本"
                 "的排序位置（n=30 时只插值到第 27.55 位，最前面 2–3 个样本才进 p95），必须对着 p95 与"
                 "离群占比一起看，不能假定「p95 已包含它」。")
    return lines


def render_completeness(control, treatment, rows):
    lines = ["## 样本完整性与召回", "",
             "| 场景 | k | 运行 | measured | warmup | complete=false | recall_min | 窗口有效 | 说明 |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for kind, k in rows:
        for run, role in ((control, "对照组"), (treatment, "本方")):
            if not run:
                continue
            cell = run["cells"].get((kind, k))
            if cell is None:
                continue
            if cell["missing"]:
                note = "缺 " + "、".join(cell["missing"])
                lines.append("| %s | %d | %s | n/a | n/a | n/a | n/a | n/a | %s |" % (
                    md_cell(scene_label(kind, cell)), k, md_cell(role), md_cell(note)))
                continue
            if cell["recall_min"] is None:
                recall = "n/a（样本无 recall）"
            else:
                recall = "%.2f" % cell["recall_min"]
                if cell["recall_missing"]:
                    recall += "*"
            verdict = cell.get("verdict") or {}
            if "exclusive_window_valid" not in verdict:
                window = "n/a（无 verdict）"
            else:
                window = "true" if verdict.get("exclusive_window_valid") is True else "false"
            if cell["problems"]:
                note = "；".join(cell["problems"])
            elif cell["unavailable"]:
                note = cell["unavailable"]
            else:
                note = "—"
            lines.append("| %s | %d | %s | %d | %d | %d | %s | %s | %s |" % (
                md_cell(scene_label(kind, cell)), k, md_cell(role), cell["measured"],
                cell["warmup"], cell["failed"], md_cell(recall), md_cell(window), md_cell(note)))
    return lines


def contract_cells(run):
    contract = (run or {}).get("contract") or {}
    cells = [("single", int(k)) for k in (contract.get("single_ks") or [])]
    cells += [("batch", int(k)) for k in (contract.get("batch_ks") or [])]
    return cells


def render_gaps(control, treatment, rows):
    lines = ["## 缺数与口径说明", ""]
    gaps = []
    for run, role in ((control, "对照组"), (treatment, "本方")):
        if not run:
            continue
        expected = contract_cells(run)
        if not expected:
            continue
        missing = [identity for identity in expected if identity not in run["cells"]]
        if missing:
            gaps.append("- %s / %s：合同要求 %d 格，产物只有 %d 格；缺 %s（矩阵未跑完或未产出，"
                        "不能按完整矩阵解读）" % (
                            role, run["name"], len(expected), len(expected) - len(missing),
                            "、".join("%s-k%d" % identity for identity in missing)))
    for kind, k in rows:
        for run, role in ((control, "对照组"), (treatment, "本方")):
            if not run:
                continue
            cell = run["cells"].get((kind, k))
            label = "%s / %s k%d" % (role, run["name"], k)
            if cell is None:
                gaps.append("- %s：无此格目录（该 run 未跑这一格）" % label)
                continue
            if cell["unavailable"]:
                gaps.append("- %s：n/a，原因：%s" % (label, cell["unavailable"]))
            for problem in cell["problems"]:
                if problem == cell["unavailable"]:
                    continue
                gaps.append("- %s：%s" % (label, problem))
    if not gaps:
        lines.append("无（所有格 measured 完整、recall 齐全、verdict 有效）。")
    else:
        lines.extend(gaps)
    lines += ["", "口径提醒：",
              "- `n/a` 表示无数据或 harness 判定无效，绝不以 0 代替；`*` 表示该格 measured 不完整。",
              "- 比值按单元格 p50 计算（批量格用整批总耗时 p50；两侧 N 相同时与每查询比值等价）。",
              "- 本表是只读后处理，不重新测量；任何重新计算都必须换新 run 目录，而不是改本脚本的输入。"]
    return lines


def render_report(control, treatment, tool_sha, outlier_factor=OUTLIER_FACTOR):
    rows = union_cells(control, treatment)
    if control and treatment:
        title = "%s（对照组） vs %s（本方）" % (control["name"], treatment["name"])
        paths = "对照组 `%s` = `%s`；本方 `%s` = `%s`" % (
            control["name"], control["dir"], treatment["name"], treatment["dir"])
    elif control:
        title = "%s（仅对照组；未指定本方）" % control["name"]
        paths = "对照组 `%s` = `%s`；本方未指定" % (control["name"], control["dir"])
    else:
        title = "%s（仅本方；未指定对照组）" % treatment["name"]
        paths = "本方 `%s` = `%s`；对照组未指定" % (treatment["name"], treatment["dir"])
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = ["# 检索基准对照报告：%s" % title, "",
             "- 生成时间（UTC）：%s；工具：`scripts/benchmark/report.py` sha256 `%s`。" % (stamp, tool_sha),
             "- %s。" % paths,
             "- 单位 ms；单元格 `p50（p95）`，批量格 `总耗时（每查询耗时）`；"
             "`*` = 该格 measured 不完整，`n/a` = 无数据或 harness 判定无效（不以 0 代替）。",
             "- 本报告只读后处理既有产物，不提交作业、不改测量语义。", ""]
    lines += render_table(control, treatment, rows)
    lines.append("")
    lines += render_scale(control, treatment)
    lines.append("")
    lines += render_resources(control, treatment)
    lines.append("")
    lines += render_window(control, treatment)
    lines.append("")
    lines += render_cache(control, treatment)
    lines.append("")
    lines += render_outliers(control, treatment, rows, outlier_factor)
    lines.append("")
    lines += render_completeness(control, treatment, rows)
    lines.append("")
    lines += render_gaps(control, treatment, rows)
    lines.append("")
    return "\n".join(lines)


# ----------------------------------------------------------------------- cli

def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Aggregate benchmark matrix directories into the standard report table.")
    parser.add_argument("--run", action="append", required=True, metavar="[name=]dir",
                        help="matrix output directory; repeatable, optional explicit name")
    parser.add_argument("--baseline", metavar="name",
                        help="run name used as the control arm; required when several runs are given")
    parser.add_argument("--out", type=Path, help="also write the Markdown table to this file")
    parser.add_argument("--contract", type=Path,
                        help="contract JSON (default: run/contract.json, else baseline-10m.json)")
    parser.add_argument("--harness-commit", default=None,
                        help="record a harness commit when the run artifacts do not carry one")
    parser.add_argument("--skip-missing-cells", action="store_true",
                        help="render a cell with n/a instead of failing when samples/verdict are absent")
    parser.add_argument("--outlier-factor", type=float, default=OUTLIER_FACTOR,
                        help="flag a measured sample above N x its query median (default 5.0)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.outlier_factor <= 1.0:
        fail("--outlier-factor must be > 1")
    runs = []
    names = set()
    for spec in args.run:
        name, directory = split_run_spec(spec)
        if name in names:
            fail("duplicate run name %r; give each --run a unique name=dir" % name)
        names.add(name)
        runs.append(load_run(name, Path(directory), args))
    by_name = {run["name"]: run for run in runs}
    if args.baseline:
        if args.baseline not in by_name:
            fail("--baseline %r not among runs %s" % (args.baseline, sorted(by_name)))
        control = by_name[args.baseline]
        treatments = [run for run in runs if run["name"] != control["name"]]
        if not treatments:
            treatments = [None]
    elif len(runs) == 1:
        control, treatments = None, [runs[0]]
    else:
        fail("several runs need an explicit --baseline NAME; runs: %s" % sorted(by_name))
    tool_sha = sha256_file(Path(__file__))
    blocks = [render_report(control, treatment, tool_sha, args.outlier_factor)
              for treatment in treatments]
    markdown = "\n\n---\n\n".join(blocks)
    print(markdown)
    if args.out:
        try:
            Path(args.out).write_text(markdown + "\n")
        except OSError as exc:
            fail("cannot write --out %s (%s)" % (args.out, exc))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ReportError as exc:
        print("REFUSED: %s" % exc, file=sys.stderr)
        sys.exit(2)
