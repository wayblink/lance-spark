#!/usr/bin/env python3
"""Prepare a fixed matrix by default; explicit --run --approved submits the five sequential cells.

``--text-cell`` appends the optional text BM25 k=10 cell after batch-k2000; it is off by default
and never changes the five vector cells. The dry-run prints the text cell's validated frozen
identity and its redacted spark-submit command without any ssh/docker/kubectl/subprocess.
"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import baseline

HERE = Path(__file__).resolve().parent


def matrix(c, contract_path, queries_path, truth_path, output_root):
    baseline.validate_contract(c)
    return [{"kind":kind, "k":k, "arguments":[str(contract_path),str(queries_path),str(truth_path),
             output_root.rstrip("/")+f"/{kind}-k{k}.jsonl",kind,str(k)],
             "class":"BaselineBenchmark", "samples":30 if kind=="single" else 3,
             "warmup":10 if kind=="single" else 1,
             "result_contract":"query_id(batch only),doc_uuid,_rowid,_distance; no native columns",
             "timing":"spark.sql through collectAsList includes query_table analysis",
             "cache_residency":"unknown until same-executor-session cache evidence exists"}
            for kind,k in baseline.cells(c)]


def main():
    p=argparse.ArgumentParser(description=__doc__,
                              formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--contract",type=Path,default=HERE/"baseline-10m.json")
    p.add_argument("--queries",required=True);p.add_argument("--truth",required=True)
    p.add_argument("--output-root",required=True)
    p.add_argument("--run", action="store_true", help="explicit approved cluster mode; dry-run is default")
    p.add_argument("--approved", action="store_true")
    p.add_argument("--window-seconds", type=int, default=1800)
    p.add_argument("--stopped-evidence", type=Path,
                   help="UID-fenced JSON of drivers verified stopped before the run")
    p.add_argument("--text-cell", action="store_true",
                   help="append the optional text BM25 k=10 cell after the five vector cells")
    p.add_argument("--text-contract", type=Path, default=HERE/"text-10m.json",
                   help="frozen text cell contract (default scripts/benchmark/text-10m.json)")
    a=p.parse_args();c=json.loads(a.contract.read_text())
    if a.run:
        if not a.approved:
            p.error("--run requires --approved after root code review and user authorization")
        import submit
        if a.text_cell:
            submit.run_matrix(c, a.contract, Path(a.queries), Path(a.truth), Path(a.output_root),
                              a.window_seconds, a.stopped_evidence,
                              text_cell=True, text_contract_path=a.text_contract)
        else:
            submit.run_matrix(c, a.contract, Path(a.queries), Path(a.truth), Path(a.output_root),
                              a.window_seconds, a.stopped_evidence)
        return
    payload={"status":"MATRIX_PREPARED_NOT_SUBMITTED", "source":"BaselineBenchmark.java",
             "contract_sha256":baseline.sha256(a.contract),"spark_conf":baseline.spark_conf(c),
             "cells":matrix(c,a.contract,a.queries,a.truth,a.output_root),
             "gate":"Require validated full truth + independent live foreign activity window + reviewed timeout/UID lifecycle before submitting"}
    if a.text_cell:
        import submit
        payload["text_cell"]=submit.text_cell_plan(c, a.text_contract)
    print(json.dumps(payload,indent=2))


if __name__=="__main__":
    try:
        main()
    except ValueError as exc:
        print("REFUSED: %s" % exc, file=sys.stderr)
        sys.exit(2)
