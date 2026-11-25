#!/usr/bin/env python3
"""Prepare one immutable five-cell matrix. Submission intentionally requires later reviewed lifecycle."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import baseline


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
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--contract",type=Path,default=Path(__file__).with_name("baseline-10m.json"))
    p.add_argument("--queries",required=True);p.add_argument("--truth",required=True)
    p.add_argument("--output-root",required=True)
    a=p.parse_args();c=json.loads(a.contract.read_text())
    print(json.dumps({"status":"MATRIX_PREPARED_NOT_SUBMITTED", "source":"BaselineBenchmark.java",
                      "contract_sha256":baseline.sha256(a.contract),"spark_conf":baseline.spark_conf(c),
                      "cells":matrix(c,a.contract,a.queries,a.truth,a.output_root),
                      "gate":"Require validated full truth + independent live foreign activity window + reviewed timeout/UID lifecycle before submitting"},indent=2))


if __name__=="__main__":main()
