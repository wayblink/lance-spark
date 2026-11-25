#!/usr/bin/env python3
"""Extract, freeze and check the 10M text (BM25) query asset; offline, standard library only.

The frozen asset is ``text-queries-10m.jsonl`` (20 lines, ``qid`` 0..19 + ``query``, file order
is the qid order). It was extracted from the historical iceberg-tools/lance-sql run:

    host   shai
    source /home/wanganyang/runs/3233/10m/text/lance-sql/lance.jsonl
    sha256 01e51d73da360a3c0baa4b97a1b9407940295313d616ed1eb0884752dde63fc2

Each source line is ``<TAG> <json>``; only ``TEXTQUERY`` records carry ``query`` and the historical
run is cache=off, so these queries are used only as a fixed query set, never as a timing baseline.
``submit.prepare_matrix`` and ``BaselineBenchmark`` both refuse an asset whose bytes do not match
``text-10m.json.queries_sha256``; this module is the reproducible way to rebuild that file and to
re-check it later.

Usage:
  python3 scripts/benchmark/text_asset.py --source <lance.jsonl> --out text-queries-10m.jsonl
  python3 scripts/benchmark/text_asset.py --check --out text-queries-10m.jsonl \
      --contract scripts/benchmark/text-10m.json
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
DEFAULT_OUT = HERE / "text-queries-10m.jsonl"
DEFAULT_CONTRACT = HERE / "text-10m.json"
EXPECTED_QUERIES = 20


class AssetError(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise AssetError(message)


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def extract(text):
    """Return ``[{"qid": i, "query": q}]`` in first-appearance order, deduplicated."""
    frozen = []
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        tag, _, payload = line.partition(" ")
        if tag != "TEXTQUERY":
            continue
        try:
            record = json.loads(payload)
        except ValueError as exc:
            raise AssetError("%d: TEXTQUERY payload is not JSON (%s)" % (number, exc))
        query = record.get("query")
        require(isinstance(query, str) and query.strip() == query and query,
                "%d: TEXTQUERY record has no usable query" % number)
        require("\n" not in query and "\r" not in query, "%d: query contains a line break" % number)
        if query not in frozen:
            frozen.append(query)
    require(len(frozen) == EXPECTED_QUERIES,
            "expected %d distinct TEXTQUERY queries, found %d" % (EXPECTED_QUERIES, len(frozen)))
    return [{"qid": index, "query": query} for index, query in enumerate(frozen)]


def render(rows):
    """The exact frozen bytes: one compact JSON object per line, newline terminated."""
    return "".join(json.dumps({"qid": row["qid"], "query": row["query"]}) + "\n" for row in rows)


def read_asset(path):
    rows = []
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError as exc:
            raise AssetError("%s:%d: invalid JSON line (%s)" % (path, number, exc))
        require(isinstance(row, dict) and set(row) == {"qid", "query"},
                "%s:%d: expected exactly qid+query" % (path, number))
        require(isinstance(row["qid"], int) and not isinstance(row["qid"], bool),
                "%s:%d: qid must be an int" % (path, number))
        require(isinstance(row["query"], str) and row["query"] == row["query"].strip()
                and row["query"], "%s:%d: query must be a non-empty trimmed string" % (path, number))
        rows.append(row)
    require(len(rows) == EXPECTED_QUERIES,
            "%s: expected %d query lines, found %d" % (path, EXPECTED_QUERIES, len(rows)))
    require([row["qid"] for row in rows] == list(range(EXPECTED_QUERIES)),
            "%s: qids must appear in file order as 0..%d" % (path, EXPECTED_QUERIES - 1))
    require(len({row["query"] for row in rows}) == EXPECTED_QUERIES, "%s: duplicate query text" % path)
    return rows


def check(asset_path, contract_path):
    """Assert the asset bytes and text contract identity agree; returns the sha256."""
    contract = json.loads(Path(contract_path).read_text())
    actual = sha256_file(asset_path)
    require(actual == contract.get("queries_sha256"),
            "asset sha256 %s does not match text contract %s" % (actual, contract.get("queries_sha256")))
    require(Path(contract_path).with_name(contract["queries_asset"]).resolve()
            == Path(asset_path).resolve(), "text contract points at another query asset")
    rows = read_asset(asset_path)
    require(len(rows) == contract.get("queries"), "text contract query count mismatch")
    return actual


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, help="historical <TAG> <json> JSONL to extract")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--check", action="store_true",
                        help="verify --out against --contract instead of extracting")
    args = parser.parse_args(argv)
    if args.check:
        if not args.out.is_file():
            raise AssetError("asset not found: %s" % args.out)
        print(json.dumps({"status": "ASSET_OK", "asset": str(args.out),
                          "sha256": check(args.out, args.contract),
                          "queries": len(read_asset(args.out))}, indent=2))
        return 0
    require(args.source and args.source.is_file(), "--source is required and must exist")
    rows = extract(args.source.read_text())
    digest = hashlib.sha256(render(rows).encode()).hexdigest()
    if args.out.is_file():
        require(sha256_file(args.out) == digest,
                "existing asset differs from the extraction; rebuild it explicitly")
    else:
        args.out.write_text(render(rows))
    print(json.dumps({"status": "ASSET_FROZEN", "asset": str(args.out), "sha256": digest,
                      "source_sha256": sha256_file(args.source), "queries": len(rows)}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssetError as exc:
        print("REFUSED: %s" % exc, file=sys.stderr)
        sys.exit(2)
