#!/usr/bin/env python3
"""Offline truth assembly and guards. This command never submits Spark or creates cluster objects."""
import argparse
import hashlib
import heapq
import json
import math
from pathlib import Path
import struct


def require(ok, message):
    if not ok:
        raise ValueError(message)


def fragment_hash(fragments):
    return hashlib.sha256("\n".join(map(str, sorted(fragments))).encode()).hexdigest()


def query_float32_hash(queries):
    require(len({q["qid"] for q in queries}) == len(queries), "duplicate query id")
    digest = hashlib.sha256()
    for query in sorted(queries, key=lambda q: q["qid"]):
        values = query["vector"]
        digest.update(struct.pack("<qi", query["qid"], len(values)))
        for value in values:
            require(value is not None and math.isfinite(float(value)), "non-finite query")
            encoded = struct.pack("<f", float(value))
            require(math.isfinite(struct.unpack("<f", encoded)[0]), "float32 query overflow")
            digest.update(encoded)
    return digest.hexdigest()


def merge_topk(streams, k, with_peak=False):
    """Bounded reference implementation, identity=rowid, never deduplicate by doc_uuid."""
    require(k > 0, "positive k required")
    heap, retained, peak = [], set(), 0
    for stream in streams:
        for distance, rowid, doc_id in stream:
            require(math.isfinite(float(distance)), "non-finite exact distance")
            require(isinstance(rowid, int) and rowid >= 0, "non-negative int64 rowid required")
            require(rowid not in retained, "duplicate retained row entity")
            require(doc_id is not None, "NULL document id")
            entry = (-distance, -rowid, str(doc_id))
            if len(heap) < k:
                heapq.heappush(heap, entry)
                retained.add(rowid)
            elif (distance, rowid) < (-heap[0][0], -heap[0][1]):
                removed = heapq.heapreplace(heap, entry)
                retained.remove(-removed[1])
                retained.add(rowid)
            peak = max(peak, len(heap))
    result = sorted((-d, -r, doc) for d, r, doc in heap)
    return (result, peak) if with_peak else result


def validate_block(block, metadata, fragments, query_ids):
    require(block.get("status") == "complete", "block not complete")
    require(block.get("metadata") == metadata, "truth identity mismatch")
    require(len(set(fragments)) == len(fragments), "expected fragment list has duplicates")
    seen = block.get("fragments", [])
    require(len(seen) == len(fragments) and len(set(seen)) == len(seen)
            and sorted(seen) == sorted(fragments), "incomplete or duplicate fragment coverage")
    require(block.get("fragment_sha256") == fragment_hash(fragments), "fragment fingerprint mismatch")
    require(block.get("query_ids") == query_ids, "block query range mismatch")
    queries = block.get("queries", [])
    require([q["qid"] for q in queries] == query_ids, "missing or duplicate query results")
    k = metadata["topk"]
    for query in queries:
        ids, rowids, distances = query["ids"], query["rowids"], query["distances"]
        require(len(ids) == len(rowids) == len(distances) == k, "incomplete top-k")
        require(len(set(rowids)) == k, "duplicate row entity")
        require(all(isinstance(r, int) and 0 <= r < 2**63 for r in rowids), "invalid signed rowid")
        require(all(doc is not None for doc in ids), "NULL document id")
        require(all(math.isfinite(float(d)) and d >= 0 for d in distances), "invalid exact L2 distance")
        order = list(zip(distances, rowids))
        require(order == sorted(order), "not ordered by distance,rowid")


def assemble(directory, metadata, fragments, block_size):
    directory = Path(directory)
    require(block_size > 0, "positive block size required")
    queries, provenance = [], []
    for start in range(0, metadata["query_count"], block_size):
        end = min(start + block_size, metadata["query_count"])
        path = directory / f"block-{start:03d}-{end - 1:03d}.json"
        checksum = path.with_suffix(".sha256")
        require(path.is_file() and checksum.is_file(), "block or checksum missing")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        require(checksum.read_text().strip() == digest, "block checksum mismatch")
        block = json.loads(path.read_text())
        validate_block(block, metadata, fragments, list(range(start, end)))
        queries.extend(block["queries"])
        provenance.append({"path": path.name, "sha256": digest, "fragments": len(fragments)})
    require([q["qid"] for q in queries] == list(range(metadata["query_count"])), "truth query coverage gap")
    return {"metadata": metadata, "queries": queries,
            "provenance": {"status": "complete", "blocks": provenance,
                           "fragment_sha256": fragment_hash(fragments), "block_size": block_size}}


def metadata_from_contract(contract):
    return {"dataset_uri": contract["dataset_uri"], "version": contract["version"],
            "rows": contract["rows"], "vector_column": contract["vector_column"],
            "dimension": contract["dimension"], "metric": contract["metric"],
            "query_sha256": contract["query_float32_sha256"],
            "query_part_sha256": contract["query_part_sha256"],
            "topk": 2000, "query_count": 100, "algorithm": "exact-bypass"}


def write_new(path, value):
    """Create-only publication. Never overwrite a previous run or a partial file."""
    with Path(path).open("x", encoding="utf-8") as out:
        out.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["dry-run", "assemble", "validate"])
    parser.add_argument("--contract", required=True, type=Path)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--block-size", default=10, type=int, choices=[10, 20])
    args = parser.parse_args()
    contract = json.loads(args.contract.read_text())
    metadata = metadata_from_contract(contract)
    require(metadata["version"] == 56 and metadata["rows"] == 10784907 and metadata["dimension"] == 2560,
            "frozen 10M contract changed")
    require(metadata["metric"] == "l2" and metadata["query_sha256"] ==
            "e60ccd2f1e2193e8d4c0fc25a4cdae7ac2ebe75621d7080a1cad65a888b4d95a",
            "query/metric identity changed")
    if args.command == "dry-run":
        print(json.dumps({"status": "DRY_RUN_ONLY", "metadata": metadata,
                          "query_part": contract["query_part"], "block_size": args.block_size,
                          "expected_fragments": 586, "topk": 2000, "requires_approval": True,
                          "spark_class": "TruthGenerator", "estimated_minutes": [30, 90],
                          "hard_deadline_minutes": 120}, indent=2))
        return
    require(args.directory, "--directory required")
    manifest = json.loads((args.directory / "run-manifest.json").read_text())
    require(manifest["metadata"] == metadata, "run manifest identity mismatch")
    require(manifest["block_size"] == args.block_size, "block size differs from run manifest")
    fragments = manifest["fragments"]
    require(len(fragments) == len(set(fragments)) == 586, "full 586-fragment manifest required")
    result = assemble(args.directory, metadata, fragments, args.block_size)
    if args.command == "assemble":
        require(args.output, "--output required (create-only)")
        write_new(args.output, result)
        digest = hashlib.sha256(args.output.read_bytes()).hexdigest()
        write_new(args.output.with_suffix(args.output.suffix + ".complete.json"),
                  {"status": "complete", "sha256": digest, "queries": 100, "topk": 2000})
    print(json.dumps({"status": "VERIFIED", "queries": len(result["queries"]),
                      "topk": 2000, "fragment_sha256": fragment_hash(fragments)}))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as exc:
        raise SystemExit("TRUTH_REFUSED: " + str(exc))
