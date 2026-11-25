"""Offline exact-truth invariants; no Spark, storage, or cluster side effects."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("truth", HERE / "truth.py")
truth = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(truth)


def identity():
    return {"dataset_uri": "s3://test/pinned", "version": 56, "rows": 8,
            "vector_column": "vector", "dimension": 2, "metric": "l2",
            "query_sha256": "f32hash", "query_part_sha256": "parquethash",
            "topk": 3, "query_count": 4, "algorithm": "exact-bypass"}


def block(start, end):
    return {"status": "complete", "metadata": identity(),
            "query_ids": list(range(start, end)), "fragments": [2, 7, 19],
            "fragment_sha256": truth.fragment_hash([2, 7, 19]),
            "queries": [{"qid": q, "ids": ["duplicate-doc", "duplicate-doc", "other"],
                         "rowids": [1, 2, 3], "distances": [0.0, 0.0, 3.0]}
                        for q in range(start, end)]}


class TopKTests(unittest.TestCase):
    def test_all_global_winners_may_come_from_one_fragment(self):
        streams = [[(float(i), i, "same-doc") for i in range(20)],
                   [(100.0 + i, 100 + i, "far") for i in range(20)]]
        self.assertEqual(truth.merge_topk(streams, 7), streams[0][:7])

    def test_ties_use_rowid_and_duplicate_doc_ids_stay_distinct(self):
        self.assertEqual(truth.merge_topk([[(1.0, 9, "d"), (1.0, 1, "d")],
                                         [(1.0, 3, "d"), (0.0, 8, "d")]], 3),
                         [(0.0, 8, "d"), (1.0, 1, "d"), (1.0, 3, "d")])

    def test_bounded_merge_matches_full_sort_and_never_retains_over_k(self):
        rnd = random.Random(3271)
        streams = [[(float(rnd.randrange(20)), i * 100 + j, "d") for j in range(100)]
                   for i in range(7)]
        result, peak = truth.merge_topk(streams, 11, with_peak=True)
        self.assertEqual(result, sorted(sum(streams, []))[:11])
        self.assertLessEqual(peak, 11)
        self.assertEqual(truth.merge_topk([truth.merge_topk([s], 11) for s in streams], 11), result)

    def test_nonfinite_distance_or_duplicate_entity_is_refused(self):
        for streams in [[[(float("nan"), 1, "x")]],
                        [[(0.0, 1, "x")], [(1.0, 1, "x")]]]:
            with self.assertRaises(ValueError): truth.merge_topk(streams, 3)


class AssetTests(unittest.TestCase):
    def test_float32_hash_is_qid_sorted_little_endian_and_rounding_stable(self):
        queries = [{"qid": 1, "vector": [1.0, 2.0]}, {"qid": 0, "vector": [0.1, -0.0]}]
        import struct
        expected = hashlib.sha256(b"".join(
            struct.pack("<qi", q["qid"], 2) + struct.pack("<ff", *q["vector"])
            for q in sorted(queries, key=lambda q: q["qid"]))).hexdigest()
        self.assertEqual(truth.query_float32_hash(queries), expected)
        self.assertEqual(truth.query_float32_hash(queries[::-1]), expected)
        bad = copy.deepcopy(queries); bad[1]["vector"][0] = float("inf")
        with self.assertRaises(ValueError): truth.query_float32_hash(bad)

    def test_full_coverage_and_identity_required_but_doc_ids_may_repeat(self):
        b = block(0, 2)
        truth.validate_block(b, identity(), [2, 7, 19], [0, 1])
        mutations = [lambda x: x.update(fragments=[2, 7]),
                     lambda x: x.update(fragments=[2, 7, 7, 19]),
                     lambda x: x.update(status="failed"),
                     lambda x: x["metadata"].update(version=55),
                     lambda x: x["queries"][0].update(rowids=[1, 1, 3]),
                     lambda x: x["queries"][0].update(rowids=[2, 1, 3]),
                     lambda x: x["queries"].pop()]
        for mutate in mutations:
            bad = copy.deepcopy(b); mutate(bad)
            with self.assertRaises(ValueError): truth.validate_block(bad, identity(), [2, 7, 19], [0, 1])

    def test_assembly_requires_every_block_hash_and_publishes_complete_last(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            for start, end in [(0, 2), (2, 4)]:
                path = root / f"block-{start:03d}-{end - 1:03d}.json"
                path.write_text(json.dumps(block(start, end)))
                path.with_suffix(".sha256").write_text(hashlib.sha256(path.read_bytes()).hexdigest() + "\n")
            result = truth.assemble(root, identity(), [2, 7, 19], 2)
            self.assertEqual([q["qid"] for q in result["queries"]], list(range(4)))
            self.assertEqual(result["metadata"], identity())
            (root / "block-002-003.sha256").write_text("0" * 64)
            with self.assertRaises(ValueError): truth.assemble(root, identity(), [2, 7, 19], 2)
            self.assertFalse((root / "complete.json").exists())


if __name__ == "__main__":
    import concurrent.futures
    def run(name):
        result = unittest.TestResult()
        unittest.defaultTestLoader.loadTestsFromName(name, module=__import__(__name__)).run(result)
        return name, result
    suite = unittest.defaultTestLoader.loadTestsFromModule(__import__(__name__))
    names = [test.id().split(".", 1)[1] for group in suite for test in group]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(run, names))
    failed = [(name, result) for name, result in results if not result.wasSuccessful()]
    for name, result in failed:
        for _, detail in result.errors + result.failures: print(name + "\n" + detail)
    print(f"{len(results) - len(failed)}/{len(results)} passed; workers=4")
    raise SystemExit(bool(failed))
