"""Key baseline invariants, runnable in parallel with only the Python standard library."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("baseline", HERE / "baseline.py")
baseline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(baseline)


def contract():
    return json.loads((HERE / "baseline-10m.json").read_text())


class ContractTests(unittest.TestCase):
    def test_five_cells_pin_results_and_parameters(self):
        cfg = contract()
        baseline.validate_contract(cfg)
        self.assertEqual(baseline.cells(cfg), [("single", 10), ("single", 100),
                                              ("single", 2000), ("batch", 100), ("batch", 2000)])
        for kind, k in baseline.cells(cfg):
            sql = baseline.sql(cfg, kind, k, [0.0] * 2560)
            self.assertIn("version => 56", sql)
            self.assertIn("index => 'r3-ivf-4b'", sql)
            self.assertIn("nprobes => 8", sql)
            self.assertIn("distance_type => 'l2'", sql)
            self.assertIn("vector_column => 'qwen_3_4b_embedding'", sql)
            self.assertNotIn("refine_factor", sql)
            self.assertNotIn("columns =>", sql)
            self.assertNotIn("count(", sql)
            self.assertNotIn("positions", sql)
            self.assertIn("doc_uuid, _rowid, _distance", sql)
            self.assertIn("with_row_id => true", sql)
            if kind == "batch":
                self.assertIn("query_table => '_baseline_q100'", sql)
                self.assertTrue(sql.startswith("SELECT query_id, doc_uuid"))

    def test_diagnostic_projection_is_not_main_cell(self):
        cfg = contract()
        main = baseline.sql(cfg, "single", 10, [0.0] * 2560)
        diagnostic = baseline.sql(cfg, "single", 10, [0.0] * 2560, diagnostic=True)
        self.assertNotIn("columns =>", main)
        self.assertIn("columns => array('doc_uuid')", diagnostic)

    def test_refuse_moving_image_bad_resources_and_refine_zero(self):
        for key, value in [("image_digest", "latest"), ("refine_factor", 0)]:
            cfg = contract(); cfg[key] = value
            with self.assertRaises(ValueError): baseline.validate_contract(cfg)
        cfg = contract(); cfg["resources"]["index_cache_bytes"] = 22 * 1024**3
        with self.assertRaises(ValueError): baseline.validate_contract(cfg)
        cfg = contract(); cfg["single_query_ids"] = [0, 0]
        with self.assertRaises(ValueError): baseline.validate_contract(cfg)

    def test_truth_requires_all_100_and_full_top2000(self):
        cfg = contract(); qs = [{"qid": i, "vector": [0.0] * 2560} for i in range(100)]
        truth = {"metadata": baseline.truth_identity(cfg, "queryhash"), "queries": [
            {"qid": i, "ids": [str(j) for j in range(2000)],
             "rowids": list(range(2000)), "distances": list(range(2000))} for i in range(100)]}
        baseline.validate_assets(cfg, qs, truth, "queryhash")
        for mutate in [lambda x: x["queries"].pop(),
                       lambda x: x["queries"][0].update(ids=["only-ten"] * 10),
                       lambda x: x["metadata"].update(version=55),
                       lambda x: x["metadata"].update(query_sha256="other")]:
            bad = copy.deepcopy(truth); mutate(bad)
            with self.assertRaises(ValueError): baseline.validate_assets(cfg, qs, bad, "queryhash")


class ResultsTests(unittest.TestCase):
    def test_true_batch_cardinality_and_query_ids(self):
        rows = [{"query_id": q, "doc_uuid": str(i), "_rowid": i, "_distance": float(i)}
                for q in range(100) for i in range(10)]
        baseline.validate_rows(rows, 100, 10)
        for bad in [rows[:-1], rows + [rows[0]],
                    [{**r, "query_id": 100} if i == 0 else r for i, r in enumerate(rows)]]:
            with self.assertRaises(ValueError): baseline.validate_rows(bad, 100, 10)

    def test_warmup_is_per_query_and_unknown_cache_is_not_residency(self):
        sample = {"warmup": False, "started_ms": 100, "ms": 10, "rows": 10,
                  "complete": True, "kind": "single", "k": 10, "qid": 0}
        with self.assertRaises(ValueError): baseline.validate_run([sample], eventlog_present=True)
        records = []
        for q in range(10):
            warm = {**sample, "warmup": True, "qid": q}
            records += [warm] + [{**sample, "qid": q, "repeat": r} for r in range(3)]
        verdict = baseline.validate_run(records, eventlog_present=True)
        self.assertEqual(verdict["cache_residency"], "unknown")
        with self.assertRaises(ValueError): baseline.validate_run(records, eventlog_present=False)
        bad = copy.deepcopy(records); bad[1]["qid"] = 1
        with self.assertRaises(ValueError): baseline.validate_run(bad, True)
        with self.assertRaises(ValueError): baseline.validate_run(records[:-1], True)
        with self.assertRaises(ValueError): baseline.validate_run([records[0], {**records[1], "complete": False}], True)

    def test_float32_query_hash_is_payload_not_json_formatting(self):
        first = [{"qid": 0, "vector": [0.1, 1.0]}]
        cast = [{"qid": 0, "vector": [0.10000000149011612, 1]}]
        self.assertEqual(baseline.query_float32_sha256(first), baseline.query_float32_sha256(cast))
        self.assertNotEqual(baseline.query_float32_sha256(first),
                            baseline.query_float32_sha256([{"qid": 1, "vector": [0.1, 1.0]}]))

    def test_matrix_requires_exactly_five_cells_and_rowid_is_entity(self):
        cfg = contract()
        groups = []
        for kind, k in baseline.cells(cfg):
            qids = range(10) if kind == "single" else ["batch"]
            records = []
            for q in qids:
                base = {"kind": kind, "k": k, "qid": q, "complete": True, "started_ms": 100, "ms": 1}
                records += [{**base, "warmup": True}] + [{**base, "warmup": False, "repeat": r} for r in range(3)]
            groups.append(records)
        baseline.validate_matrix([r for g in groups for r in g], True)
        with self.assertRaises(ValueError): baseline.validate_matrix(groups[0], True)
        bad = copy.deepcopy(groups[-1]);
        for r in bad: r["k"] = 10
        with self.assertRaises(ValueError): baseline.validate_matrix([r for g in groups[:-1] for r in g] + bad, True)
        same_doc = [{"query_id": 0, "doc_uuid": "same", "_rowid": r, "_distance": float(r)} for r in range(2)]
        baseline.validate_rows(same_doc, 1, 2)
        with self.assertRaises(ValueError): baseline.validate_rows([same_doc[0], same_doc[0]], 1, 2)

    def test_asset_cli_hash_is_canonical_and_not_json_bytes(self):
        cfg = contract(); qs = [{"qid": i, "vector": [0.1] * 2560} for i in range(100)]
        expected = baseline.query_float32_sha256(qs)
        truth = {"metadata": baseline.truth_identity(cfg, expected), "queries": [
            {"qid": i, "ids": ["same"] * 2000, "rowids": list(range(2000)), "distances": list(range(2000))}
            for i in range(100)]}
        baseline.validate_assets(cfg, qs, truth, expected)
        with tempfile.TemporaryDirectory() as root:
            q = Path(root) / "queries.jsonl"; t = Path(root) / "truth.json"
            q.write_text("\n".join(json.dumps(x, indent=None) for x in qs));t.write_text(json.dumps(truth))
            baseline.validate_asset_files(cfg, q, t)
            q.write_text("\n".join(json.dumps(x, separators=(",", ":")) for x in qs))
            baseline.validate_asset_files(cfg, q, t)
            qs[0]["vector"][0] = 0.2;q.write_text("\n".join(json.dumps(x) for x in qs))
            with self.assertRaises(ValueError): baseline.validate_asset_files(cfg, q, t)

    def test_prepared_matrix_has_one_true_batch_statement_and_safe_evidence(self):
        import benchmark
        import pilot
        cfg = contract(); plans = benchmark.matrix(cfg, "contract.json", "queries.parquet", "truth.json", "hdfs://run")
        self.assertEqual(len(plans), 5)
        self.assertEqual([p["samples"] for p in plans], [30, 30, 30, 3, 3])
        self.assertEqual(len({p["arguments"][3] for p in plans}), 5)
        pod = {"spec": {"containers": [{"env": [{"name": "AWS_SECRET_ACCESS_KEY", "value": "raw-secret"}],
                                            "args": ["spark.sql.catalog.lance.storage.access_key_id=raw-key"]}]}}
        self.assertNotIn("raw-secret", json.dumps(pilot.redact_json(pod)))
        self.assertNotIn("raw-key", json.dumps(pilot.redact_json(pod)))

    def test_window_ignores_terminal_and_uses_uid_not_name(self):
        own = {"uid": "ours", "name": "mine", "phase": "Running", "role": "executor"}
        old = {"uid": "old", "name": "same", "phase": "Succeeded", "role": "executor"}
        foreign = {"uid": "new", "name": "same", "phase": "Running", "role": "executor"}
        self.assertEqual(baseline.foreign_active([own, old], {"ours"}), [])
        self.assertEqual(baseline.foreign_active([own, old, foreign], {"ours"}), [foreign])

    def test_pod_gone_distinguished_from_transient_api_error(self):
        self.assertEqual(baseline.pod_state(None, api_ok=False), "API_UNAVAILABLE")
        self.assertEqual(baseline.pod_state(None, api_ok=True), "POD_GONE")
        self.assertEqual(baseline.pod_state({"status": {"phase": "Succeeded"}}, True), "SUCCEEDED")

    def test_ledger_cannot_claim_existing_or_delete_rebuilt_pod(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "created.json"
            baseline.record_created(path, "run-a", "spark", "pod-a", "uid-a")
            self.assertTrue(baseline.cleanup_allowed(path, "run-a", "spark", "pod-a", "uid-a"))
            self.assertFalse(baseline.cleanup_allowed(path, "run-a", "spark", "pod-a", "uid-b"))
            self.assertFalse(baseline.cleanup_allowed(path, "run-b", "spark", "pod-a", "uid-a"))
            with self.assertRaises(ValueError): baseline.record_created(path, "run-a", "spark", "pod-a", "uid-b")


if __name__ == "__main__":
    unittest.main()
