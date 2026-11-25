"""Text BM25 (k=10) sixth-cell invariants: frozen asset, submit wiring, report rows, dry-run.

Offline only: every cluster/docker/ssh/subprocess call is mocked or absent, and the only files
touched are temporary directories plus the frozen assets in this directory. The five vector
cells' behaviour is covered by the existing suites; this file only adds the optional text cell.
"""
import contextlib
import copy
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
import baseline
import submit
import text_asset
import test_baseline
import test_report

SPEC = importlib.util.spec_from_file_location("report", HERE / "report.py")
report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(report)

TEXT_CONTRACT = json.loads((HERE / "text-10m.json").read_text())
TEXT_ASSET = HERE / "text-queries-10m.jsonl"
TEXT_QUERIES = [json.loads(line)["query"]
                for line in TEXT_ASSET.read_text().splitlines() if line.strip()]
FROZEN_ASSET_SHA256 = "9d04605cc228cb9b7f9a9bc22a73ef9fd917daa0eb4d51bb808800ac660d3bc3"
HISTORICAL_SQL = ("SELECT doc_uuid, `_score` FROM lance.smoke.mm_fixed WHERE "
                  "lance_match(content, 'probability distribution') "
                  "ORDER BY `_score` DESC LIMIT 10")


def text_records(queries=None, ms=lambda qid, repeat: 10.0 + qid + repeat, sql=None):
    """80 records: one warmup + three measured repeats per frozen query."""
    queries = TEXT_QUERIES if queries is None else queries
    rows = []
    for qid, query in enumerate(queries):
        statement = sql if sql is not None else baseline.text_sql(TEXT_CONTRACT, query)
        base = {"kind": "text", "k": 10, "qid": qid, "started_ms": 1700000000000,
                "sql": statement, "query": query, "rows": 10,
                "cache_evidence": {"scope": "sql-executor-session",
                                   "residency_verified": False}}
        rows.append({**base, "warmup": True, "repeat": -1, "ms": 1.0, "complete": True,
                     "results": [{"doc_uuid": "d%d" % qid, "_score": 1.0}]})
        for repeat in range(3):
            rows.append({**base, "warmup": False, "repeat": repeat, "complete": True,
                         "ms": ms(qid, repeat),
                         "results": [{"doc_uuid": "d%d" % qid, "_score": 1.0}]})
    return rows


def write_text_run(root, cells=None, text_contract=None, asset_text=None, name="run",
                   contract_value=None):
    """A run root with the frozen text identity copied the way prepare_matrix copies it."""
    run = test_report.write_run(root, name=name, cells=cells,
                                contract_value=contract_value or test_baseline.contract())
    (run / "text-contract.json").write_text(
        json.dumps(text_contract if text_contract is not None else TEXT_CONTRACT))
    (run / "text-queries.jsonl").write_text(
        TEXT_ASSET.read_text() if asset_text is None else asset_text)
    return run


def render_report(*args):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = report.main(list(args))
    assert code == 0
    return buffer.getvalue()


class TextAssetTests(unittest.TestCase):
    """The frozen query set, its sha256 and the SQL template are the whole measurement identity."""

    def test_frozen_asset_has_twenty_ordered_unique_queries(self):
        rows = text_asset.read_asset(TEXT_ASSET)
        self.assertEqual([row["qid"] for row in rows], list(range(20)))
        self.assertEqual(len({row["query"] for row in rows}), 20)
        self.assertEqual(rows[0]["query"], "probability distribution")
        self.assertEqual(rows[-1]["query"], "probability density function")
        self.assertEqual(text_asset.check(TEXT_ASSET, HERE / "text-10m.json"),
                         FROZEN_ASSET_SHA256)
        self.assertEqual(TEXT_CONTRACT["queries_sha256"], FROZEN_ASSET_SHA256)

    def test_text_contract_and_sql_template_match_the_historical_statement(self):
        baseline.validate_text_contract(TEXT_CONTRACT)
        self.assertEqual(baseline.text_cells(), [("text", 10)])
        self.assertNotIn(("text", 10), baseline.cells(test_baseline.contract()))
        self.assertEqual(baseline.text_sql(TEXT_CONTRACT, TEXT_QUERIES[0]), HISTORICAL_SQL)
        self.assertEqual(baseline.text_expected_samples(TEXT_CONTRACT), 80)
        self.assertEqual(baseline.text_sql(TEXT_CONTRACT, "it's"),
                         TEXT_CONTRACT["sql_template"].replace("{query}", "it''s"))

    def test_extraction_reproduces_the_frozen_bytes(self):
        source = ['TEXTMETA {"queries":20}']
        for query in TEXT_QUERIES:
            source.append("TEXTQUERY " + json.dumps({"phase": "warmup", "query": query}))
            source.append("TEXTQUERY " + json.dumps({"sql": "x", "query": query}))
        extracted = text_asset.extract("\n".join(source))
        self.assertEqual(extracted, text_asset.read_asset(TEXT_ASSET))
        self.assertEqual(text_asset.render(extracted), TEXT_ASSET.read_text())
        with self.assertRaises(ValueError):
            text_asset.extract("\n".join(source[:5]))

    def test_tampered_asset_or_contract_is_refused(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            (root / "text-queries-10m.jsonl").write_text(TEXT_ASSET.read_text())
            contract = copy.deepcopy(TEXT_CONTRACT)
            contract["queries_sha256"] = "0" * 64
            (root / "text-10m.json").write_text(json.dumps(contract))
            with self.assertRaises(ValueError):
                baseline.load_text_asset(contract, root / "text-10m.json")
            (root / "text-10m.json").write_text(json.dumps(TEXT_CONTRACT))
            rows = (root / "text-queries-10m.jsonl").read_text().splitlines()
            rows[0], rows[1] = rows[1], rows[0]
            (root / "text-queries-10m.jsonl").write_text("\n".join(rows) + "\n")
            with self.assertRaises(ValueError):
                baseline.load_text_asset(TEXT_CONTRACT, root / "text-10m.json")
        for mutate in [lambda c: c.update(queries=19),
                       lambda c: c.update(k=100),
                       lambda c: c.update(index="other"),
                       lambda c: c.update(sql_template=c["sql_template"] + " "),
                       lambda c: c.update(result_columns=["doc_uuid"]),
                       lambda c: c.update(queries_asset="../escape.jsonl")]:
            broken = copy.deepcopy(TEXT_CONTRACT)
            mutate(broken)
            with self.assertRaises(ValueError):
                baseline.validate_text_contract(broken)

    def test_run_and_matrix_validation_accept_the_text_cell_only_when_declared(self):
        cfg = test_baseline.contract()
        groups = []
        for kind, k in baseline.cells(cfg):
            qids = range(10) if kind == "single" else ["batch"]
            records = []
            for qid in qids:
                base = {"kind": kind, "k": k, "qid": qid, "complete": True,
                        "started_ms": 100, "ms": 1}
                records += [{**base, "warmup": True}] + [
                    {**base, "warmup": False, "repeat": r} for r in range(3)]
            groups.append(records)
        records = [row for group in groups for row in group]
        text = text_records()
        baseline.validate_matrix(records, True)
        baseline.validate_run(text, True)
        baseline.validate_matrix(records + text, True, text=True)
        with self.assertRaises(ValueError):
            baseline.validate_matrix(records + text, True, text=False)
        with self.assertRaises(ValueError):
            baseline.validate_matrix(records, True, text=True)
        with self.assertRaises(ValueError):
            baseline.validate_run(text[:-4], True)
        with self.assertRaises(ValueError):
            baseline.validate_run([{**text[0], "k": 100}] + text[1:], True)


class TextSubmitTests(unittest.TestCase):
    """The text cell reuses the reviewed lifecycle; only the JVM payload and the report differ."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.contract = test_baseline.contract()

    def tearDown(self):
        self._tmp.cleanup()

    def valid_assets(self):
        """The frozen contract with a synthetic-but-consistent 100x2560 query/truth pair."""
        cfg = copy.deepcopy(self.contract)
        queries = [{"qid": qid, "vector": [0.1] * 2560} for qid in range(100)]
        query_hash = baseline.query_float32_sha256(queries)
        cfg["query_float32_sha256"] = query_hash
        truth = {"metadata": baseline.truth_identity(cfg, query_hash), "queries": [
            {"qid": qid, "ids": ["same"] * 2000, "rowids": list(range(2000)),
             "distances": list(range(2000))} for qid in range(100)]}
        contract_path = self.root / "contract.json"
        queries_path = self.root / "queries.jsonl"
        truth_path = self.root / "truth.json"
        contract_path.write_text(json.dumps(cfg))
        queries_path.write_text("\n".join(json.dumps(row) for row in queries))
        truth_path.write_text(json.dumps(truth))
        return cfg, contract_path, queries_path, truth_path

    def test_text_command_reuses_the_vector_conf_and_passes_five_arguments(self):
        text = {"remote_contract": "/e/text-contract.json", "remote_asset": "/e/text-queries.jsonl",
                "k": 10}
        conf, command = submit.text_cell_command(self.contract, text, "b.jar", "/e/text-k10",
                                                 "b3255-text-k10-x", {})
        vector_conf, vector_command = submit.cell_command(
            self.contract, "single", 10, "b.jar", "/e/contract.json", "/e/truth.json",
            "/e/single-k10", "b3255-single-k10-x", {})
        self.assertEqual(set(conf), set(vector_conf))
        self.assertEqual(conf["spark.executor.instances"], "12")
        self.assertEqual(command[0], vector_command[0])
        self.assertEqual(command[:6], vector_command[:6])
        self.assertEqual(command[7:9], vector_command[7:9])
        self.assertEqual(command[-5:], ["/e/text-contract.json", "/e/text-queries.jsonl",
                                        "/e/text-k10/samples.jsonl", "text", "10"])

    def test_prepare_matrix_validates_copies_and_uploads_the_frozen_text_identity(self):
        cfg, contract_path, queries_path, truth_path = self.valid_assets()
        calls = []

        def fake_call(argv, timeout=30, **kwargs):
            calls.append([str(item) for item in argv])
            return subprocess.CompletedProcess(argv, 0, "", "")

        output = self.root / "matrix"
        with mock.patch.object(submit, "platform_config", lambda: ({}, ())), \
                mock.patch.object(submit.pilot, "call", fake_call):
            ctx = submit.prepare_matrix(cfg, contract_path, queries_path, truth_path, output,
                                        text_contract_path=HERE / "text-10m.json")
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertIs(manifest["text_cell"], True)
        self.assertEqual(manifest["text_queries_sha256"], FROZEN_ASSET_SHA256)
        self.assertEqual(manifest["text_cell_detail"]["queries"], 20)
        self.assertEqual(manifest["text_cell_detail"]["warmup_passes"], 1)
        self.assertEqual(manifest["text_cell_detail"]["repeats"], 3)
        self.assertEqual((output / "text-queries.jsonl").read_text(), TEXT_ASSET.read_text())
        self.assertEqual(json.loads((output / "text-contract.json").read_text()), TEXT_CONTRACT)
        uploads = [call for call in calls if "dfs" in call and "-put" in call]
        self.assertEqual([Path(call[-2]).name for call in uploads],
                         ["contract.json", "truth.json", "text-10m.json",
                          "text-queries-10m.jsonl"])
        self.assertEqual(uploads[-2][-1], ctx["remote"] + "/text-contract.json")
        self.assertEqual(uploads[-1][-1], ctx["remote"] + "/text-queries.jsonl")
        self.assertEqual(ctx["text"]["remote_asset"], ctx["remote"] + "/text-queries.jsonl")

    def test_prepare_matrix_refuses_a_tampered_text_asset(self):
        cfg, contract_path, queries_path, truth_path = self.valid_assets()
        frozen = self.root / "frozen"
        frozen.mkdir()
        (frozen / "text-10m.json").write_text((HERE / "text-10m.json").read_text())
        (frozen / "text-queries-10m.jsonl").write_text(TEXT_ASSET.read_text() + "\n")
        with mock.patch.object(submit.pilot, "call", side_effect=AssertionError("no cluster")):
            with self.assertRaises(ValueError) as caught:
                submit.prepare_matrix(cfg, contract_path, queries_path, truth_path,
                                      self.root / "matrix-bad",
                                      text_contract_path=frozen / "text-10m.json")
        self.assertIn("text query asset changed", str(caught.exception))
        self.assertFalse((self.root / "matrix-bad").exists())

    def test_run_matrix_appends_text_and_counts_it_in_the_verdict(self):
        def prepare(c, contract_path, queries_path, truth_path, output, stopped_evidence=None,
                    text_contract_path=None):
            output = Path(output)
            output.mkdir(parents=True, exist_ok=False)
            return {"contract": c, "output": output, "remote": "/evidence/card3285",
                    "storage": {}, "secrets": (), "hdfs": "hdfs",
                    "jar": output / "BaselineBenchmark.jar",
                    "text": {"queries_sha256": FROZEN_ASSET_SHA256,
                             "contract": TEXT_CONTRACT, "k": 10}}

        calls = []

        def run_one_cell(ctx, kind, k, window_seconds):
            calls.append("%s-k%d" % (kind, k))
            verdict = {"kind": kind, "k": k, "window_usable": True,
                       "exclusive_window_valid": True, "baseline_valid": True, "coverage_ok": True,
                       "foreign_observed": [], "foreign_observed_total": 0, "contamination": None,
                       "window_wait_exhausted": False}
            if kind == "text":
                return text_records(), verdict
            rows = {("single", 10): test_report.single_cell(10),
                    ("single", 100): test_report.single_cell(100),
                    ("single", 2000): test_report.single_cell(2000),
                    ("batch", 100): test_report.batch_cell(100),
                    ("batch", 2000): test_report.batch_cell(2000)}[(kind, k)]
            return rows, verdict

        output = self.root / "matrix"
        with mock.patch.object(submit, "prepare_matrix",
                               side_effect=lambda *a, **k: prepare(*a, **k)), \
                mock.patch.object(submit, "run_one_cell", run_one_cell):
            submit.run_matrix(self.contract, "contract.json", "queries.jsonl", "truth.json",
                              output, text_cell=True, text_contract_path=HERE / "text-10m.json")
        verdict = json.loads((output / "matrix-verdict.json").read_text())
        self.assertEqual(calls, ["single-k10", "single-k100", "single-k2000", "batch-k100",
                                 "batch-k2000", "text-k10"])
        self.assertIs(verdict["complete"], True)
        self.assertIs(verdict["text_cell"], True)
        self.assertEqual(verdict["cells"], 6)
        self.assertEqual(verdict["samples"], 156)
        self.assertEqual(verdict["text_queries_sha256"], FROZEN_ASSET_SHA256)

    def test_run_matrix_requires_a_text_contract_when_enabled(self):
        with mock.patch.object(submit, "prepare_matrix",
                               side_effect=AssertionError("must refuse before preparing")):
            with self.assertRaises(ValueError):
                submit.run_matrix(self.contract, "contract.json", "queries.jsonl", "truth.json",
                                  self.root / "matrix", text_cell=True)

    def test_validate_expected_cell_requires_the_full_text_sample_set(self):
        summary = submit.validate_expected_cell(text_records(), "text", 10, True, TEXT_CONTRACT)
        self.assertEqual(summary["samples"], 60)
        for broken in (text_records()[:-1], text_records(queries=TEXT_QUERIES[:19])):
            with self.assertRaises(ValueError):
                submit.validate_expected_cell(broken, "text", 10, True, TEXT_CONTRACT)
        with self.assertRaises(ValueError):
            submit.validate_expected_cell(text_records(), "text", 10, True, None)


class TextDryRunTests(unittest.TestCase):
    """The dry-run must show the text cell's spark-submit plan and touch nothing external."""

    def dry_run(self, *extra):
        argv = ["benchmark.py", "--queries", "queries.jsonl", "--truth", "truth.json",
                "--output-root", "/tmp/unused"] + list(extra)
        buffer = io.StringIO()

        def forbidden(*_args, **_kwargs):
            raise AssertionError("dry-run must not run a subprocess")

        with mock.patch("subprocess.run", side_effect=forbidden), \
                mock.patch.object(submit.pilot, "call", side_effect=forbidden), \
                mock.patch.object(submit.pilot, "kube", side_effect=forbidden), \
                mock.patch("sys.argv", argv), contextlib.redirect_stdout(buffer):
            import benchmark
            benchmark.main()
        return json.loads(buffer.getvalue())

    def test_dry_run_prints_the_text_cell_spark_submit_plan(self):
        payload = self.dry_run("--text-cell")
        self.assertEqual(len(payload["cells"]), 5)
        text = payload["text_cell"]
        self.assertEqual((text["kind"], text["k"], text["cell_dir"]), ("text", 10, "text-k10"))
        self.assertEqual((text["queries"], text["samples"]), (20, 80))
        self.assertEqual(text["queries_sha256"], FROZEN_ASSET_SHA256)
        self.assertEqual(text["sql_examples"][0], HISTORICAL_SQL)
        self.assertEqual(text["spark_submit_command"][-2:], ["text", "10"])
        self.assertEqual(text["spark_submit_command"][-5], "<hdfs-run-root>/text-contract.json")
        plan = json.dumps(text["spark_submit_command"])
        self.assertIn("spark.kubernetes.driverEnv.AWS_SECRET_ACCESS_KEY=<redacted>", plan)
        self.assertIn("spark.executor.instances=12", plan)

    def test_default_dry_run_has_no_text_cell(self):
        payload = self.dry_run()
        self.assertEqual(len(payload["cells"]), 5)
        self.assertNotIn("text_cell", payload)


class TextFeatureRunTests(unittest.TestCase):
    """feature-run drives feature images through benchmark.py: it must forward --text-cell."""

    def test_feature_run_forwards_text_cell_only_when_requested(self):
        import test_feature
        fixture = test_feature.FeatureRunPlanTests(
            "test_plan_is_detached_create_only_and_never_deletes")
        fixture.setUp()
        try:
            default = fixture.plan()
            self.assertNotIn("--text-cell", default["launch_command"])
            payload = fixture.plan("--text-cell")
            self.assertIn("--text-cell", payload["launch_command"])
            self.assertIn("--run --approved", payload["launch_command"])
        finally:
            fixture.tearDown()


class TextReportTests(unittest.TestCase):
    """A text cell renders as 文本 BM25 k=10 with p50(p95) and verified frozen SQL."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def table_row(self, markdown):
        return [row for row in test_report.table_rows(markdown)
                if row.startswith("| 文本 BM25 | 10 |")]

    def test_text_row_is_rendered_last_with_p50_and_p95(self):
        run = write_text_run(self.root, {("single", 10): test_report.single_cell(10),
                                         ("text", 10): text_records()})
        markdown = render_report("--run", "r=%s" % run)
        rows = self.table_row(markdown)
        self.assertEqual(len(rows), 1)
        self.assertRegex(rows[0], r"^\| 文本 BM25 \| 10 \| n/a（未指定对照组） \| "
                                  r"20\.5（[0-9.]+） \| n/a \|$")
        self.assertEqual(rows[0], test_report.table_rows(markdown)[-1])
        self.assertIn("n/a（文本无真值）", markdown)
        self.assertNotIn("条 measured 样本无 recall", markdown)
        self.assertIn("文本 BM25 格是可选第六格", markdown)

    def test_text_sql_mismatch_is_reported_not_hidden(self):
        records = text_records()
        for row in records:
            if row.get("warmup") is False and row["qid"] == 3 and row["repeat"] == 1:
                row["sql"] = row["sql"].replace("linear algebra", "something else")
        run = write_text_run(self.root, {("single", 10): test_report.single_cell(10),
                                         ("text", 10): records})
        markdown = render_report("--run", "r=%s" % run)
        self.assertIn("文本 SQL 与冻结模板不一致（1/60 条）", markdown)

    def test_missing_text_identity_marks_the_cell_unverifiable(self):
        run = write_text_run(self.root, {("single", 10): test_report.single_cell(10),
                                         ("text", 10): text_records()})
        (run / "text-queries.jsonl").unlink()
        markdown = render_report("--run", "r=%s" % run)
        self.assertIn("文本格证据不完整：缺 text-queries.jsonl", markdown)
        self.assertTrue(self.table_row(markdown))

    def test_text_query_count_from_contract_is_enforced(self):
        contract_value = copy.deepcopy(TEXT_CONTRACT)
        contract_value["queries"] = 21
        run = write_text_run(self.root, {("single", 10): test_report.single_cell(10),
                                         ("text", 10): text_records()},
                             text_contract=contract_value)
        markdown = render_report("--run", "r=%s" % run)
        self.assertIn("文本格 measured 覆盖 20 条 query，合同要求 21 条", markdown)

    def test_run_without_text_cell_still_renders(self):
        run = write_text_run(self.root, {("single", 10): test_report.single_cell(10)})
        (run / "text-contract.json").unlink()
        (run / "text-queries.jsonl").unlink()
        markdown = render_report("--run", "r=%s" % run)
        self.assertEqual(self.table_row(markdown), [])
        self.assertEqual(len(test_report.table_rows(markdown)), 1)


if __name__ == "__main__":
    unittest.main()
