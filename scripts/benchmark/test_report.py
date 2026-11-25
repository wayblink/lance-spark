"""Invariants of the read-only report aggregator; synthetic evidence, no cluster work.

Runs under both runners used in this repo:
  python3 -m pytest scripts/benchmark/test_report.py -q
  python3 scripts/benchmark/run-tests.py
Only the standard library is imported, pytest just collects the TestCase classes.
"""

import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("report", HERE / "report.py")
report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(report)

IMAGE = "registry.example/sparkann:test@sha256:" + "0" * 64


def contract(**overrides):
    base = {
        "rows": 1000, "fragments": 4, "version": 7, "table": "t.db.tbl",
        "dataset_uri": "s3://bucket/t", "vector_column": "v", "id_column": "id",
        "dimension": 8, "metric": "l2", "index": "r3-ivf-4b", "index_segments": 2,
        "nprobes": 8, "refine_factor": None, "query_count": 100,
        "single_ks": [10, 100, 2000], "batch_ks": [100, 2000],
        "image_digest": "sha256:" + "0" * 64,
        "resources": {"executors": 4, "executor_cores": 2, "executor_heap": "8g",
                      "executor_overhead": "2g", "driver_cores": 2, "driver_heap": "4g",
                      "driver_overhead": "1g", "task_cpus": 1,
                      "index_cache_bytes": 1024 ** 3,
                      "metadata_cache_bytes": 512 * 1024 ** 2,
                      "aqe": True, "shuffle_partitions": 8},
    }
    base.update(overrides)
    return base


def spark_conf(**overrides):
    base = {
        "spark.executor.instances": "4", "spark.executor.cores": "2",
        "spark.executor.memory": "8g", "spark.executor.memoryOverhead": "2g",
        "spark.driver.cores": "2", "spark.driver.memory": "4g",
        "spark.driver.memoryOverhead": "1g", "spark.task.cpus": "1",
        "spark.sql.adaptive.enabled": "true", "spark.sql.shuffle.partitions": "8",
        "spark.sql.catalog.lance.index_cache_backend": "moka://?capacity=1073741824",
        "spark.sql.catalog.lance.metadata_cache_backend": "moka://?capacity=536870912",
        "spark.sql.lance.search.distributed.enabled": "true",
        "spark.kubernetes.container.image": IMAGE,
    }
    base.update(overrides)
    return base


def sql_for(kind, k, index="r3-ivf-4b", nprobes=8, version=7):
    query = ("query_table => 'q', query_id_column => 'qid', query_vector_column => 'vector'"
             if kind == "batch" else "query_vector => array(0.0)")
    return ("SELECT doc_uuid, _rowid, _distance FROM VECTOR_SEARCH(table => 't.db.tbl', %s, "
            "vector_column => 'v', index => '%s', version => %d, k => %d, nprobes => %d, "
            "distance_type => 'l2', with_row_id => true)" % (query, index, version, k, nprobes))


def record(kind, k, qid, repeat, ms, warmup=False, recall=None, complete=True, results=None):
    value = {"kind": kind, "k": k, "qid": "batch" if kind == "batch" else qid,
             "warmup": warmup, "repeat": -1 if warmup else repeat,
             "started_ms": 1700000000000, "ms": ms, "complete": complete,
             "rows": (100 if kind == "batch" else 1) * k,
             "cache_evidence": {"scope": "sql-executor-session", "residency_verified": False},
             "sql": sql_for(kind, k)}
    if complete:
        if results is None:
            results = {str(qid): [{"doc_uuid": "d", "_rowid": 1, "_distance": 0.0}]}
        value["results"] = results
        value["plan"] = "physical plan"
    if recall is not None:
        value["recall"] = recall
    return value


def single_cell(k=10, ms=lambda qid, repeat: 100 * (repeat + 1) + qid, recall=None):
    rows = []
    for qid in range(10):
        rows.append(record("single", k, qid, -1, 1.0, warmup=True))
        for repeat in range(3):
            rows.append(record("single", k, qid, repeat, ms(qid, repeat),
                               recall={str(qid): 0.9} if recall is None else recall))
    return rows


def batch_cell(k=100, values=(1000.0, 2000.0, 3000.0), with_results=True):
    payload = {str(qid): [] for qid in range(100)} if with_results else {}
    rows = [record("batch", k, 0, -1, 1.0, warmup=True, results=payload)]
    for repeat, value in enumerate(values):
        rows.append(record("batch", k, 0, repeat, value, results=payload,
                           recall={str(qid): 1.0 for qid in range(100)}))
    return rows


def write_run(root, name="run", cells=None, contract_value=None, manifest=None, conf=None,
              verdicts=None):
    run = Path(root) / name
    run.mkdir(parents=True, exist_ok=True)
    (run / "contract.json").write_text(json.dumps(contract_value or contract()))
    if manifest is not None:
        (run / "manifest.json").write_text(json.dumps(manifest))
    for (kind, k), rows in (cells or {}).items():
        cell = run / ("%s-k%d" % (kind, k))
        cell.mkdir(parents=True, exist_ok=True)
        with (cell / "samples.jsonl").open("w") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
        verdict = {"kind": kind, "k": k, "state": "SUCCEEDED", "baseline_valid": True,
                   "exclusive_window_valid": True, "done_marker_present": True,
                   "eventlog_present": True, "cache_residency": "unknown"}
        verdict.update((verdicts or {}).get((kind, k), {}))
        (cell / "verdict.json").write_text(json.dumps(verdict))
        (cell / "spark-conf.json").write_text(json.dumps(conf or spark_conf()))
        (cell / "activity.jsonl").write_text(json.dumps(
            {"at_ms": 1, "api_ok": True, "foreign": [],
             "classification": {"benchmark": 0, "background": 1, "unknown": 0}}) + "\n")
    return run


def table_rows(text):
    lines = text.splitlines()
    start = lines.index("| 场景 | k | 对照组数值 | 本方数值 | 比值 |") + 2
    rows = []
    for line in lines[start:]:
        if not line.startswith("|"):
            break
        rows.append(line)
    return rows


class ReportCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def run_report(self, *args):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = report.main(list(args))
        self.assertEqual(code, 0)
        return buffer.getvalue()


class StatisticsTests(ReportCase):
    def test_quantile_uses_linear_interpolation_not_nearest_rank(self):
        self.assertEqual(report.quantile_linear([10, 20, 30, 40], 0.95), 38.5)
        self.assertAlmostEqual(report.quantile_linear(list(range(1, 31)), 0.95), 28.55)
        self.assertEqual(report.quantile_linear([5.0], 0.95), 5.0)
        self.assertAlmostEqual(report.quantile_linear([1000, 2000, 3000], 0.95), 2900.0)
        # nearest-rank would answer 40 / 29 / 3000 here, so these cases discriminate.

    def test_single_cell_median_of_medians_and_p95_over_all_samples(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)})
        text = self.run_report("--run", "r=%s" % run)
        self.assertEqual(table_rows(text)[0],
                         "| 向量单查询 | 10 | n/a（未指定对照组） | 204.5（307.6） | n/a |")
        self.assertIn("| 向量单查询 | 10 | 本方 | 30 | 10 | 0 | 0.90 | true | — |", text)

    def test_batch_cell_reports_total_and_per_query(self):
        run = write_run(self.root, cells={("batch", 100): batch_cell(100)})
        text = self.run_report("--run", "r=%s" % run)
        self.assertEqual(table_rows(text)[0],
                         "| 批量 100 查询 | 100 | n/a（未指定对照组） | 2000.0（20.0） | n/a |")

    def test_batch_cell_without_results_falls_back_to_contract_query_count(self):
        run = write_run(self.root, cells={("batch", 100): batch_cell(100, with_results=False)})
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("| 批量 100 查询 | 100 | n/a（未指定对照组） | 2000.0（20.0） | n/a |", text)
        self.assertIn("样本缺 results，每查询按合同 query_count=100 折算", text)


class OutlierTests(ReportCase):
    def test_single_outlier_is_listed_with_share_and_kept_in_p50(self):
        grid = single_cell(10)
        for row in grid:
            if row["qid"] == 9 and row["repeat"] == 2 and not row["warmup"]:
                row["ms"] = 100000.0
        run = write_run(self.root, cells={("single", 10): grid})
        text = self.run_report("--run", "r=%s" % run)
        # p50 is untouched (median of per-qid medians); all 30 samples are still counted.
        self.assertIn("| 向量单查询 | 10 | n/a（未指定对照组） | 204.5（307.6） | n/a |", text)
        self.assertIn("| 向量单查询 | 10 | 本方 | 30 | 10 | 0 | 0.90 | true | — |", text)
        self.assertIn("| 向量单查询 | 10 | 本方（r） | 9 | 2 | 100000.0 | 94.5% | 478.5× |", text)

    def test_batch_outlier_raises_p95_but_not_p50(self):
        run = write_run(self.root, cells={
            ("batch", 100): batch_cell(100, values=(1000.0, 2000.0, 100000.0))})
        cell = report.compute_cell("batch", 100, run / "batch-k100", "r", contract(), False, 5.0)
        # p95 = 2000 + 0.9 * (100000 - 2000) = 90200; p50 stays the median of the 3 totals.
        self.assertAlmostEqual(cell["p50"], 2000.0)
        self.assertAlmostEqual(cell["p95"], 90200.0)
        self.assertAlmostEqual(cell["per_query"], (1000.0 + 2000.0 + 100000.0) / 3 / 100)
        self.assertEqual([(item["qid"], item["repeat"], item["ms"]) for item in cell["outliers"]],
                         [("batch", 2, 100000.0)])
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("| 批量 100 查询 | 100 | n/a（未指定对照组） | 2000.0（343.3） | n/a |", text)
        self.assertIn("| 批量 100 查询 | 100 | 本方（r） | batch | 2 | 100000.0 | 97.1% | 50.0× |", text)
        completeness = [line for line in text.splitlines()
                        if "| 批量 100 查询 | 100 | 本方 |" in line]
        self.assertTrue(completeness and "| 3 | 1 | 0 | 1.00 | true |" in completeness[0])
        self.assertIn("不剔除", text)


class RatioTests(ReportCase):
    def test_ratio_names_the_faster_arm_in_both_directions(self):
        slow = write_run(self.root, "slow",
                         cells={("single", 10): single_cell(10, ms=lambda q, r: 300.0)})
        fast = write_run(self.root, "fast",
                         cells={("single", 10): single_cell(10, ms=lambda q, r: 100.0)})
        text = self.run_report("--run", "fast=%s" % fast, "--run", "slow=%s" % slow,
                               "--baseline", "slow")
        self.assertTrue(table_rows(text)[0].endswith("| 本方快 3.00×（对照组慢） |"))
        text = self.run_report("--run", "fast=%s" % fast, "--run", "slow=%s" % slow,
                               "--baseline", "fast")
        self.assertTrue(table_rows(text)[0].endswith("| 对照组快 3.00×（本方慢） |"))
        same = write_run(self.root, "same",
                         cells={("single", 10): single_cell(10, ms=lambda q, r: 100.0)})
        text = self.run_report("--run", "fast=%s" % fast, "--run", "same=%s" % same,
                               "--baseline", "same")
        self.assertTrue(table_rows(text)[0].endswith("| 持平 |"))


class MissingDataTests(ReportCase):
    def test_missing_recall_is_na_and_never_zero(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10, recall={})})
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("| 向量单查询 | 10 | 本方 | 30 | 10 | 0 | n/a（样本无 recall） | true |", text)
        self.assertNotIn("| 1.00 |", text)
        self.assertNotIn("| 0.00 |", text)

    def test_missing_verdict_refuses_and_names_the_file(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)})
        (run / "single-k10" / "verdict.json").unlink()
        with self.assertRaises(report.ReportError) as caught:
            report.main(["--run", "r=%s" % run])
        self.assertIn("verdict.json", str(caught.exception))
        self.assertIn("single-k10", str(caught.exception))

    def test_cli_exits_2_on_refusal(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)})
        (run / "single-k10" / "samples.jsonl").unlink()
        result = subprocess.run([sys.executable, str(HERE / "report.py"), "--run", str(run)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("REFUSED:", result.stderr)
        self.assertIn("samples.jsonl", result.stderr)

    def test_skip_missing_cells_renders_na_with_reason(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)})
        (run / "single-k100").mkdir()
        text = self.run_report("--run", "r=%s" % run, "--skip-missing-cells")
        self.assertIn("| 向量单查询 | 100 | n/a（未指定对照组） | n/a（缺 samples.jsonl、verdict.json） | n/a |",
                      text)

    def test_directory_name_must_match_sample_kind(self):
        run = write_run(self.root, cells={("batch", 100): batch_cell(100)})
        (run / "batch-k100" / "samples.jsonl").write_text(json.dumps({
            "kind": "single", "k": 10, "qid": 0, "warmup": False, "repeat": 0,
            "ms": 1.0, "complete": True}) + "\n")
        with self.assertRaises(report.ReportError) as caught:
            report.main(["--run", "r=%s" % run])
        self.assertIn("does not match directory name", str(caught.exception))

    def test_measured_without_warmup_is_refused(self):
        rows = [record("single", 10, 0, 0, 10.0)]
        run = write_run(self.root, cells={("single", 10): rows})
        with self.assertRaises(report.ReportError) as caught:
            report.main(["--run", "r=%s" % run])
        self.assertIn("no preceding warmup", str(caught.exception))

    def test_measured_repeat_outside_012_is_refused(self):
        for repeat in (3, -1, 0.5):
            with self.subTest(repeat=repeat):
                rows = [record("single", 10, 0, -1, 1.0, warmup=True),
                        record("single", 10, 0, repeat, 10.0)]
                run = write_run(self.root, "repeat%s" % repeat,
                                cells={("single", 10): rows})
                with self.assertRaises(report.ReportError) as caught:
                    report.main(["--run", "r=%s" % run])
                self.assertIn("outside {0, 1, 2}", str(caught.exception))

    def test_complete_false_cell_is_na_but_counted(self):
        rows = [row for row in single_cell(10)
                if not (row["qid"] == 9 and row["repeat"] == 2 and not row["warmup"])]
        rows.append(record("single", 10, 9, 2, 10.0, complete=False))
        run = write_run(self.root, cells={("single", 10): rows})
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("| 向量单查询 | 10 | n/a（未指定对照组） | n/a（1 条 complete=false） | n/a |", text)
        self.assertIn("| 向量单查询 | 10 | 本方 | 29 | 10 | 1 | 0.90 |", text)

    def test_invalid_verdict_cell_is_na_and_ratio_is_na(self):
        good = write_run(self.root, "good", cells={("single", 10): single_cell(10)})
        bad = write_run(self.root, "bad",
                        cells={("single", 10): single_cell(10, ms=lambda q, r: 100.0)},
                        verdicts={("single", 10): {"baseline_valid": False,
                                                   "exclusive_window_valid": False}})
        text = self.run_report("--run", "bad=%s" % bad, "--run", "good=%s" % good,
                               "--baseline", "bad")
        row = table_rows(text)[0]
        self.assertIn("n/a（verdict.baseline_valid=false：exclusive_window_valid=false", row)
        self.assertTrue(row.endswith("| n/a |"))
        self.assertIn("窗口有效 0/1 格", text)

    def test_missing_repeat_marks_cell_incomplete(self):
        rows = [row for row in single_cell(10)
                if not (row["qid"] == 3 and row["repeat"] == 2 and not row["warmup"])]
        run = write_run(self.root, cells={("single", 10): rows})
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("| 向量单查询 | 10 | n/a（未指定对照组） | 204.5（307.6）* | n/a |", text)
        self.assertIn("repeat 不足 3 个的 identity：3", text)


class ResourceTests(ReportCase):
    def test_resource_misalignment_reports_direction(self):
        baseline = write_run(self.root, "base", cells={("single", 10): single_cell(10)})
        treatment = write_run(self.root, "feat", cells={("single", 10): single_cell(10)},
                              conf=spark_conf(**{"spark.executor.instances": "2"}))
        text = self.run_report("--run", "base=%s" % baseline, "--run", "feat=%s" % treatment,
                               "--baseline", "base")
        self.assertIn("| executor 数 | 4 | 2 | 本方更少 → 本方数字是保守下界 |", text)
        self.assertIn("不对齐项：executor 数", text)

    def test_contract_fingerprint_mismatch_is_flagged(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)},
                        manifest={"contract_sha256": "sha256:" + "f" * 64})
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("⚠ sha256 与 manifest.contract_sha256 不一致", text)

    def test_sql_that_ignores_contract_is_flagged(self):
        rows = single_cell(10)
        for row in rows:
            row["sql"] = sql_for("single", 10, nprobes=64)
        run = write_run(self.root, cells={("single", 10): rows})
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("SQL 与合同索引参数不一致（30/30 条）", text)


class OutputTests(ReportCase):
    def test_markdown_header_sections_and_order(self):
        baseline = write_run(self.root, "base", cells={("single", 10): single_cell(10),
                                                       ("batch", 100): batch_cell(100)})
        treatment = write_run(self.root, "feat", cells={("single", 10): single_cell(10),
                                                        ("batch", 100): batch_cell(100)})
        text = self.run_report("--run", "base=%s" % baseline, "--run", "feat=%s" % treatment,
                               "--baseline", "base")
        lines = text.splitlines()
        header = lines.index("| 场景 | k | 对照组数值 | 本方数值 | 比值 |")
        self.assertEqual(lines[header + 1], "| --- | --- | --- | --- | --- |")
        self.assertTrue(lines[header + 2].startswith("| 向量单查询 | 10 |"))
        self.assertTrue(lines[header + 3].startswith("| 批量 100 查询 | 100 |"))
        for row in table_rows(text):
            self.assertEqual(len(row.strip("|").split(" | ")), 5)
        sections = ["## 数据规模与索引参数", "## 资源、镜像与 harness", "## 窗口口径",
                    "## 缓存口径", "## 离群样本（不剔除，仍计入 p50/p95）",
                    "## 样本完整性与召回", "## 缺数与口径说明"]
        positions = [text.index(section) for section in sections]
        self.assertEqual(positions, sorted(positions))
        self.assertLess(text.index("| 场景 | k | 对照组数值 | 本方数值 | 比值 |"),
                        text.index(sections[0]))
        for required in ("1000", "fragments", "版本", "索引名", "nprobes", "refine_factor",
                         "harness commit", "warm-session", "native index-cache residency"):
            self.assertIn(required, text)

    def test_partial_matrix_is_flagged_as_not_a_full_matrix(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)})
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("合同要求 5 格，产物只有 1 格；缺 single-k100、single-k2000、"
                      "batch-k100、batch-k2000（矩阵未跑完或未产出，不能按完整矩阵解读）", text)

    def test_out_file_matches_stdout(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)})
        out = self.root / "table.md"
        text = self.run_report("--run", "r=%s" % run, "--out", str(out))
        self.assertTrue(out.is_file())
        self.assertEqual(out.read_text().rstrip("\n"), text.rstrip("\n"))


if __name__ == "__main__":
    unittest.main()


class WindowFieldTests(ReportCase):
    """Old verdicts have no window fields; contaminated-but-usable cells keep their numbers."""

    def contaminated_verdict(self, **overrides):
        verdict = {"window_usable": True, "coverage_ok": True, "exclusive_window_valid": False,
                   "baseline_valid": False, "contamination": "foreign_benchmark",
                   "foreign_observed": [{"app": "app-text-bench", "name": "text-bench-iceberg-1",
                                         "role": "executor", "node": "cpu-0149",
                                         "category": "benchmark", "first_at_ms": 1,
                                         "last_at_ms": 2}]}
        verdict.update(overrides)
        return verdict

    def test_legacy_verdict_without_window_fields_is_tolerated_and_reported(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)})
        text = self.run_report("--run", "r=%s" % run)
        self.assertIn("窗口有效 1/1 格", text)
        self.assertIn("未记录窗口字段", text)
        # The numbers of a legacy cell are still reported.
        self.assertEqual(table_rows(text)[0],
                         "| 向量单查询 | 10 | n/a（未指定对照组） | 204.5（307.6） | n/a |")

    def test_contaminated_but_usable_cell_keeps_its_numbers_and_is_flagged(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)},
                        verdicts={("single", 10): self.contaminated_verdict()})
        text = self.run_report("--run", "r=%s" % run)
        self.assertEqual(table_rows(text)[0],
                         "| 向量单查询 | 10 | n/a（未指定对照组） | 204.5（307.6） | n/a |")
        self.assertIn("窗口可用 1/1 格（其中无外来污染 0 格、记录到外来污染但可用 1 格）", text)
        self.assertIn("可用（有外来）", text)
        self.assertIn("外来 benchmark 污染", text)

    def test_window_usable_false_marks_the_cell_na_with_the_reason(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)},
                        verdicts={("single", 10): {"window_usable": False, "coverage_ok": True,
                                                   "exclusive_window_valid": False,
                                                   "baseline_valid": False,
                                                   "contamination": "api_failure"}})
        text = self.run_report("--run", "r=%s" % run)
        row = table_rows(text)[0]
        self.assertIn("n/a（verdict.window_usable=false", row)
        self.assertIn("contamination=api_failure", row)
        self.assertIn("窗口可用 0/1 格", text)

    def test_coverage_hole_reason_never_claims_contamination(self):
        run = write_run(self.root, cells={("single", 10): single_cell(10)},
                        verdicts={("single", 10): {"window_usable": False, "coverage_ok": False,
                                                   "exclusive_window_valid": False,
                                                   "baseline_valid": False,
                                                   "contamination": None}})
        text = self.run_report("--run", "r=%s" % run)
        row = table_rows(text)[0]
        self.assertIn("n/a（verdict.window_usable=false", row)
        self.assertIn("覆盖不足", row)
        self.assertNotIn("contamination=foreign_benchmark", row)
