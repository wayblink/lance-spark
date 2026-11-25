"""Interleaved pair runner: dry-run is a pure plan; execution reuses submit.run_one_cell."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import submit
import test_report

CONTRACT = HERE / "baseline-10m.json"


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pair_run = load("pair_run_module", "pair-run.py")


@contextlib.contextmanager
def no_execution():
    """Fail the test if anything tries to spawn a process while the block runs."""
    def boom(*args, **kwargs):
        raise AssertionError("unexpected process spawn: %r" % (args,))

    patchers = [mock.patch.object(subprocess, name, side_effect=boom)
                for name in ("run", "Popen", "call", "check_output", "check_call")]
    patchers.append(mock.patch.object(os, "system", side_effect=boom))
    with contextlib.ExitStack() as stack:
        for patcher in patchers:
            stack.enter_context(patcher)
        yield


def quiet(call, *args, **kwargs):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(io.StringIO()):
        result = call(*args, **kwargs)
    return result, buffer.getvalue()


class PairPlanTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.base = self.root / "base-contract.json"
        self.base.write_bytes(CONTRACT.read_bytes())
        self.feat = self.root / "feat-contract.json"
        feature = json.loads(CONTRACT.read_text())
        feature["image"] = "registry.example/lance-spark:feat-3284"
        feature["image_digest"] = "sha256:" + "c" * 64
        feature["feature_card"] = "3284"
        self.feat.write_text(json.dumps(feature))

    def tearDown(self):
        self._tmp.cleanup()

    def argv(self, *extra):
        return ["--arm", "base=%s" % self.base, "--arm", "feat=%s" % self.feat,
                "--queries", "/data/queries.jsonl", "--truth", "/data/truth.json",
                "--output-root", "/data/pair-3284"] + list(extra)

    def plan(self, *extra):
        with no_execution(), \
                mock.patch.object(submit, "prepare_matrix",
                                  side_effect=AssertionError("dry-run must not prepare")), \
                mock.patch.object(submit, "run_one_cell",
                                  side_effect=AssertionError("dry-run must not submit")):
            return pair_run.build_plan(pair_run.parse_args(self.argv(*extra)))

    def test_dry_run_prints_the_exact_round_cell_arm_order_without_any_subprocess(self):
        plan = self.plan("--cells", "single:10,batch:2000", "--repeats", "2")
        self.assertEqual([(entry["round"], entry["cell"], entry["arm"]) for entry in plan["schedule"]],
                         [(r, cell, arm) for r in (1, 2)
                          for cell in ("single-k10", "batch-k2000") for arm in ("base", "feat")])
        self.assertEqual(plan["status"], "DRY_RUN_ONLY")
        self.assertFalse(plan["parallel"])
        self.assertFalse(plan["deletes_remote_files"])
        self.assertFalse(plan["stops_foreign_jobs"])
        self.assertEqual(len(plan["commands"]), 8)
        for command in plan["commands"]:
            self.assertIn("spark-submit", command["command"])
            self.assertIn("--conf", command["command"])
            self.assertIn("BaselineBenchmark", command["command"])
        self.assertEqual(plan["commands"][0]["directory"],
                         "/data/pair-3284/base/r1/single-k10")
        self.assertEqual(plan["commands"][1]["directory"],
                         "/data/pair-3284/feat/r1/single-k10")
        self.assertIn("no subprocess", plan["dry_run_note"])

    def test_default_cells_are_the_frozen_five_and_the_round_loop_is_outermost(self):
        plan = self.plan("--repeats", "2")
        cells = [entry["cell"] for entry in plan["schedule"] if entry["round"] == 1]
        self.assertEqual(cells, [cell for cell in ("single-k10", "single-k100", "single-k2000",
                                                   "batch-k100", "batch-k2000")
                                 for _arm in ("base", "feat")])
        self.assertEqual([entry["round"] for entry in plan["schedule"][:10]], [1] * 10)
        self.assertEqual([entry["round"] for entry in plan["schedule"][10:]], [2] * 10)
        self.assertTrue(plan["warnings"])
        self.assertIn("noise", " ".join(plan["warnings"]))

    def test_identical_contracts_are_allowed_but_warned_as_an_a_a_run(self):
        plan = pair_run.build_plan(pair_run.parse_args(
            ["--arm", "a=%s" % self.base, "--arm", "b=%s" % self.base,
             "--queries", "/data/q", "--truth", "/data/t", "--output-root", "/data/pair-aa"]))
        self.assertIn("same image_digest", " ".join(plan["warnings"]))

    def test_at_least_two_distinct_arms_are_required(self):
        for extra in (["--arm", "only=%s" % self.base],
                      ["--arm", "same=%s" % self.base, "--arm", "same=%s" % self.feat]):
            with self.subTest(extra=extra):
                with self.assertRaises(ValueError):
                    pair_run.parse_args(["--queries", "/data/q", "--truth", "/data/t",
                                         "--output-root", "/data/pair"] + extra)

    def test_arms_that_change_frozen_parameters_are_refused(self):
        # "table" passes baseline.validate_contract but is not an accounted field, so only the
        # arm-vs-arm same-caliber check can refuse it.
        tampered = json.loads(CONTRACT.read_text())
        tampered["table"] = "t.db.other"
        path = self.root / "tampered.json"
        path.write_text(json.dumps(tampered))
        with self.assertRaises(ValueError) as caught:
            pair_run.build_plan(pair_run.parse_args(
                ["--arm", "base=%s" % self.base, "--arm", "bad=%s" % path,
                 "--queries", "/data/q", "--truth", "/data/t", "--output-root", "/data/pair"]))
        self.assertIn("table", str(caught.exception))

    def test_cells_outside_the_frozen_matrix_are_refused(self):
        for cells in ("single:7", "vector:10", "single:10,", "single-k10"):
            with self.subTest(cells=cells):
                with self.assertRaises(ValueError):
                    pair_run.parse_args(self.argv("--cells", cells))

    def test_run_requires_approved_and_a_fresh_output_root(self):
        with self.assertRaises(SystemExit):
            quiet(pair_run.main, self.argv("--run"))
        existing = self.root / "existing"
        existing.mkdir()
        args = pair_run.parse_args(["--queries", "/data/q", "--truth", "/data/t",
                                    "--output-root", str(existing),
                                    "--arm", "base=%s" % self.base,
                                    "--arm", "feat=%s" % self.feat, "--run", "--approved"])
        plan = pair_run.build_plan(args)
        with mock.patch.object(submit, "prepare_matrix",
                               side_effect=AssertionError("refused before prepare")):
            with self.assertRaises(ValueError) as caught:
                pair_run.execute(plan)
        self.assertIn("create-only", str(caught.exception))


class PairExecutionTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.base = self.root / "base.json"
        self.base.write_bytes(CONTRACT.read_bytes())
        self.feat = self.root / "feat.json"
        feature = json.loads(CONTRACT.read_text())
        feature["image"] = "registry.example/lance-spark:feat-3284"
        feature["image_digest"] = "sha256:" + "d" * 64
        self.feat.write_text(json.dumps(feature))
        self.prepared, self.submitted = [], []

    def tearDown(self):
        self._tmp.cleanup()

    def test_execute_keeps_the_printed_order_and_is_strictly_serial(self):
        def prepare(contract, contract_path, queries, truth, output, stopped_evidence=None):
            output = Path(output)
            output.mkdir(parents=True, exist_ok=True)
            self.prepared.append(str(output))
            return {"contract": contract, "output": output, "remote": "/evidence/pair",
                    "storage": {}, "secrets": (), "hdfs": "hdfs", "jar": output / "b.jar"}

        def run_one_cell(ctx, kind, k, window_seconds):
            self.submitted.append("%s/%s-k%d" % (Path(ctx["output"]).parent.name, kind, k))
            return [], {"kind": kind, "k": k, "window_usable": True,
                        "exclusive_window_valid": True, "contamination": None}

        args = pair_run.parse_args([
            "--arm", "base=%s" % self.base, "--arm", "feat=%s" % self.feat,
            "--cells", "single:10,batch:2000", "--repeats", "2",
            "--queries", "/data/q", "--truth", "/data/t",
            "--output-root", str(self.root / "pair")])
        plan = pair_run.build_plan(args)
        with mock.patch.object(submit, "prepare_matrix", prepare), \
                mock.patch.object(submit, "run_one_cell", run_one_cell):
            result = pair_run.execute(plan)
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(self.prepared,
                         [str(self.root / "pair" / arm / ("r%d" % index))
                          for index in (1, 2) for arm in ("base", "feat")])
        self.assertEqual(self.submitted,
                         ["%s/%s-k%d" % (arm, kind, k)
                          for _round in (1, 2)
                          for kind, k in (("single", 10), ("batch", 2000))
                          for arm in ("base", "feat")])
        report_text = (self.root / "pair" / "pair-report.md").read_text()
        self.assertIn("n/a", report_text)          # no samples were written by the stub
        document = json.loads((self.root / "pair" / "pair-verdict.json").read_text())
        self.assertEqual(document["status"], "COMPLETED")
        self.assertFalse(document["parallel"])
        self.assertFalse(document["deletes_remote_files"])
        self.assertEqual(len(document["schedule"]), 8)


class PairAggregationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.cells = [("single", 10)]

    def tearDown(self):
        self._tmp.cleanup()

    def write_arm(self, arm, values):
        for index, value in enumerate(values, 1):
            test_report.write_run(self.root, name="%s/r%d" % (arm, index),
                                  cells={("single", 10): test_report.single_cell(
                                      10, ms=lambda qid, repeat, value=value: value)})

    def test_paired_table_reports_per_arm_medians_and_round_dispersion(self):
        self.write_arm("base", (100.0, 110.0))
        self.write_arm("feat", (200.0, 210.0))
        contract = test_report.contract()
        collected = pair_run.collect_rounds(self.root, ["base", "feat"], self.cells, 2,
                                            {"base": contract, "feat": contract})
        text = "\n".join(pair_run.render_pair(collected, ["base", "feat"], self.cells))
        self.assertIn("| 向量单查询 | 10 | base | 100.0、110.0 | 105.0 |", text)
        self.assertIn("| 向量单查询 | 10 | feat | 200.0、210.0 | 205.0 |", text)
        self.assertIn("9.5%", text)
        self.assertIn("对照组快 1.95×（本方慢）", text)
        self.assertIn("R1 +100.0 ms", text)   # feat is slower; the signed delta shows the gap

    def test_a_missing_round_cell_is_rendered_na_and_never_zero(self):
        self.write_arm("base", (100.0, 110.0))
        self.write_arm("feat", (200.0,))
        contract = test_report.contract()
        collected = pair_run.collect_rounds(self.root, ["base", "feat"], self.cells, 2,
                                            {"base": contract, "feat": contract})
        text = "\n".join(pair_run.render_pair(collected, ["base", "feat"], self.cells))
        self.assertIn("n/a", text)
        self.assertNotIn("| 0.0 |", text)


if __name__ == "__main__":
    unittest.main()
