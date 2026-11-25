"""Formal submission gates: activity is keyed by app/UID, never a pod name prefix."""
import copy
import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import submit
import test_report

HERE = Path(__file__).resolve().parent
FROZEN_CONTRACT = json.loads((HERE / "baseline-10m.json").read_text())


class SubmitTests(unittest.TestCase):
    def test_terminal_ignored_executor_without_bench_prefix_is_foreign(self):
        pod = {"metadata": {"name": "random-exec", "uid": "u", "labels": {"spark-role": "executor", "spark-app-selector": "foreign"}},
               "status": {"phase": "Running"}, "spec": {"nodeName": "n"}}
        self.assertEqual(submit.activity([pod], "ours")[0]["uid"], "u")
        pod["status"]["phase"] = "Succeeded"
        self.assertEqual(submit.activity([pod], "ours"), [])
        pod["status"]["phase"] = "Running"; pod["metadata"]["labels"]["spark-app-selector"] = "ours"
        self.assertEqual(submit.activity([pod], "ours"), [])

    def test_unknown_app_blocks_and_background_is_recorded_only(self):
        pods = [
            {"metadata": {"name": "spark-exec-x", "uid": "u1", "labels": {"spark-role": "executor", "spark-app-selector": "mystery"}},
             "status": {"phase": "Running"}, "spec": {"nodeName": "n1"}},
            {"metadata": {"name": "kyuubi-engine-x", "uid": "u2", "labels": {"spark-role": "driver", "spark-app-selector": "svc"}},
             "status": {"phase": "Running"}, "spec": {"nodeName": "n2"}},
        ]
        details = submit.activity_details(pods)
        self.assertEqual({x["category"] for x in details}, {"unknown", "background"})
        self.assertEqual([x["uid"] for x in submit.activity(pods)], ["u1"])
        self.assertEqual(submit.schedule()["spark.kubernetes.driver.node.selector.kubernetes.io/hostname"], "cpu-1122.host.platform.shaipower.com")

    def test_unknown_api_or_dynamic_overlap_invalidates_whole_cell(self):
        sample = {"warmup": False, "started_ms": 1000, "ms": 500}
        clean = [{"at_ms": 900, "api_ok": True, "foreign": []}, {"at_ms": 1600, "api_ok": True, "foreign": []}]
        self.assertTrue(submit.window_valid([sample], clean))
        self.assertFalse(submit.window_valid([sample], [{"at_ms": 1100, "api_ok": False, "foreign": []}]))
        self.assertFalse(submit.window_valid([sample], [{"at_ms": 1100, "api_ok": True, "foreign": [{"uid": "external"}]}]))
        self.assertFalse(submit.window_valid([sample], []))

    def test_window_relevance_filter_matches_gap_tolerance(self):
        """Preceding observation 20.1s before a sample start is inside the 35s tolerance."""
        observations=[{"at_ms":base,"api_ok":True,"foreign":[]} for base in
                      (80000,102900,125800)]
        sample={"warmup":False,"started_ms":100100,"ms":5190}
        self.assertTrue(submit.window_valid([sample],observations))
        # Removing the middle poll leaves a 45.8s hole: the same sample must fail.
        self.assertFalse(submit.window_valid([sample],[observations[0],observations[2]]))

    def test_wrong_cell_samples_cannot_pass_expected_cell(self):
        rows=[]
        for q in range(10):
            base={"kind":"single","k":100,"qid":q,"complete":True,"started_ms":100,"ms":1}
            rows += [{**base,"warmup":True}] + [{**base,"warmup":False,"repeat":r} for r in range(3)]
        submit.validate_expected_cell(rows,"single",100,True)
        with self.assertRaises(ValueError): submit.validate_expected_cell(rows,"single",10,True)

    def test_evidence_timeout_does_not_prevent_failure_verdict(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root);errors={}
            def timeout():raise subprocess.TimeoutExpired("kubectl",1)
            self.assertIsNone(submit.capture("logs",timeout,errors))
            submit.write_verdict(p,{"state":"ORIGINAL_FAILED","baseline_valid":True},errors,())
            v=json.loads((p/"verdict.json").read_text())
            self.assertFalse(v["baseline_valid"])
            self.assertEqual(v["state"],"ORIGINAL_FAILED")
            self.assertEqual(v["evidence_errors"]["logs"],"TimeoutExpired")

    def test_verified_stopped_driver_is_uid_fenced_and_executor_revokes_exemption(self):
        stopped_uid = "stopped-u"
        driver = {"metadata": {"name": "tpcds-old-driver", "uid": stopped_uid,
                               "labels": {"spark-role": "driver", "spark-app-selector": "old-app"}},
                  "status": {"phase": "Running"}, "spec": {"nodeName": "old-node"}}
        old = dict(submit.VERIFIED_STOPPED_DRIVERS)
        try:
            submit.VERIFIED_STOPPED_DRIVERS[stopped_uid] = {
                "app": "old-app", "evidence": "no executors; UI refused; SparkContext stopped"}
            details = submit.activity_details([driver])
            self.assertEqual(details[0]["category"], "stopped")
            self.assertEqual(submit.activity([driver]), [])
            replacement = {**driver, "metadata": {**driver["metadata"], "uid": "new-u"}}
            self.assertEqual(submit.activity_details([replacement])[0]["category"], "benchmark")
            executor = {"metadata": {"name": "unrelated-name", "uid": "exec-u",
                                     "labels": {"spark-role": "executor", "spark-app-selector": "old-app"}},
                        "status": {"phase": "Running"}, "spec": {"nodeName": "new-node"}}
            self.assertEqual({x["category"] for x in submit.activity_details([driver, executor])}, {"benchmark"})
        finally:
            submit.VERIFIED_STOPPED_DRIVERS.clear()
            submit.VERIFIED_STOPPED_DRIVERS.update(old)

    def test_verified_driver_node_label_is_not_silently_changed(self):
        self.assertEqual(submit.schedule()["spark.kubernetes.driver.node.selector.kubernetes.io/hostname"],
                         "cpu-1122.host.platform.shaipower.com")

    def test_benchmark_cli_forwards_stopped_evidence(self):
        import benchmark
        from unittest import mock
        captured = {}

        def fake_run(c, contract, queries, truth, output, window_seconds=1800, stopped_evidence=None):
            captured["stopped_evidence"] = stopped_evidence
            captured["window_seconds"] = window_seconds

        argv = ["benchmark.py", "--run", "--approved",
                "--contract", str(Path(__file__).with_name("baseline-10m.json")),
                "--queries", "queries.jsonl", "--truth", "truth.json",
                "--output-root", "/tmp/unused", "--window-seconds", "1800",
                "--stopped-evidence", "verified-stopped-drivers.json"]
        with mock.patch.object(submit, "run_matrix", fake_run), mock.patch("sys.argv", argv):
            benchmark.main()
        self.assertEqual(Path(captured["stopped_evidence"]).name, "verified-stopped-drivers.json")
        self.assertEqual(captured["window_seconds"], 1800)

    def test_schedule_is_frozen_and_no_existing_pod_delete_command(self):
        self.assertEqual(submit.schedule(), {"spark.kubernetes.driver.node.selector.kubernetes.io/hostname": "cpu-1122.host.platform.shaipower.com"})
        options = submit.delete_options("u1")
        self.assertEqual(options["preconditions"]["uid"], "u1")
        self.assertNotIn("labelSelector", options)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------------------------
# 2026-11-25 window policy: foreign benchmark activity is recorded, never used to void a cell.
# Coverage and API health keep their old, strict meaning.
# ---------------------------------------------------------------------------------------------

FOREIGN_POD = {"uid": "foreign-u1", "name": "text-bench-iceberg-1", "app": "app-text-bench",
               "role": "executor", "phase": "Running", "node": "cpu-0149",
               "category": "benchmark"}


class WindowPolicyTests(unittest.TestCase):
    """cell_window is the single decision point: usable vs merely clean."""

    def sample(self):
        # 100.1 s .. 105.29 s; the 35 s tolerance governs relevance and hole size.
        return {"warmup": False, "started_ms": 100100, "ms": 5190}

    def clean_observations(self):
        return [{"at_ms": 80000, "api_ok": True, "foreign": []},
                {"at_ms": 102900, "api_ok": True, "foreign": []},
                {"at_ms": 125800, "api_ok": True, "foreign": []}]

    def test_foreign_benchmark_is_recorded_but_the_cell_stays_usable(self):
        observations = [{"at_ms": 80000, "api_ok": True, "foreign": []},
                        {"at_ms": 101000, "api_ok": True, "foreign": [FOREIGN_POD]},
                        {"at_ms": 102900, "api_ok": True, "foreign": [FOREIGN_POD]},
                        {"at_ms": 125800, "api_ok": True, "foreign": []}]
        self.assertTrue(submit.coverage_ok([self.sample()], observations))
        # The old predicate keeps its "clean window" meaning; it just no longer decides acceptance.
        self.assertFalse(submit.window_valid([self.sample()], observations))
        window = submit.cell_window([self.sample()], observations, state="SUCCEEDED",
                                    done_marker_present=True)
        self.assertEqual((window["window_usable"], window["exclusive_window_valid"],
                          window["baseline_valid"]), (True, False, False))
        self.assertEqual(window["contamination"], "foreign_benchmark")
        self.assertEqual(window["foreign_observed"],
                         [{"app": "app-text-bench", "name": "text-bench-iceberg-1",
                           "role": "executor", "node": "cpu-0149", "category": "benchmark",
                           "first_at_ms": 101000, "last_at_ms": 102900}])
        self.assertEqual(window["foreign_observed_total"], 1)

    def test_foreign_summary_is_deduplicated_and_capped_at_twenty(self):
        pods = [{**FOREIGN_POD, "uid": "u%d" % index, "name": "text-bench-iceberg-%d" % index}
                for index in range(25)]
        observations = [{"at_ms": 80000, "api_ok": True, "foreign": []},
                        {"at_ms": 101000, "api_ok": True, "foreign": pods},
                        {"at_ms": 125800, "api_ok": True, "foreign": []}]
        window = submit.cell_window([self.sample()], observations, state="SUCCEEDED",
                                    done_marker_present=True)
        self.assertEqual(len(window["foreign_observed"]), 20)
        self.assertEqual(window["foreign_observed_total"], 25)

    def test_api_failure_still_makes_the_cell_unusable(self):
        failed = [{"at_ms": 80000, "api_ok": True, "foreign": []},
                  {"at_ms": 101000, "api_ok": False, "foreign": []},
                  {"at_ms": 125800, "api_ok": True, "foreign": []}]
        window = submit.cell_window([self.sample()], failed, state="SUCCEEDED",
                                    done_marker_present=True)
        self.assertFalse(window["coverage_ok"])
        self.assertFalse(window["window_usable"])
        self.assertEqual(window["contamination"], "api_failure")
        unknown = submit.cell_window([self.sample()], self.clean_observations(),
                                     state="SUCCEEDED", done_marker_present=True, api_unknown=True)
        self.assertFalse(unknown["window_usable"])
        self.assertEqual(unknown["contamination"], "api_failure")

    def test_coverage_hole_is_still_refused_and_is_not_called_contamination(self):
        holed = [{"at_ms": 80000, "api_ok": True, "foreign": []},
                 {"at_ms": 125800, "api_ok": True, "foreign": []}]
        window = submit.cell_window([self.sample()], holed, state="SUCCEEDED",
                                    done_marker_present=True)
        self.assertFalse(window["coverage_ok"])
        self.assertFalse(window["window_usable"])
        self.assertIsNone(window["contamination"])
        # An observation that starts after the sample is not a preceding observation.
        late = [{"at_ms": 102900, "api_ok": True, "foreign": []}]
        self.assertFalse(submit.coverage_ok([self.sample()], late))
        # No measured sample (only warmups) can never be covered.
        self.assertFalse(submit.coverage_ok([{**self.sample(), "warmup": True}],
                                            self.clean_observations()))

    def test_failed_state_or_missing_done_marker_is_unusable(self):
        for kwargs in ({"state": "FAILED", "done_marker_present": True},
                       {"state": "SUCCEEDED", "done_marker_present": False}):
            with self.subTest(**kwargs):
                window = submit.cell_window([self.sample()], self.clean_observations(), **kwargs)
                self.assertFalse(window["window_usable"])
                self.assertFalse(window["exclusive_window_valid"])
                self.assertIsNone(window["contamination"])

    def test_clean_window_is_usable_and_exclusive(self):
        window = submit.cell_window([self.sample()], self.clean_observations(),
                                    state="SUCCEEDED", done_marker_present=True)
        self.assertEqual((window["coverage_ok"], window["window_usable"],
                          window["exclusive_window_valid"], window["baseline_valid"],
                          window["contamination"], window["foreign_observed"]),
                         (True, True, True, True, None, []))


class WindowWaitTests(unittest.TestCase):
    """A busy cluster records window_wait_exhausted instead of killing the matrix."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def observed(self, foreign=()):
        return {"at_ms": 1, "api_ok": True, "foreign": list(foreign), "background": [],
                "classification": {"benchmark": len(foreign), "background": 1, "unknown": 0},
                "owned": []}

    def test_clean_window_returns_immediately_with_exhausted_false(self):
        evidence = self.root / "window-before.jsonl"
        with mock.patch.object(submit, "snapshot", return_value=self.observed()):
            window = submit.wait_window("ns", 60, evidence)
        self.assertFalse(window["exhausted"])
        self.assertEqual(window["polls"], 1)
        self.assertEqual(len(evidence.read_text().splitlines()), 1)

    def test_busy_window_exhausts_the_wait_and_records_evidence_instead_of_raising(self):
        evidence = self.root / "window-before.jsonl"
        with mock.patch.object(submit, "snapshot", return_value=self.observed([FOREIGN_POD])), \
                mock.patch.object(submit, "POLL_SECONDS", 0):
            window = submit.wait_window("ns", 0, evidence)
        self.assertTrue(window["exhausted"])
        self.assertEqual(window["polls"], 1)
        lines = [json.loads(line) for line in evidence.read_text().splitlines()]
        self.assertTrue(lines[-1]["window_wait_exhausted"])
        # The marker line must not look like a foreign list to any activity parser.
        self.assertNotIn("foreign", lines[-1])


class MatrixPolicyTests(unittest.TestCase):
    """run_matrix keeps the five-cell structure while contamination no longer aborts it."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.calls = []
        self.records = {("single", 10): test_report.single_cell(10),
                        ("single", 100): test_report.single_cell(100),
                        ("single", 2000): test_report.single_cell(2000),
                        ("batch", 100): test_report.batch_cell(100),
                        ("batch", 2000): test_report.batch_cell(2000)}

    def tearDown(self):
        self._tmp.cleanup()

    def usable_verdict(self, kind, k):
        return {"kind": kind, "k": k, "window_usable": True, "exclusive_window_valid": True,
                "baseline_valid": True, "coverage_ok": True, "foreign_observed": [],
                "foreign_observed_total": 0, "contamination": None,
                "window_wait_exhausted": False}

    def run_matrix(self, verdicts, output_name="out"):
        def prepare(c, contract_path, queries_path, truth_path, output, stopped_evidence=None):
            output = Path(output)
            output.mkdir(parents=True, exist_ok=False)
            return {"contract": c, "output": output, "remote": "/evidence/card3284",
                    "storage": {}, "secrets": (), "hdfs": "hdfs",
                    "jar": output / "BaselineBenchmark.jar"}

        def run_one_cell(ctx, kind, k, window_seconds):
            self.calls.append("%s-k%d" % (kind, k))
            verdict = verdicts.get((kind, k), self.usable_verdict(kind, k))
            if verdict["window_usable"] is not True:
                raise ValueError("cell %s k%d is not usable; matrix stopped" % (kind, k))
            return self.records[(kind, k)], verdict

        with mock.patch.object(submit, "prepare_matrix", prepare), \
                mock.patch.object(submit, "run_one_cell", run_one_cell), \
                mock.patch("os.umask"):
            submit.run_matrix(FROZEN_CONTRACT, "contract.json", "queries.jsonl", "truth.json",
                              self.root / output_name)
        return json.loads((self.root / output_name / "matrix-verdict.json").read_text())

    def test_five_cell_run_keeps_its_structure_and_the_real_sample_count(self):
        verdict = self.run_matrix({})
        self.assertEqual(self.calls, ["single-k10", "single-k100", "single-k2000",
                                      "batch-k100", "batch-k2000"])
        self.assertEqual(verdict["cells"], 5)
        self.assertEqual(verdict["samples"], 96)  # baseline.validate_matrix: 3*30 single + 2*3 batch
        self.assertIs(verdict["complete"], True)
        self.assertEqual((verdict["cells_usable"], verdict["cells_clean"]), (5, 5))
        self.assertEqual(verdict["contaminated_cells"], [])
        self.assertEqual(verdict["window_wait_exhausted_cells"], [])

    def test_a_contaminated_cell_does_not_stop_the_matrix(self):
        contaminated = {**self.usable_verdict("single", 10), "exclusive_window_valid": False,
                        "baseline_valid": False, "contamination": "foreign_benchmark",
                        "foreign_observed": [{"app": "app-text-bench",
                                              "name": "text-bench-iceberg-1", "role": "executor",
                                              "node": "cpu-0149", "category": "benchmark",
                                              "first_at_ms": 1, "last_at_ms": 2}],
                        "foreign_observed_total": 1}
        verdict = self.run_matrix({("single", 10): contaminated})
        self.assertEqual(len(self.calls), 5)
        self.assertIs(verdict["complete"], True)
        self.assertEqual((verdict["cells_usable"], verdict["cells_clean"]), (5, 4))
        self.assertEqual(verdict["contaminated_cells"], ["single-k10"])

    def test_an_unusable_cell_stops_the_matrix_and_preserves_the_failure(self):
        broken = {**self.usable_verdict("single", 100), "window_usable": False,
                  "exclusive_window_valid": False, "baseline_valid": False,
                  "coverage_ok": False}
        with self.assertRaises(ValueError):
            self.run_matrix({("single", 100): broken})
        verdict = json.loads((self.root / "out" / "matrix-verdict.json").read_text())
        self.assertIs(verdict["complete"], False)
        self.assertEqual(self.calls, ["single-k10", "single-k100"])
        self.assertEqual(verdict["cells_completed"], 1)
        self.assertEqual(verdict["cells_usable"], 1)
        self.assertIn("not usable", verdict["reason"])


class StoragePlaceholderTests(unittest.TestCase):
    def test_dry_run_placeholders_cover_exactly_the_platform_keys(self):
        with tempfile.TemporaryDirectory() as root:
            defaults = Path(root) / "spark-defaults.conf"
            defaults.write_text("spark.hadoop.fs.s3a.access.key=key-id\n"
                                "spark.hadoop.fs.s3a.secret.key=secret-key\n"
                                "spark.hadoop.fs.s3a.endpoint=http://minio:9000\n")
            env = {name: value for name, value in os.environ.items()
                   if name not in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_URL")}
            env["SPARK_DEFAULTS"] = str(defaults)
            with mock.patch.dict(os.environ, env, clear=True):
                conf, secrets = submit.platform_config()
        self.assertEqual(secrets, ("key-id", "secret-key"))
        placeholder = submit.placeholder_storage()
        self.assertEqual(set(placeholder), set(conf))
        self.assertNotIn("key-id", json.dumps(placeholder))
        self.assertNotIn("secret-key", json.dumps(placeholder))


class CellLifecycleTests(unittest.TestCase):
    """One full run_one_cell pass with mocked Kubernetes/HDFS: locks the verdict assembly.

    No cluster, no docker, no ssh: every external call is faked, and the sample clocks are built
    from the real clock so the coverage window is genuinely exercised.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.foreign = [FOREIGN_POD]

    def tearDown(self):
        self._tmp.cleanup()

    def records_text(self, kind, k, started_ms, ms=0):
        rows = []
        for qid in range(10):
            base = {"kind": kind, "k": k, "qid": qid, "complete": True,
                    "started_ms": started_ms, "ms": ms,
                    "cache_evidence": {"scope": "sql-executor-session", "residency_verified": False}}
            rows.append({**base, "warmup": True, "repeat": -1})
            rows += [{**base, "warmup": False, "repeat": repeat} for repeat in range(3)]
        return "\n".join(json.dumps(row) for row in rows) + "\n"

    def run_cell(self, output, foreign):
        pod = {"metadata": {"uid": "uid-1", "name": "b3255-single-k10-x",
                            "labels": {"spark-app-selector": "own-app"}},
               "status": {"phase": "Succeeded"}, "spec": {"nodeName": "cpu-1122"}}
        state = {"submitted": False, "discovered": False, "last_poll_ms": None}
        observations = []

        def snapshot(namespace, own_app=None, own_name=None):
            observed = {"at_ms": int(time.time() * 1000), "api_ok": True,
                        "foreign": list(foreign) if state["submitted"] else [],
                        "background": [], "classification": {}, "owned": []}
            observations.append(observed)
            state["last_poll_ms"] = observed["at_ms"]
            return observed

        def get_pod(namespace, name):
            if not state["discovered"]:
                state["discovered"] = True
                return None, True          # unique-pod precondition for the real driver name
            return pod, True

        def call(argv, timeout=30, **kwargs):
            if "spark-submit" in " ".join(str(item) for item in argv):
                state["submitted"] = True
                return subprocess.CompletedProcess(argv, 0, "", "")
            if "-cat" in argv:
                # The JVM runs the samples between the polls: anchor them on the last in-cell poll
                # with ms=0, so a poll exists at the start/end and the final snapshot covers it.
                return subprocess.CompletedProcess(
                    argv, 0, self.records_text("single", 10, state["last_poll_ms"]), "")
            if "-get" in argv:
                target = Path(argv[-1])
                (target / "app-1").mkdir(parents=True)
                (target / "app-1" / "events_1_1").write_text("{}")
                return subprocess.CompletedProcess(argv, 0, "", "")
            return subprocess.CompletedProcess(argv, 0, "", "")

        def kube(argv, timeout=30):
            if argv[:1] == ["logs"]:
                return subprocess.CompletedProcess(
                    argv, 0, 'BASELINE_DONE {"ok":true,"kind":"single","k":10}\n', "")
            return subprocess.CompletedProcess(argv, 0, "described", "")

        contract = copy.deepcopy(FROZEN_CONTRACT)
        contract["timeouts"] = {"cell_seconds": 60, "submit_seconds": 60}
        ctx = {"contract": contract, "output": output, "remote": "/evidence/pair",
               "storage": {}, "secrets": (), "hdfs": "hdfs", "jar": "b.jar"}
        with mock.patch.object(submit.pilot, "get_pod", get_pod), \
                mock.patch.object(submit.pilot, "call", call), \
                mock.patch.object(submit.pilot, "kube", kube), \
                mock.patch.object(submit.pilot, "record_pods_and_events", lambda *a, **k: None), \
                mock.patch.object(submit, "snapshot", snapshot), \
                mock.patch.object(submit, "POLL_SECONDS", 0):
            return submit.run_one_cell(ctx, "single", 10, 0)

    def test_foreign_window_is_accepted_with_its_contamination_recorded(self):
        output = self.root / "dirty"
        output.mkdir()
        records, verdict = self.run_cell(output, self.foreign)
        self.assertEqual(len(records), 40)
        self.assertIs(verdict["window_usable"], True)
        self.assertIs(verdict["exclusive_window_valid"], False)
        self.assertIs(verdict["baseline_valid"], False)
        self.assertEqual(verdict["contamination"], "foreign_benchmark")
        self.assertEqual(verdict["foreign_observed"][0]["app"], "app-text-bench")
        self.assertIs(verdict["done_marker_present"], True)
        self.assertIs(verdict["coverage_ok"], True)
        self.assertIs(verdict["window_wait_exhausted"], False)
        self.assertIn("accepted, never clean", verdict["window_action"])
        stored = json.loads((output / "single-k10" / "verdict.json").read_text())
        self.assertIs(stored["window_usable"], True)
        self.assertEqual(stored["contamination"], "foreign_benchmark")
        self.assertTrue((output / "single-k10" / "samples.jsonl").is_file())
        self.assertTrue((output / "single-k10" / "driver.log").is_file())
        self.assertTrue((output / "single-k10" / "window-before.jsonl").is_file())

    def test_clean_window_is_accepted_as_exclusive(self):
        output = self.root / "clean"
        output.mkdir()
        _records, verdict = self.run_cell(output, [])
        self.assertIs(verdict["window_usable"], True)
        self.assertIs(verdict["exclusive_window_valid"], True)
        self.assertIs(verdict["baseline_valid"], True)
        self.assertIsNone(verdict["contamination"])
        self.assertEqual(verdict["foreign_observed"], [])