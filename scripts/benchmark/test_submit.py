"""Formal submission gates: activity is keyed by app/UID, never a pod name prefix."""
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import submit


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
