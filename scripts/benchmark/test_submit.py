"""Formal submission gates: activity is keyed by app/UID, never a pod name prefix."""
import unittest
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
        import json,subprocess,tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as root:
            p=Path(root);errors={}
            def timeout():raise subprocess.TimeoutExpired("kubectl",1)
            self.assertIsNone(submit.capture("logs",timeout,errors))
            submit.write_verdict(p,{"state":"ORIGINAL_FAILED","baseline_valid":True},errors,())
            v=json.loads((p/"verdict.json").read_text())
            self.assertFalse(v["baseline_valid"])
            self.assertEqual(v["state"],"ORIGINAL_FAILED")
            self.assertEqual(v["evidence_errors"]["logs"],"TimeoutExpired")

    def test_schedule_is_frozen_and_no_existing_pod_delete_command(self):
        self.assertEqual(submit.schedule(), {"spark.kubernetes.driver.node.selector.kubernetes.io/hostname": "cpu-1122.host.platform.shaipower.com"})
        options = submit.delete_options("u1")
        self.assertEqual(options["preconditions"]["uid"], "u1")
        self.assertNotIn("labelSelector", options)


if __name__ == "__main__":
    unittest.main()
