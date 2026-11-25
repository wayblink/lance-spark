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

    def test_schedule_is_frozen_and_no_existing_pod_delete_command(self):
        self.assertEqual(submit.schedule(), {"spark.kubernetes.driver.node.selector.kubernetes.io/hostname": "cpu-1122.host.platform.shaipower.com"})
        options = submit.delete_options("u1")
        self.assertEqual(options["preconditions"]["uid"], "u1")
        self.assertNotIn("labelSelector", options)


if __name__ == "__main__":
    unittest.main()
