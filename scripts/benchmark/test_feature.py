"""Offline invariants for the feature image/contract/run pipeline.

Standard library only, no docker, ssh, mvn or cluster calls, and every dry-run assertion
also proves that no subprocess was spawned. Collected by run-tests.py (test_*.py).
"""
import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import baseline
import feature_common


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, HERE / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


feature_image = load("feature_image_module", "feature-image.py")
feature_contract = load("feature_contract_module", "feature-contract.py")
feature_run = load("feature_run_module", "feature-run.py")

DIGEST = "sha256:" + "a" * 64
BUNDLE = "46bfca05e3be270c3c38734c48a5e703bc2be40d51840763e36fd5f5fa6041bf"
PARENT = "hub.i.basemind.com/wanganyang/sparkann-e2e@sha256:" + "3" * 64
IMAGE = "hub.i.basemind.com/wanganyang/sparkann-e2e:feat-3280"
COMMIT = "5cbafef"
JAR_NAME = "lance-spark-bundle-3.5_2.12-0.9.1-beta.1.jar"


def contract():
    return json.loads((HERE / "baseline-10m.json").read_text())


def derived(**overrides):
    args = dict(image=IMAGE, image_digest=DIGEST, image_bundle_sha256=BUNDLE,
                feature_commit=COMMIT, parent_image=PARENT, feature_card="3280")
    args.update(overrides)
    return feature_contract.derive(contract(), args["image"], args["image_digest"],
                                   args["image_bundle_sha256"], args["feature_commit"],
                                   args["parent_image"], args["feature_card"])


def quiet(call, *args, **kwargs):
    """Run a main() that prints JSON while keeping test output clean."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = call(*args, **kwargs)
    return result, buffer.getvalue()


class ContractDerivationTests(unittest.TestCase):
    def test_only_the_six_accounted_fields_may_change(self):
        base = contract()
        new = derived()
        self.assertEqual(feature_common.differences(feature_common.strip_accounted(base),
                                                    feature_common.strip_accounted(new)), [])
        self.assertEqual(sorted(set(new) - set(base)),
                         ["feature_card", "feature_commit", "parent_image"])
        self.assertEqual((new["image"], new["image_digest"], new["image_bundle_sha256"]),
                         (IMAGE, DIGEST, BUNDLE))
        self.assertEqual(new["feature_commit"], COMMIT)
        self.assertEqual(new["feature_card"], "3280")
        self.assertEqual(new["parent_image"], PARENT)
        self.assertEqual((new["rows"], new["single_ks"], new["version"]), (base["rows"], base["single_ks"], 56))
        baseline.validate_contract(new)
        self.assertEqual(base["image_digest"],
                         "sha256:374dddc63fc0de9b52bb604135594a38298c920be2950e9f1c87be72eb2ee0fb")

    def test_frozen_parameter_changes_are_refused(self):
        base = contract()
        mutations = [lambda c: c["resources"].update(executors=24),
                     lambda c: c.update(single_ks=[10, 100]),
                     lambda c: c.update(query_part="hdfs://other"),
                     lambda c: c["index_uuids"].pop()]
        for mutate in mutations:
            tampered = derived()
            mutate(tampered)
            with self.assertRaises(ValueError):
                feature_contract.verify_derived(base, tampered)
        # Changing only an accounted field is allowed and must pass the same check.
        feature_contract.verify_derived(base, derived(image_bundle_sha256="0" * 64))

    def test_floating_parent_and_malformed_identifiers_are_refused(self):
        cases = [dict(parent_image="hub.i.basemind.com/wanganyang/sparkann-e2e:latest"),
                 dict(image_digest="latest"),
                 dict(image_digest="sha256:" + "A" * 64),
                 dict(image_bundle_sha256="XYZ"),
                 dict(feature_commit="HEAD"),
                 dict(feature_card="ticket-3280"),
                 dict(image=IMAGE + "@" + DIGEST)]
        for case in cases:
            with self.assertRaises(ValueError, msg=repr(case)):
                derived(**case)

    def test_cli_derives_from_image_json_and_refuses_dry_run_json(self):
        built = {"image": IMAGE, "image_digest": DIGEST, "image_bundle_sha256": BUNDLE,
                 "feature_commit": COMMIT, "parent_image": PARENT,
                 "status": "BUILT", "image_digest_source": "docker push"}
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            image_json = root / "feature-image.json"
            image_json.write_text(json.dumps(built))
            out = root / "contract.json"
            with mock.patch.object(subprocess, "run") as run:
                _, text = quiet(feature_contract.main,
                                ["--image-json", str(image_json), "--feature-card", "3280",
                                 "--out", str(out)])
            self.assertEqual(run.call_count, 0)
            summary = json.loads(text)
            self.assertEqual(summary["status"], "DERIVED")
            self.assertEqual(summary["out"], str(out))
            self.assertEqual(summary["contract_sha256"], feature_common.sha256_file(out))
            reloaded = json.loads(out.read_text())
            self.assertEqual((reloaded["image"], reloaded["image_digest"]), (IMAGE, DIGEST))
            self.assertEqual(reloaded["feature_card"], "3280")
            self.assertTrue(summary["non_exempt_fields_unchanged"])

            image_json.write_text(json.dumps(dict(built, image_digest=None)))
            with self.assertRaises(ValueError):
                feature_contract.main(["--image-json", str(image_json), "--feature-card", "3280",
                                       "--out", str(root / "other.json")])
            self.assertFalse((root / "other.json").exists())

    def test_write_is_verified_before_rename(self):
        base = contract()
        new = derived()
        with tempfile.TemporaryDirectory() as root:
            out = Path(root) / "nested" / "contract-feat.json"
            path = feature_contract.write_contract(base, new, out)
            self.assertTrue(path.is_file())
            self.assertEqual(feature_contract.read_and_verify(path, base, expected=new), new)
            self.assertEqual([item.name for item in out.parent.iterdir()], ["contract-feat.json"])

            tampered = json.loads(out.read_text())
            tampered["rows"] = 1
            out.write_text(json.dumps(tampered))
            with self.assertRaises(ValueError):
                feature_contract.read_and_verify(out, base, expected=new)

            swapped = json.loads(out.read_text())
            swapped["rows"] = base["rows"]
            swapped["image"] = "hub.i.basemind.com/wanganyang/sparkann-e2e:other"
            out.write_text(json.dumps(swapped))
            with self.assertRaises(ValueError):
                feature_contract.read_and_verify(out, base, expected=new)

            bad = derived()
            bad["query_count"] = 1000
            bad_path = Path(root) / "bad.json"
            with self.assertRaises(ValueError):
                feature_contract.write_contract(base, bad, bad_path)
            self.assertFalse(bad_path.exists())


class ImageTests(unittest.TestCase):
    def worktree(self, root, jars=1):
        target = Path(root) / feature_image.BUNDLE_MODULE / "target"
        target.mkdir(parents=True)
        if jars == 1:
            (target / JAR_NAME).write_bytes(b"bundle")
            (target / ("original-" + JAR_NAME)).write_bytes(b"orig")
            (target / JAR_NAME.replace(".jar", "-sources.jar")).write_bytes(b"src")
            (target / JAR_NAME.replace(".jar", "-javadoc.jar")).write_bytes(b"doc")
        if jars == 2:
            (target / JAR_NAME).write_bytes(b"bundle")
            (target / "lance-spark-bundle-3.5_2.12-0.9.2-beta.1.jar").write_bytes(b"stale")
        return Path(root)

    def dry_run(self, worktree, *extra):
        argv = ["--worktree", str(worktree), "--tag-suffix", "3280", "--feature-card", "3280",
                "--base-contract", str(HERE / "baseline-10m.json")] + list(extra)
        with mock.patch.object(subprocess, "run") as run, \
                mock.patch.object(subprocess, "Popen") as popen, \
                mock.patch.object(subprocess, "call") as call, \
                mock.patch.object(subprocess, "check_output") as check:
            payload, _ = quiet(feature_image.main, argv)
        self.assertEqual(run.call_count + popen.call_count + call.call_count + check.call_count, 0)
        return payload

    def test_dry_run_spawns_no_process_and_reports_commands(self):
        with tempfile.TemporaryDirectory() as root:
            payload = self.dry_run(self.worktree(root))
        base = contract()
        self.assertEqual(payload["status"], "DRY_RUN_ONLY")
        self.assertEqual(payload["image"], "hub.i.basemind.com/wanganyang/sparkann-e2e:feat-3280")
        self.assertIsNone(payload["image_digest"])
        self.assertEqual(payload["parent_image"],
                         feature_image.repo_of(base["image"]) + "@" + base["image_digest"])
        self.assertEqual(payload["jar_name"], JAR_NAME)
        self.assertEqual(payload["image_bundle_sha256"],
                         feature_common.sha256_bytes(b"bundle"))
        commands = [step["command"] for step in payload["steps"]]
        self.assertIn("./mvnw -s /tmp/dsh-mvn-settings.xml -pl " + feature_image.BUNDLE_MODULE +
                      " -am package -DskipTests -Dspotless.skip=true", commands)
        self.assertTrue(any(command.startswith("scp ") and "Dockerfile" in command
                            for command in commands))
        self.assertTrue(any(command == "ssh shai 'docker push hub.i.basemind.com/wanganyang/"
                            "sparkann-e2e:feat-3280'" for command in commands))
        self.assertTrue(any("docker build -t hub.i.basemind.com/wanganyang/sparkann-e2e:feat-3280"
                            in command for command in commands))
        self.assertTrue(any("docker inspect --format" in command for command in commands))
        self.assertTrue(any("test ! -e ~/img-3280" in command for command in commands))
        self.assertFalse(payload["deletes_remote_files"])
        self.assertIsNone(payload["built_at"])

    def test_dockerfile_pins_parent_copies_jar_deletes_legacy_and_verifies(self):
        text = feature_image.dockerfile_text(PARENT, JAR_NAME, BUNDLE, commit=COMMIT, card="3280")
        directives = [line for line in text.splitlines() if line and not line.startswith("#")]
        self.assertEqual(directives[0], "FROM " + PARENT)
        self.assertIn("COPY " + JAR_NAME + " /opt/spark/jars/" + JAR_NAME, text)
        self.assertIn("RUN rm -f /opt/spark/jars/lance-spark-bundle-3.5_2.12-0.9.0-beta.2.jar", text)
        self.assertIn("sha256sum /opt/spark/jars/" + JAR_NAME, text)
        self.assertIn('= "%s"' % BUNDLE, text)
        self.assertIn("# feature commit: " + COMMIT, text)
        self.assertNotIn("latest", text)
        same = feature_image.dockerfile_text(PARENT, feature_image.LEGACY_JAR, BUNDLE)
        self.assertNotIn("RUN rm -f", same)
        for bad_parent in ["hub.i.basemind.com/wanganyang/sparkann-e2e:latest",
                           "hub.i.basemind.com/wanganyang/sparkann-e2e@sha256:abc"]:
            with self.assertRaises(ValueError):
                feature_image.dockerfile_text(bad_parent, JAR_NAME, BUNDLE)
        with self.assertRaises(ValueError):
            feature_image.dockerfile_text(PARENT, JAR_NAME, "ABC")

    def test_bundle_jar_resolution_ignores_shade_sidecars(self):
        with tempfile.TemporaryDirectory() as root:
            jar = feature_image.find_bundle_jar(self.worktree(root))
            self.assertEqual(jar.name, JAR_NAME)
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(ValueError):
                feature_image.find_bundle_jar(self.worktree(root, jars=2))
        with tempfile.TemporaryDirectory() as root:
            missing = Path(root) / "missing.jar"
            with self.assertRaises(ValueError):
                feature_image.find_bundle_jar(Path(root), jar=missing)

    def test_dry_run_without_a_built_jar_uses_a_placeholder(self):
        with tempfile.TemporaryDirectory() as root:
            empty = Path(root) / "unbuilt"
            empty.mkdir()
            payload = self.dry_run(empty)
        self.assertIsNone(payload["jar"])
        self.assertIsNone(payload["image_bundle_sha256"])
        self.assertIn("PLACEHOLDER", payload["dockerfile"])
        self.assertIn("not built yet", payload["note"])

    def test_explicit_jar_and_registry_override(self):
        with tempfile.TemporaryDirectory() as root:
            jar = self.worktree(root) / feature_image.BUNDLE_MODULE / "target" / JAR_NAME
            payload = self.dry_run(Path(root), "--jar", str(jar),
                                   "--registry", "hub.i.basemind.com/wanganyang/other")
        self.assertEqual(payload["jar"], str(jar))
        self.assertEqual(payload["image"], "hub.i.basemind.com/wanganyang/other:feat-3280")

    def test_run_mode_refuses_before_any_command(self):
        with tempfile.TemporaryDirectory() as root:
            worktree = self.worktree(root)
            argv = ["--worktree", str(worktree), "--tag-suffix", "3280", "--run",
                    "--base-contract", str(HERE / "baseline-10m.json")]
            with mock.patch.object(subprocess, "run") as run, \
                    mock.patch.object(subprocess, "Popen") as popen:
                with self.assertRaises(ValueError):
                    feature_image.main(argv)          # no ./mvnw
                (worktree / "mvnw").write_text("#!/bin/sh\n")
                with self.assertRaises(ValueError):
                    feature_image.main(argv)          # no resolvable git HEAD
                self.assertEqual(run.call_count + popen.call_count, 0)

    def git_worktree(self, root):
        worktree = self.worktree(root)
        commit = "e" * 40
        (worktree / ".git" / "refs" / "heads" / "card").mkdir(parents=True)
        (worktree / ".git" / "HEAD").write_text("ref: refs/heads/card/3280\n")
        (worktree / ".git" / "refs" / "heads" / "card" / "3280").write_text(commit + "\n")
        (worktree / "mvnw").write_text("#!/bin/sh\n")
        return worktree, commit

    def run_image(self, worktree, root, *extra,
                  push=("latest: digest: " + DIGEST + " size: 4242"),
                  inspect="repo@" + DIGEST):
        recorded = []

        def fake_execute(args, steps, collected):
            recorded.append(([step["step"] for step in steps], dict(collected)))
            if len(recorded) == 1:
                return dict(collected)
            return dict(collected, **{"docker push": push,
                                      "docker inspect RepoDigests": inspect})

        argv = ["--worktree", str(worktree), "--tag-suffix", "3280", "--run",
                "--context-dir", str(Path(root) / "context"),
                "--out", str(Path(root) / "image.json"),
                "--base-contract", str(HERE / "baseline-10m.json")] + list(extra)
        with mock.patch.object(feature_image, "execute", side_effect=fake_execute), \
                mock.patch.object(feature_image, "check_clean_worktree", return_value=""), \
                mock.patch.object(subprocess, "run") as run, \
                mock.patch.object(subprocess, "Popen") as popen:
            payload, _ = quiet(feature_image.main, argv)
        self.assertEqual(run.call_count + popen.call_count, 0)
        return payload, recorded, Path(root)

    def test_run_branch_builds_pushes_and_reads_back_digest(self):
        with tempfile.TemporaryDirectory() as root:
            worktree, commit = self.git_worktree(root)
            payload, recorded, out = self.run_image(worktree, root)
            self.assertEqual(payload["status"], "BUILT")
            self.assertEqual(payload["image_digest"], DIGEST)
            self.assertEqual(payload["digest_source"], "docker push")
            self.assertEqual(payload["feature_commit"], commit)
            self.assertEqual(payload["image_bundle_sha256"],
                             feature_common.sha256_bytes(b"bundle"))
            self.assertIn("mvn package bundle", recorded[0][0])
            self.assertNotIn("mvn package bundle", recorded[1][0])
            self.assertTrue(any("docker push" == step for step in recorded[1][0]))
            dockerfile = (out / "context" / "Dockerfile").read_text()
            self.assertEqual(dockerfile.splitlines()[2], "FROM " + payload["parent_image"])
            self.assertIn("COPY " + JAR_NAME + " /opt/spark/jars/" + JAR_NAME, dockerfile)
            written = json.loads((out / "image.json").read_text())
            self.assertEqual(written["image_digest"], DIGEST)
            self.assertEqual(written["built_at"], payload["built_at"])

    def test_run_branch_can_skip_maven_with_an_explicit_jar_and_needs_a_digest(self):
        with tempfile.TemporaryDirectory() as root:
            worktree, _ = self.git_worktree(root)
            jar = worktree / feature_image.BUNDLE_MODULE / "target" / JAR_NAME
            payload, recorded, _ = self.run_image(worktree, root, "--jar", str(jar))
            self.assertNotIn("mvn package bundle", recorded[0][0])
            self.assertEqual(payload["jar"], str(jar))
        with tempfile.TemporaryDirectory() as root:
            worktree, _ = self.git_worktree(root)
            payload, _, _ = self.run_image(worktree, root, push="no digest here")
            self.assertEqual(payload["digest_source"], "docker inspect RepoDigests")
            self.assertEqual(payload["image_digest"], DIGEST)
        with tempfile.TemporaryDirectory() as root:
            worktree, _ = self.git_worktree(root)
            with self.assertRaises(ValueError):
                self.run_image(worktree, root, push="no digest here", inspect="<no value>")

    def test_run_refuses_a_dirty_worktree_unless_allowed(self):
        with tempfile.TemporaryDirectory() as root:
            worktree, _ = self.git_worktree(root)
            argv = ["--worktree", str(worktree), "--tag-suffix", "3280", "--run",
                    "--context-dir", str(Path(root) / "context"),
                    "--base-contract", str(HERE / "baseline-10m.json")]
            with mock.patch.object(feature_image, "check_clean_worktree",
                                   return_value=" M src/main/scala/X.scala"):
                with mock.patch.object(feature_image, "execute") as execute:
                    with self.assertRaises(ValueError):
                        feature_image.main(argv)
                    self.assertEqual(execute.call_count, 0)
            with mock.patch.object(feature_image, "check_clean_worktree",
                                   return_value=" M src/main/scala/X.scala"), \
                    mock.patch.object(feature_image, "execute",
                                      side_effect=lambda args, steps, collected: dict(collected)), \
                    mock.patch.object(feature_image, "assemble_context",
                                      side_effect=lambda context, jar, text, sha:
                                      {"context": str(context)}):
                with mock.patch.object(feature_image, "parse_push_digest", return_value=DIGEST):
                    payload, _ = quiet(feature_image.main, argv + ["--allow-dirty"])
            self.assertEqual(payload["status"], "BUILT")

    def test_parent_from_contract_requires_digest(self):
        base = contract()
        self.assertEqual(feature_image.parent_from_contract(base),
                         feature_image.repo_of(base["image"]) + "@" + base["image_digest"])
        self.assertEqual(feature_image.repo_of("reg:5000/team/img:tag"), "reg:5000/team/img")
        for bad in ["latest", "sha256:" + "a" * 63, "sha256:" + "A" * 64]:
            mutated = copy.deepcopy(base)
            mutated["image_digest"] = bad
            with self.assertRaises(ValueError):
                feature_image.parent_from_contract(mutated)


class FeatureRunPlanTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.base = Path(self.root.name)
        self.contract_path = self.base / "contract-feat.json"
        feature_contract.write_contract(contract(), derived(), self.contract_path)
        self.queries = "/home/wanganyang/runs/3271-truth-b1/queries-f32.jsonl"
        self.truth = "/home/wanganyang/runs/3271-truth-b1/truth-top2000.json"

    def tearDown(self):
        self.root.cleanup()

    def args(self, **overrides):
        values = dict(contract=self.contract_path, baseline=HERE / "baseline-10m.json",
                      queries=self.queries, truth=self.truth, output_root="~/runs/3280-feat",
                      tool_dir="~/runs/tool-3280", stopped_evidence=None, remote="shai",
                      kubeconfig="~/workspace/k8s-config/clusterA.yaml",
                      java_home="/usr/lib/jvm/java-17-openjdk-amd64",
                      hadoop_home="/opt/hadoop-3.3.1", spark_home="/opt/spark",
                      card=None, window_seconds=1800, sync_method="rsync",
                      sync_timeout=600, launch_timeout=120)
        values.update(overrides)
        return feature_run.argparse.Namespace(**values)

    def plan(self, *extra, output_root="~/runs/3280-feat", contract_path=None):
        argv = ["--contract", str(contract_path or self.contract_path),
                "--queries", self.queries, "--truth", self.truth,
                "--output-root", output_root] + list(extra)
        with mock.patch.object(subprocess, "run") as run:
            payload, _ = quiet(feature_run.main, argv)
        self.assertEqual(run.call_count, 0, "dry-run must not spawn ssh/scp/rsync")
        return payload

    def test_plan_is_detached_create_only_and_never_deletes(self):
        payload = self.plan()
        self.assertEqual(payload["status"], "DRY_RUN_ONLY")
        launch = payload["launch_command"]
        for token in ["( mkdir -p ", "cd ", "exec setsid nohup env ", "sh -c 'echo $$ > \"$1\"; shift; exec \"$@\"'",
                      "2>&1 < /dev/null", ") & for _ in", "cat "]:
            self.assertIn(token, launch)
        for token in ["KUBECONFIG=$HOME/workspace/k8s-config/clusterA.yaml",
                      "JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64",
                      "HADOOP_HOME=/opt/hadoop-3.3.1", "SPARK_HOME=/opt/spark",
                      "--run --approved", "--window-seconds 1800"]:
            self.assertIn(token, launch)
        self.assertNotIn("=~", launch)
        self.assertEqual(payload["output_root"], "~/runs/3280-feat")
        self.assertEqual(payload["runner_log"], "~/runs/3280-feat.runner.log")
        self.assertEqual(payload["pid_file"], "~/runs/3280-feat.runner.pid")
        self.assertEqual([guard["path"] for guard in payload["guards"]],
                         [payload["tool_dir"], payload["output_root"],
                          payload["runner_log"], payload["pid_file"]])
        self.assertTrue(all(guard["policy"] == "create-only" for guard in payload["guards"]))
        self.assertTrue(payload["same_caliber_as_baseline"])
        self.assertEqual(payload["image"], IMAGE + "@" + DIGEST)
        self.assertEqual(payload["feature_card"], "3280")
        text = "\n".join(step["command"] for step in payload["commands"]) + launch
        for forbidden in ["rm -f", "rm -rf", "--delete", "kubectl delete", "docker rm",
                          "kill -", "pkill", "stop "]:
            self.assertNotIn(forbidden, text)
        steps = [step["step"] for step in payload["commands"]]
        self.assertEqual(steps[0], "precondition: create-only " + payload["tool_dir"])
        self.assertIn("upload feature contract", steps)
        self.assertEqual(steps[-1], "launch detached")
        self.assertIn("rsync -az --exclude __pycache__", text)

    def test_run_requires_approved_and_dry_run_is_the_default(self):
        for argv in (["--contract", str(self.contract_path), "--queries", self.queries,
                      "--truth", self.truth, "--output-root", "~/runs/x", "--run"],
                     ["--contract", str(self.contract_path), "--queries", self.queries,
                      "--truth", self.truth, "--output-root", "~/runs/x", "--run", "--dry-run"]):
            with mock.patch.object(subprocess, "run") as run:
                with self.assertRaises(SystemExit):
                    with contextlib.redirect_stderr(io.StringIO()):
                        feature_run.main(argv)
                self.assertEqual(run.call_count, 0)

    def test_tampered_or_incomplete_contracts_are_refused(self):
        tampered = derived()
        tampered["single_ks"] = [10]
        path = self.base / "tampered.json"
        path.write_text(json.dumps(tampered))
        with self.assertRaises(ValueError):
            self.plan(contract_path=path)

        stripped = derived()
        stripped.pop("feature_commit")
        path = self.base / "stripped.json"
        path.write_text(json.dumps(stripped))
        with self.assertRaises(ValueError):
            self.plan(contract_path=path)

        floating = derived()
        floating["parent_image"] = "hub.i.basemind.com/wanganyang/sparkann-e2e:latest"
        path = self.base / "floating.json"
        path.write_text(json.dumps(floating))
        with self.assertRaises(ValueError):
            self.plan(contract_path=path)

    def test_stopped_evidence_and_scp_sync_are_planned(self):
        evidence = self.base / "verified-stopped.json"
        evidence.write_text(json.dumps({}))
        payload = self.plan("--stopped-evidence", str(evidence), "--sync-method", "scp")
        commands = [step["command"] for step in payload["commands"]]
        self.assertTrue(any(command.startswith("scp -p ") and "feature-image.py" in command
                            for command in commands))
        self.assertFalse(any(command.startswith("rsync") for command in commands))
        self.assertTrue(any(command.startswith("scp ") and "stopped-evidence.json" in command
                            for command in commands))
        self.assertIn("--stopped-evidence stopped-evidence.json", payload["launch_command"])
        self.assertEqual(payload["stopped_evidence"], str(evidence))

    def test_execute_refuses_existing_path_and_reads_the_pid(self):
        payload, steps = feature_run.build_plan(self.args())
        existing = subprocess.CompletedProcess([], 1, "", "exists")
        with mock.patch.object(feature_run, "run_command",
                               return_value=existing) as run:
            with self.assertRaises(ValueError):
                feature_run.execute(payload, steps)
            self.assertEqual(run.call_count, 1)

        payload, steps = feature_run.build_plan(self.args())
        def fake(argv, timeout):
            if argv[-1].startswith("test ! -e"):
                return subprocess.CompletedProcess(argv, 0, "", "")
            if argv[-1] == steps[-1]["argv"][-1]:
                return subprocess.CompletedProcess(argv, 0, "4242\n", "")
            return subprocess.CompletedProcess(argv, 0, "ok", "")
        with mock.patch.object(feature_run, "run_command", side_effect=fake):
            started = feature_run.execute(payload, steps)
        self.assertEqual(started["status"], "STARTED")
        self.assertEqual(started["pid"], 4242)

    def test_execute_refuses_a_non_pid_launch_output(self):
        payload, steps = feature_run.build_plan(self.args())
        def fake(argv, timeout):
            return subprocess.CompletedProcess(argv, 0, "no pid here\n", "")
        with mock.patch.object(feature_run, "run_command", side_effect=fake):
            with self.assertRaises(ValueError):
                feature_run.execute(payload, steps)

    def test_uri_assets_are_refused(self):
        for bad, good in (("hdfs://nn:8020/q.parquet", self.truth),
                          (self.queries, "hdfs://nn:8020/truth.json")):
            argv = ["--contract", str(self.contract_path), "--queries", bad,
                    "--truth", good, "--output-root", "~/runs/3280-feat"]
            with mock.patch.object(subprocess, "run") as run:
                with self.assertRaises(ValueError):
                    with contextlib.redirect_stdout(io.StringIO()):
                        feature_run.main(argv)
                self.assertEqual(run.call_count, 0)

    def test_output_root_must_be_a_shell_safe_directory(self):
        for bad in ["~/runs/x; rm -rf /", "$HOME/runs/x", "~/runs/x y", "~/runs/../etc",
                    "~/runs/~/x", "/", ""]:
            with self.assertRaises(ValueError, msg=bad):
                self.plan(output_root=bad)
        payload = self.plan(output_root="/data/bench/3280-feat/")
        self.assertEqual(payload["output_root"], "/data/bench/3280-feat")
        self.assertEqual(payload["follow"], "ssh shai tail -f /data/bench/3280-feat.runner.log")


class GitHeadTests(unittest.TestCase):
    def test_git_head_reader_matches_git_rev_parse(self):
        worktree = HERE.parent.parent
        expected = subprocess.run(["git", "-C", str(worktree), "rev-parse", "HEAD"],
                                  capture_output=True, text=True)
        self.assertEqual(expected.returncode, 0, expected.stderr)
        self.assertEqual(feature_common.git_head(worktree), expected.stdout.strip())
        self.assertEqual(len(expected.stdout.strip()), 40)

    def test_git_head_reads_worktree_gitdir_file_and_packed_refs(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            plain = root / "plain"
            (plain / ".git" / "refs" / "heads").mkdir(parents=True)
            (plain / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
            (plain / ".git" / "refs" / "heads" / "main").write_text("b" * 40 + "\n")
            self.assertEqual(feature_common.git_head(plain), "b" * 40)
            (plain / ".git" / "HEAD").write_text("c" * 40 + "\n")
            self.assertEqual(feature_common.git_head(plain), "c" * 40)
            self.assertIsNone(feature_common.git_head(root / "absent"))

            repo = root / "repo"
            admin = repo / ".git" / "worktrees" / "card-3280"
            admin.mkdir(parents=True)
            (repo / ".git" / "refs" / "heads").mkdir(parents=True, exist_ok=True)
            (repo / ".git" / "refs" / "heads" / "dev").write_text("d" * 40 + "\n")
            (admin / "HEAD").write_text("ref: refs/heads/dev\n")
            (admin / "commondir").write_text("../..\n")
            worktree = root / "wt"
            worktree.mkdir()
            (worktree / ".git").write_text("gitdir: %s\n" % admin)
            self.assertEqual(feature_common.git_head(worktree), "d" * 40)


if __name__ == "__main__":
    unittest.main()
