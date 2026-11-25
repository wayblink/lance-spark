#!/usr/bin/env python3
"""Build, tag and describe one feature image from a source worktree; dry-run by default.

Parent image (must be immutable): --parent-image repo@sha256:<64 hex>, or --base-contract
(default baseline-10m.json, which must hash to feature_common.BASELINE_SHA256).
--run requires --approved; dry-run prints every command and spawns nothing.

Steps with --run:
  1. ./mvnw -s <settings> -pl lance-spark-bundle-3.5_2.12 -am clean package
       -DskipTests -Dspotless.skip=true      (cwd = --worktree)
     "clean" is deliberate: an incremental package can carry stale classes of renamed or
     deleted sources into the shaded bundle, which would benchmark the wrong code.
  2. resolve exactly one target/lance-spark-bundle-3.5_2.12-*.jar (excluding original-/
     sources-/javadoc- sidecars) and sha256 it. --jar skips the build, but it must be bound
     to the commit that produced it via --jar-source-commit, and that commit is recorded as
     the image's feature_commit (the worktree HEAD is recorded separately).
  3. Dockerfile: FROM <parent digest>; LABEL purpose/lance-spark.commit/lance-spark.bundle/
     lance-spark.card; remove every old bundle jar BEFORE the COPY so a parent-image version
     bump cannot leave two bundle jars on the classpath; COPY the new jar; sha256sum
     assertion; "exactly one bundle jar" assertion.
  4. ssh <remote> mkdir -p ~/img-<suffix>; scp Dockerfile + jar; ssh docker build/push;
     read the immutable digest back from the push output or `docker inspect RepoDigests`,
     and cross-check both when both are available.

Defaults (flag or environment):
  --remote         shai                       (FEATURE_REMOTE)
  --remote-root    ~/img-<tag-suffix>
  --registry       repository of the parent image / base contract
  --mvn-settings   /tmp/dsh-mvn-settings.xml
  docker runs on the remote host, so no local Docker daemon is required.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import baseline
import feature_common

BUNDLE_MODULE = "lance-spark-bundle-3.5_2.12"
JAR_DIR = "/opt/spark/jars"
DEFAULT_SETTINGS = "/tmp/dsh-mvn-settings.xml"
TAG_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
JAR_NAME_RE = re.compile(r"[A-Za-z0-9._-]+\.jar")


def require(ok, message):
    feature_common.require(ok, message)


def mvn_argv(settings, module=BUNDLE_MODULE):
    """The reviewed bundle build command; `clean` keeps stale classes out of the bundle."""
    return ["./mvnw", "-s", str(settings), "-pl", module, "-am", "clean", "package",
            "-DskipTests", "-Dspotless.skip=true"]


def jar_candidates(worktree, module=BUNDLE_MODULE):
    target = Path(worktree) / module / "target"
    if not target.is_dir():
        return []
    return sorted(p for p in target.glob(module + "-*.jar")
                  if not any(token in p.name for token in ("original-", "sources", "javadoc")))


def find_bundle_jar(worktree, module=BUNDLE_MODULE, jar=None):
    """Resolve the single shaded bundle jar, excluding maven shade/dependency sidecars."""
    if jar is not None:
        path = Path(jar)
        require(path.is_file(), "explicit --jar does not exist: " + str(path))
        require(JAR_NAME_RE.fullmatch(path.name) is not None, "explicit --jar must be a bundle jar")
        return path
    candidates = jar_candidates(worktree, module)
    require(len(candidates) <= 1,
            "expected one bundle jar, found %d (%s); clean stale target/ or pass --jar"
            % (len(candidates), ", ".join(p.name for p in candidates)))
    return candidates[0] if candidates else None


def parent_from_contract(contract):
    """Pure digest form repo@sha256:..., so the parent can never move."""
    require(feature_common.is_digest(contract.get("image_digest")),
            "baseline image_digest must be sha256:<64 lowercase hex>")
    ref = "%s@%s" % (repo_of(contract["image"]), contract["image_digest"])
    feature_common.require_digest_ref(ref, "parent image")
    return ref


def repo_of(image):
    """Strip a trailing tag from repo[:port]/path:tag, keeping registry ports intact."""
    head, sep, last = image.rpartition("/")
    if ":" in last:
        last = last.split(":", 1)[0]
    return head + sep + last if sep else last


def dockerfile_text(parent_image, jar_name, bundle_sha256, dest=JAR_DIR, commit=None,
                    card=None):
    """Minimal Dockerfile: pinned parent, exactly one bundle jar, sha256 verified.

    The old bundle jars are removed *before* the COPY, so a parent image that already carries
    a different (or bumped) bundle version cannot leave two jars whose classpath order would
    decide which connector runs.
    """
    feature_common.require_digest_ref(parent_image, "parent image")
    require(JAR_NAME_RE.fullmatch(jar_name) is not None, "jar name must be a bare file name")
    require(feature_common.is_sha256_hex(bundle_sha256),
            "bundle sha256 must be 64 lowercase hex characters")
    lines = ["# Generated by scripts/benchmark/feature-image.py; do not edit by hand.",
             "# Old bundle jars are removed before COPY so exactly one can survive."]
    if commit:
        lines.append("# feature commit: " + commit)
    if card:
        lines.append("# feature card: " + str(card))
    lines.append("FROM " + parent_image)
    lines.append('LABEL purpose="lance-spark feature bundle for the frozen five-cell benchmark"')
    lines.append('LABEL lance-spark.bundle="%s"' % bundle_sha256)
    if commit:
        lines.append('LABEL lance-spark.commit="%s"' % commit)
    if card:
        lines.append('LABEL lance-spark.card="%s"' % card)
    lines.append("RUN rm -f %s/%s-*.jar" % (dest, BUNDLE_MODULE))
    lines.append("COPY " + jar_name + " " + dest + "/" + jar_name)
    lines.append('RUN test "$(sha256sum %s/%s | cut -d\' \' -f1)" = "%s"'
                 % (dest, jar_name, bundle_sha256))
    lines.append('RUN test "$(ls %s/%s-*.jar 2>/dev/null | wc -l | tr -d \' \')" -eq 1'
                 % (dest, BUNDLE_MODULE))
    return "\n".join(lines) + "\n"


def assemble_context(context, jar, text, expected_sha):
    """Materialise Dockerfile + jar; the copied bytes must still hash to expected_sha."""
    context = Path(context)
    context.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(jar), str(context / jar.name))
    require(feature_common.sha256_file(context / jar.name) == expected_sha,
            "copied bundle jar hash changed during context assembly")
    (context / "Dockerfile").write_text(text)
    require(feature_common.sha256_file(context / "Dockerfile") ==
            feature_common.sha256_bytes(text.encode()),
            "Dockerfile bytes changed during context assembly")
    return {"context": str(context), "dockerfile": str(context / "Dockerfile"),
            "jar": str(context / jar.name),
            "dockerfile_sha256": feature_common.sha256_file(context / "Dockerfile")}


def parse_push_digest(text):
    found = re.findall(r"digest:\s*(sha256:[0-9a-f]{64})", text or "")
    return found[-1] if found else None


def parse_repo_digest(text):
    for line in reversed((text or "").strip().splitlines()):
        line = line.strip()
        if "@sha256:" in line:
            digest = line.split("@", 1)[1].strip()
            if feature_common.is_digest(digest):
                return digest
    return None


def build_steps(args, worktree, remote_dir, context_dir, image_ref, jar_name, include_mvn=True):
    """Pre steps never need the jar; post steps upload, build, push and read the digest back."""
    remote = args.remote
    pre = [
        {"step": "remote dir precondition (create-only)", "kind": "probe",
         "argv": ["ssh", remote, "test ! -e " + remote_dir], "path": remote_dir, "timeout": 60},
    ]
    if include_mvn:
        pre.append({"step": "mvn clean package bundle", "kind": "run", "cwd": str(worktree),
                    "argv": mvn_argv(args.mvn_settings), "timeout": args.build_timeout})
    pre.append({"step": "remote mkdir", "kind": "run",
                "argv": ["ssh", remote, "mkdir -p " + remote_dir], "timeout": 60})
    post = [
        {"step": "scp Dockerfile", "kind": "run",
         "argv": ["scp", str(Path(context_dir) / "Dockerfile"),
                  "%s:%s/Dockerfile" % (remote, remote_dir)], "timeout": args.transfer_timeout},
        {"step": "scp bundle jar", "kind": "run",
         "argv": ["scp", str(Path(context_dir) / jar_name),
                  "%s:%s/%s" % (remote, remote_dir, jar_name)], "timeout": args.transfer_timeout},
        {"step": "docker build", "kind": "run",
         "argv": ["ssh", remote, "docker build -t %s %s" % (image_ref, remote_dir)],
         "timeout": args.remote_command_timeout},
        {"step": "docker push", "kind": "run",
         "argv": ["ssh", remote, "docker push %s" % image_ref],
         "timeout": args.remote_command_timeout},
        {"step": "docker inspect RepoDigests", "kind": "run",
         "argv": ["ssh", remote, 'docker inspect --format "{{index .RepoDigests 0}}" %s' % image_ref],
         "timeout": 60},
    ]
    return pre, post


def tail(text, limit=40):
    lines = [line for line in (text or "").splitlines() if line.strip()]
    return "\n".join(lines[-limit:])[:4000]


def run_command(argv, timeout, cwd=None):
    return subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=timeout)


def execute(args, steps, collected):
    for step in steps:
        result = run_command(step["argv"], step["timeout"], cwd=step.get("cwd"))
        output = (result.stdout or "") + (result.stderr or "")
        if step["kind"] == "probe":
            require(result.returncode in (0, 1),
                    "cannot verify remote path %s: %s" % (step["path"], tail(output)))
            require(result.returncode == 0, "refuse pre-existing remote path: " + step["path"])
            continue
        require(result.returncode == 0, "%s failed (exit %d): %s"
                % (step["step"], result.returncode, tail(output)))
        collected[step["step"]] = output
    return collected


def resolve_parent(args):
    if args.parent_image is not None:
        feature_common.require_digest_ref(args.parent_image, "parent image")
        return args.parent_image, None
    contract_path = Path(args.base_contract)
    require(contract_path.is_file(), "base contract not found: " + str(contract_path))
    feature_common.require_baseline_pin(contract_path)
    contract = json.loads(contract_path.read_text())
    baseline.validate_contract(contract)
    return parent_from_contract(contract), contract


def resolve_repo(args, parent, contract):
    if args.registry:
        repo = args.registry.rstrip("/")
    elif contract is not None:
        repo = repo_of(contract["image"])
    else:
        repo = repo_of(parent.split("@", 1)[0])
    require("/" in repo or "." in repo, "registry/repository looks wrong: " + repo)
    return repo


def resolve_tag(args):
    if args.tag:
        tag = args.tag
    else:
        require(args.tag_suffix, "--tag-suffix (or --tag) is required")
        tag = "%s-%s" % (args.tag_prefix, args.tag_suffix)
    require(TAG_RE.fullmatch(tag) is not None, "tag must match [A-Za-z0-9][A-Za-z0-9._-]*")
    return tag


def context_dir(args, tag):
    if args.context_dir:
        return Path(args.context_dir)
    return Path(tempfile.gettempdir()) / ("feature-image-%s-%s"
                                          % (tag, feature_common.utc_stamp()))


def check_clean_worktree(worktree):
    result = run_command(["git", "-C", str(worktree), "status", "--porcelain"], 120)
    require(result.returncode == 0, "cannot read git status of --worktree: " + tail(result.stderr))
    return result.stdout.strip()


def git_commit_exists(worktree, commit):
    result = run_command(["git", "-C", str(worktree), "cat-file", "-e", commit + "^{commit}"], 120)
    return result.returncode == 0


def resolve_image_commit(args, worktree, worktree_head):
    """--jar must name the commit that produced it; otherwise the worktree HEAD is the source."""
    if args.jar is None:
        return worktree_head, None
    require(args.jar_source_commit is not None,
            "--jar must be bound to the commit that produced it; pass --jar-source-commit <sha>")
    require(feature_common.is_commit(args.jar_source_commit),
            "--jar-source-commit must be 7-40 lowercase hex")
    return args.jar_source_commit, args.jar_source_commit


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--worktree", type=Path, required=True,
                        help="source tree to package (usually a card worktree)")
    parser.add_argument("--tag-suffix", help="tag suffix; final tag is <tag-prefix>-<suffix>")
    parser.add_argument("--tag-prefix", default="feat")
    parser.add_argument("--tag", help="explicit tag, overrides prefix/suffix")
    parser.add_argument("--parent-image", help="repo@sha256:<64 hex>; floating tags are refused")
    parser.add_argument("--base-contract", type=Path, default=HERE / "baseline-10m.json")
    parser.add_argument("--registry",
                        help="destination repository, e.g. hub.i.basemind.com/wanganyang/sparkann-e2e")
    parser.add_argument("--remote", default=os.environ.get("FEATURE_REMOTE", "shai"))
    parser.add_argument("--remote-root", help="remote context directory (default ~/img-<suffix>)")
    parser.add_argument("--mvn-settings", default=DEFAULT_SETTINGS)
    parser.add_argument("--jar", type=Path, help="explicit bundle jar instead of building one")
    parser.add_argument("--jar-source-commit",
                        help="commit that produced --jar; recorded as feature_commit")
    parser.add_argument("--context-dir", type=Path,
                        help="local build context directory (default: a fresh temp dir)")
    parser.add_argument("--build-timeout", type=int, default=1800)
    parser.add_argument("--transfer-timeout", type=int, default=600)
    parser.add_argument("--remote-command-timeout", type=int, default=1200)
    parser.add_argument("--feature-card", default=None,
                        help="card id kept in the Dockerfile comment and label")
    parser.add_argument("--allow-dirty", action="store_true",
                        help="allow --run on a worktree with uncommitted changes (not recommended)")
    parser.add_argument("--dry-run", action="store_true", help="print the plan only (default)")
    parser.add_argument("--run", action="store_true", help="actually build, push and inspect")
    parser.add_argument("--approved", action="store_true",
                        help="required with --run after root review and user authorization")
    parser.add_argument("--out", type=Path, help="write the JSON result here as well")
    args = parser.parse_args(argv)
    if args.run and args.dry_run:
        parser.error("--run and --dry-run are mutually exclusive")
    if args.run and not args.approved:
        parser.error("--run requires --approved after root review and user authorization")

    parent, contract = resolve_parent(args)
    worktree = Path(args.worktree).resolve()
    require(worktree.is_dir(), "--worktree does not exist: " + str(worktree))
    feature_common.require_remote_path(args.remote, "remote host")
    repo = resolve_repo(args, parent, contract)
    tag = resolve_tag(args)
    image_ref = "%s:%s" % (repo, tag)
    remote_dir = args.remote_root or ("~/img-" + (args.tag_suffix or tag))
    feature_common.require_remote_path(remote_dir, "remote dir")
    context = context_dir(args, tag)
    worktree_head = feature_common.git_head(worktree)
    image_commit, jar_source_commit = resolve_image_commit(args, worktree, worktree_head)

    jar = find_bundle_jar(worktree, jar=args.jar)
    bundle_sha = feature_common.sha256_file(jar) if jar else None
    jar_name = jar.name if jar else (BUNDLE_MODULE + "-<version>.jar")

    if args.run:
        if args.jar is None:
            require((worktree / "mvnw").exists(),
                    "no ./mvnw in --worktree (pass --jar --jar-source-commit to skip the build)")
            require(image_commit is not None, "cannot resolve git HEAD of --worktree")
            dirty = check_clean_worktree(worktree)
            if dirty and not args.allow_dirty:
                raise ValueError("--worktree has uncommitted changes; commit first or pass "
                                 "--allow-dirty: " + dirty.splitlines()[0][:200] + "...")
        else:
            require(image_commit is not None, "--jar needs --jar-source-commit")
            require(git_commit_exists(worktree, image_commit),
                    "--jar-source-commit %s is not a commit in --worktree" % image_commit)
        # Always rebuild from the current worktree unless an explicit --jar was supplied, so a
        # stale target/ jar can never be pushed as if it came from HEAD.
        pre, _ = build_steps(args, worktree, remote_dir, context, image_ref, jar_name,
                             include_mvn=args.jar is None)
        collected = execute(args, pre, {})
        jar = find_bundle_jar(worktree, jar=args.jar)
        require(jar is not None, "mvn finished but no bundle jar was found in target/")
        bundle_sha = feature_common.sha256_file(jar)
        jar_name = jar.name
        dockerfile = dockerfile_text(parent, jar_name, bundle_sha, commit=image_commit,
                                     card=args.feature_card)
        assembled = assemble_context(context, jar, dockerfile, bundle_sha)
        _, post = build_steps(args, worktree, remote_dir, context, image_ref, jar_name,
                              include_mvn=False)
        collected = execute(args, post, collected)
        push_digest = parse_push_digest(collected.get("docker push", ""))
        inspect_digest = parse_repo_digest(collected.get("docker inspect RepoDigests", ""))
        require(feature_common.is_digest(push_digest) or feature_common.is_digest(inspect_digest),
                "could not read back an immutable digest from push/inspect output:\n"
                + tail(collected.get("docker push", "")))
        if push_digest and inspect_digest:
            require(push_digest == inspect_digest,
                    "push digest %s disagrees with RepoDigests %s"
                    % (push_digest, inspect_digest))
        digest = push_digest or inspect_digest
        payload = {"status": "BUILT", "image": image_ref, "image_digest": digest,
                   "digest_source": "docker push" if push_digest else "docker inspect RepoDigests",
                   "push_digest": push_digest, "inspect_digest": inspect_digest,
                   "image_bundle_sha256": bundle_sha, "feature_commit": image_commit,
                   "jar_source_commit": jar_source_commit, "worktree_head": worktree_head,
                   "parent_image": parent, "tag": tag,
                   "built_at": feature_common.utc_iso(), "worktree": str(worktree),
                   "jar": str(jar), "jar_name": jar_name, "remote": args.remote,
                   "remote_dir": remote_dir, "dockerfile": dockerfile,
                   "context_dir": assembled["context"],
                   "steps": [{"step": step["step"],
                              "command": feature_common.command_text(step["argv"]),
                              "cwd": step.get("cwd")} for step in pre + post],
                   "deletes_remote_files": False,
                   "push_log_tail": tail(collected.get("docker push", ""), 20)}
    else:
        preview_sha = bundle_sha or "0" * 64
        preview_name = jar_name if jar else (BUNDLE_MODULE + "-PLACEHOLDER.jar")
        text = dockerfile_text(parent, preview_name, preview_sha, commit=image_commit,
                               card=args.feature_card)
        pre, post = build_steps(args, worktree, remote_dir, context, image_ref, preview_name,
                                include_mvn=args.jar is None)
        payload = {"status": "DRY_RUN_ONLY",
                   "requires": "--run --approved (then docker build/push on " + args.remote + ")",
                   "image": image_ref, "image_digest": None,
                   "image_bundle_sha256": bundle_sha, "feature_commit": image_commit,
                   "jar_source_commit": jar_source_commit, "worktree_head": worktree_head,
                   "parent_image": parent, "tag": tag, "built_at": None,
                   "worktree": str(worktree), "jar": str(jar) if jar else None,
                   "jar_glob": str(worktree / BUNDLE_MODULE / "target"
                                   / (BUNDLE_MODULE + "-*.jar")),
                   "jar_name": jar_name, "remote": args.remote, "remote_dir": remote_dir,
                   "context_dir": str(context), "dockerfile": text,
                   "dockerfile_sha256": feature_common.sha256_bytes(text.encode()),
                   "steps": [{"step": step["step"],
                              "command": feature_common.command_text(step["argv"]),
                              "cwd": step.get("cwd")} for step in pre + post],
                   "note": ("jar not built yet; Dockerfile uses a placeholder sha256 and is "
                            "regenerated after mvn clean package") if bundle_sha is None else
                           "jar resolved from target/; --run rebuilds it from HEAD first",
                   "deletes_remote_files": False}
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if args.out:
        Path(args.out).write_text(rendered + "\n")
    return payload


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        print("REFUSED:", error, file=sys.stderr)
        sys.exit(2)
