#!/usr/bin/env python3
"""Interleave two or more contracts cell by cell so shared-cluster noise hits every arm.

Why: on a shared cluster another team's benchmark can appear at any moment. Rather than voiding
a whole five-cell matrix (or waiting for a window that never opens), this tool alternates the
arms inside one round -- ``for r in 1..N: for cell in cells: for arm in arms:`` -- so whatever
noise exists is spread over both arms instead of landing on one of them.

Lifecycle: every cell runs through ``submit.prepare_matrix`` + ``submit.run_one_cell``, i.e. the
same reviewed window policy, UID ledger, evidence capture and verdict code as the formal matrix.
Each (arm, round, cell) gets its own output directory ``<output-root>/<arm>/r<round>/<kind>-k<k>``
and its own HDFS evidence root, so replicates never share artifacts.

Guarantees: strictly serial (no automatic parallelism), nothing is deleted on the remote host,
no foreign job is stopped or rescheduled, and a cell that is not ``window_usable`` stops the run
with its evidence preserved (same rule as ``submit.run_matrix``).

Dry-run is the default and only prints the plan: every round/cell/arm entry plus the exact
spark-submit command of each cell, with object-store values as placeholders (they are resolved
from the platform at --run time). Dry-run spawns no subprocess at all.

Examples:
  pair-run.py --arm base=baseline-10m.json --arm feat=feature-10m.json \\
      --queries ~/assets/queries.jsonl --truth ~/assets/truth.json \\
      --output-root ~/runs/3284-pair                     # plan only
  ... --run --approved                                   # execute after review
"""
import argparse
import json
from pathlib import Path
import shlex
import statistics
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import baseline
import feature_common
import report
import submit


def require(ok, message):
    baseline.require(ok, message)


def parse_arm(spec):
    """``name=contract.json`` -> (name, resolved contract path)."""
    require(spec.count("=") == 1, "--arm must be name=contract.json, got %r" % (spec,))
    name, path = spec.split("=", 1)
    require(name == name.strip() and name and "/" not in name and "\\" not in name
            and " " not in name, "--arm name must be a single path-free token: %r" % (name,))
    require(Path(path).is_file(), "--arm contract not found: %s" % path)
    return name, Path(path).resolve()


def parse_arms(specs):
    require(len(specs) >= 2, "at least two --arm name=contract.json are required")
    arms = [parse_arm(spec) for spec in specs]
    names = [name for name, _path in arms]
    require(len(set(names)) == len(names), "duplicate --arm name: %s" % "、".join(names))
    return arms


def parse_cells(spec, universe):
    """``single:10,batch:2000`` -> [(kind, k)] restricted to the frozen cell layout."""
    if not spec:
        return list(universe)
    cells = []
    for token in spec.split(","):
        require(":" in token, "--cells entries must be kind:k, got %r" % (token,))
        kind, raw = token.split(":", 1)
        require(raw.strip().isdigit(), "--cells k must be a non-negative integer, got %r" % (raw,))
        identity = (kind.strip(), int(raw))
        require(identity in universe,
                "--cells %s:%s is not one of the frozen cells %s"
                % (kind.strip(), raw.strip(), universe))
        cells.append(identity)
    require(len(set(cells)) == len(cells), "duplicate --cells entry")
    return cells


def cell_name(kind, k):
    return "%s-k%d" % (kind, k)


def cell_directory(root, arm, round_number, kind, k):
    return Path(root) / arm / ("r%d" % round_number) / cell_name(kind, k)


def submission_order(cells, arms, repeats):
    """The one and only order: round outermost, then cell, then arm."""
    return [{"round": round_number, "kind": kind, "k": k, "arm": name,
             "cell": cell_name(kind, k),
             "directory": cell_directory("<output-root>", name, round_number, kind, k)}
            for round_number in range(1, repeats + 1)
            for kind, k in cells
            for name, _path in arms]


def command_entry(contract, entry, root):
    """The exact spark-submit preview of one cell; no subprocess, no secret values."""
    evidence_root = contract["cluster"]["evidence_root"] + "/<run-id>"
    remote = evidence_root + "/" + entry["cell"]
    _conf, command = submit.cell_command(
        contract, entry["kind"], entry["k"],
        "<run-root>/build/BaselineBenchmark.jar",
        evidence_root + "/contract.json", evidence_root + "/truth.json",
        remote, "b3255-" + entry["cell"] + "-<uuid12>", submit.placeholder_storage())
    return {"round": entry["round"], "cell": entry["cell"], "arm": entry["arm"],
            "directory": str(cell_directory(root, entry["arm"], entry["round"],
                                            entry["kind"], entry["k"])),
            "remote": remote, "command": shlex.join(command)}


def build_plan(args):
    """Validate every arm and render the whole schedule; nothing is created or submitted."""
    contracts = {}
    for name, path in args.arms:
        contract = json.loads(path.read_text())
        baseline.validate_contract(contract)
        contracts[name] = contract
    base_name = args.arms[0][0]
    base_contract = contracts[base_name]
    for name, contract in list(contracts.items())[1:]:
        diff = feature_common.differences(
            feature_common.strip_accounted(base_contract),
            feature_common.strip_accounted(contract))
        require(not diff, "arms %s and %s changed frozen five-cell parameters: %s"
                % (base_name, name, "; ".join(diff[:5])))
    arms = [{"name": name, "contract": str(path),
             "contract_sha256": report.sha256_file(path),
             "image": contracts[name]["image"] + "@" + contracts[name]["image_digest"],
             "image_digest": contracts[name]["image_digest"]}
            for name, path in args.arms]
    root = Path(args.output_root)
    schedule = submission_order(args.cells, args.arms, args.repeats)
    for entry in schedule:
        entry["directory"] = str(cell_directory(root, entry["arm"], entry["round"],
                                                entry["kind"], entry["k"]))
    warnings = [
        "shared-cluster noise is only recorded (verdict.foreign_observed/contamination); "
        "interleaving spreads it across arms instead of waiting for a clean window",
        "cells run strictly serially in round/cell/arm order; nothing is parallelised, "
        "deleted or stopped",
    ]
    digests = {arm["image_digest"] for arm in arms}
    if len(digests) == 1:
        warnings.insert(0, "every arm uses the same image_digest: this is an A/A noise "
                           "measurement, not a feature comparison")
    plan = {
        "status": "DRY_RUN_ONLY",
        "arms": arms,
        "cells": [{"kind": kind, "k": k, "name": cell_name(kind, k)} for kind, k in args.cells],
        "repeats": args.repeats,
        "order": "for round in 1..N: for cell in cells: for arm in arms",
        "output_root": str(root),
        "queries": str(args.queries),
        "truth": str(args.truth),
        "stopped_evidence": str(args.stopped_evidence) if args.stopped_evidence else None,
        "window_seconds": args.window_seconds,
        "parallel": False,
        "deletes_remote_files": False,
        "stops_foreign_jobs": False,
        "guards": [{"path": str(root), "policy": "create-only"}],
        "warnings": warnings,
        "dry_run_note": "dry-run prints the whole schedule and every cell command and spawns "
                        "no subprocess; object-store values are placeholders resolved from the "
                        "platform at --run time, and <run-id>/<uuid12>/<run-root> are real "
                        "per-cell values generated when that cell actually runs",
        "schedule": schedule,
        "commands": [command_entry(contracts[entry["arm"]], entry, root) for entry in schedule],
        "report": str(root / "pair-report.md"),
        "verdict": str(root / "pair-verdict.json"),
        "requires": "--run --approved (root review and user authorization first)",
    }
    return plan


# ---------------------------------------------------------------------------------- aggregation

def collect_rounds(root, arms, cells, repeats, contracts):
    """Read each finished cell through report.compute_cell; no statistic is recomputed here."""
    collected = {}
    for name in arms:
        for kind, k in cells:
            collected[(name, kind, k)] = [
                report.compute_cell(kind, k, cell_directory(root, name, round_number, kind, k),
                                    "r%d" % round_number, contracts[name], True,
                                    report.OUTLIER_FACTOR)
                for round_number in range(1, repeats + 1)]
    return collected


def arm_cell(rounds):
    """One arm's pooled p50 over rounds, shaped for ``report.ratio_text``."""
    values = [cell["p50"] for cell in rounds if cell["p50"] is not None]
    return {"unavailable": None if values else "无可用 p50",
            "p50": statistics.median(values) if values else None,
            "complete": bool(values) and all(cell["complete"] for cell in rounds)}


def dispersion_pct(values):
    usable = [value for value in values if value is not None]
    if len(usable) < 2 or statistics.median(usable) <= 0:
        return None
    return (max(usable) - min(usable)) / statistics.median(usable) * 100.0


def dispersion_text(values):
    usable = [value for value in values if value is not None]
    if len(usable) < 2:
        return "单轮，无离散度"
    percent = dispersion_pct(usable)
    if percent is None:
        return "n/a（中位数为 0）"
    return "%.1f%%（span %.1f ms）" % (percent, max(usable) - min(usable))


def cell_contamination(cell):
    contamination = ((cell.get("verdict") or {}).get("contamination"))
    return contamination


def render_pair(collected, arms, cells):
    lines = ["## 交错对照：每格每臂中位数（轮次离散度）", "",
             "| 场景 | k | 臂 | 各轮 p50 | 中位数 | 离散度 | 污染轮次 |",
             "| --- | --- | --- | --- | --- | --- | --- |"]
    for kind, k in cells:
        label = report.scene_label(kind, collected[(arms[0], kind, k)][0])
        for arm in arms:
            rounds = collected[(arm, kind, k)]
            values = [cell["p50"] for cell in rounds]
            shown = "、".join("n/a" if value is None else "%.1f" % value for value in values)
            dirty = ["R%d %s" % (index, cell_contamination(cell))
                     for index, cell in enumerate(rounds, 1) if cell_contamination(cell)]
            lines.append("| %s | %d | %s | %s | %s | %s | %s |" % (
                report.md_cell(label), k, report.md_cell(arm), report.md_cell(shown),
                report.fmt_ms(arm_cell(rounds)["p50"]), report.md_cell(dispersion_text(values)),
                report.md_cell("；".join(dirty) if dirty else "—")))
    lines += ["", "## 成对比较（对照臂 = `%s`）" % arms[0], "",
              "| 场景 | k | 对照臂中位数 | 对比臂中位数 | 比值 | 同轮差值 |",
              "| --- | --- | --- | --- | --- | --- |"]
    for kind, k in cells:
        label = report.scene_label(kind, collected[(arms[0], kind, k)][0])
        control_rounds = collected[(arms[0], kind, k)]
        control = arm_cell(control_rounds)
        for arm in arms[1:]:
            treatment_rounds = collected[(arm, kind, k)]
            treatment = arm_cell(treatment_rounds)
            deltas = []
            for index, (left, right) in enumerate(zip(control_rounds, treatment_rounds), 1):
                if left["p50"] is None or right["p50"] is None:
                    deltas.append("R%d n/a" % index)
                else:
                    deltas.append("R%d %+.1f ms" % (index, right["p50"] - left["p50"]))
            lines.append("| %s | %d | %s | %s | %s | %s |" % (
                report.md_cell(label), k, report.fmt_ms(control["p50"]),
                report.fmt_ms(treatment["p50"]),
                report.md_cell(report.ratio_text(control, treatment)),
                report.md_cell("、".join(deltas))))
    lines += ["", "- 单位 ms；每格每臂中位数 = 该臂各轮 p50 的中位数（轮次就是交错重复）。",
              "- 同轮差值 = 该轮 对比臂 p50 − 对照臂 p50（正 = 对比臂更慢），用来看差值是否跨轮稳定；"
              "`*` 表示该臂有轮次 measured 不完整。",
              "- 污染轮次来自 verdict.contamination；交错的目的就是让共享集群噪声均摊到各臂，"
              "而不是等一个空窗。"]
    return lines


def verdict_document(plan, arms, cells, collected, completed):
    document = {"status": "COMPLETED", "order": plan["order"], "repeats": plan["repeats"],
                "parallel": False, "deletes_remote_files": False, "stops_foreign_jobs": False,
                "arms": {arm["name"]: {"contract": arm["contract"],
                                       "contract_sha256": arm["contract_sha256"],
                                       "image": arm["image"],
                                       "image_digest": arm["image_digest"]}
                         for arm in plan["arms"]},
                "schedule": [{key: entry[key] for key in ("round", "cell", "arm", "directory")}
                             for entry in completed],
                "cells": []}
    for kind, k in cells:
        for arm in arms:
            rounds = collected[(arm, kind, k)]
            document["cells"].append({
                "arm": arm, "cell": cell_name(kind, k), "k": k,
                "rounds": [{"round": index, "p50": cell["p50"], "p95": cell["p95"],
                            "per_query": cell["per_query"], "measured": cell["measured"],
                            "complete": cell["complete"],
                            "contamination": cell_contamination(cell),
                            "window_usable": (cell.get("verdict") or {}).get("window_usable")}
                           for index, cell in enumerate(rounds, 1)],
                "median_p50": arm_cell(rounds)["p50"],
                "dispersion_pct": dispersion_pct([cell["p50"] for cell in rounds])})
    return document


# ------------------------------------------------------------------------------------ execution

def execute(plan):
    """Run the plan exactly as printed: serial, create-only, never deleting or stopping anything."""
    root = Path(plan["output_root"])
    require(not root.exists(), "refuse pre-existing output root (create-only): " + str(root))
    paths = {arm["name"]: Path(arm["contract"]) for arm in plan["arms"]}
    arms = [arm["name"] for arm in plan["arms"]]
    cells = [(cell["kind"], cell["k"]) for cell in plan["cells"]]
    stopped = Path(plan["stopped_evidence"]) if plan["stopped_evidence"] else None
    completed = []
    current = None
    try:
        for round_number in range(1, plan["repeats"] + 1):
            # Per round each arm gets its own run root: manifest, compiled jar, HDFS evidence root.
            contexts = {}
            for name in arms:
                contexts[name] = submit.prepare_matrix(
                    json.loads(paths[name].read_text()), paths[name], Path(plan["queries"]),
                    Path(plan["truth"]), root / name / ("r%d" % round_number), stopped)
            for kind, k in cells:
                for name in arms:
                    current = {"round": round_number, "cell": cell_name(kind, k), "arm": name,
                               "directory": str(cell_directory(root, name, round_number, kind, k))}
                    # ``run_one_cell`` refuses an unusable cell by raising. In a paired run that
                    # must not throw away the rounds that already succeeded, so the failure is
                    # recorded and the plan continues; the cell simply has no comparable sample.
                    try:
                        records, verdict = submit.run_one_cell(contexts[name], kind, k,
                                                               plan["window_seconds"])
                    except Exception as exc:
                        completed.append({**current, "samples": 0, "window_usable": False,
                                          "unusable": "%s: %s" % (type(exc).__name__, exc)})
                        continue
                    completed.append({**current, "samples": len(records),
                                      "window_usable": verdict.get("window_usable"),
                                      "exclusive_window_valid": verdict.get("exclusive_window_valid"),
                                      "contamination": verdict.get("contamination")})
    except Exception as exc:
        if root.exists():
            (root / "pair-status.json").write_text(json.dumps(
                {"status": "FAILED", "failed_entry": current, "completed": completed,
                 "error": "%s: %s" % (type(exc).__name__, exc),
                 "note": "evidence kept; nothing was deleted or stopped"}, indent=2) + "\n")
        raise
    contracts = {name: json.loads(paths[name].read_text()) for name in arms}
    collected = collect_rounds(root, arms, cells, plan["repeats"], contracts)
    (root / "pair-report.md").write_text("\n".join(render_pair(collected, arms, cells)) + "\n")
    (root / "pair-verdict.json").write_text(
        json.dumps(verdict_document(plan, arms, cells, collected, completed), indent=2) + "\n")
    return {"status": "COMPLETED", "output_root": str(root), "cells_run": len(completed),
            "report": str(root / "pair-report.md"),
            "verdict": str(root / "pair-verdict.json")}


# ------------------------------------------------------------------------------------------ cli

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--arm", action="append", required=True, metavar="name=contract.json",
                        help="one arm per contract; repeatable, at least two; the first is the control")
    parser.add_argument("--cells", help="cell filter, e.g. single:10,batch:2000 (default: all five)")
    parser.add_argument("--repeats", type=int, default=2, help="interleaved rounds (default 2)")
    parser.add_argument("--queries", required=True,
                        help="path on the runner host of the frozen 100 query vectors")
    parser.add_argument("--truth", required=True,
                        help="path on the runner host of the frozen full top-2000 truth")
    parser.add_argument("--output-root", required=True,
                        help="new output directory (create-only): <root>/<arm>/r<round>/<kind>-k<k>")
    parser.add_argument("--stopped-evidence", type=Path,
                        help="UID-fenced JSON of drivers verified stopped before the run")
    parser.add_argument("--window-seconds", type=int, default=1800,
                        help="per-cell wait for a clean window before proceeding anyway")
    parser.add_argument("--dry-run", action="store_true", help="print the plan only (default)")
    parser.add_argument("--run", action="store_true", help="execute the plan")
    parser.add_argument("--approved", action="store_true",
                        help="required with --run after root review and user authorization")
    args = parser.parse_args(argv)
    if args.run and args.dry_run:
        parser.error("--run and --dry-run are mutually exclusive")
    if args.run and not args.approved:
        parser.error("--run requires --approved after root review and user authorization")
    args.arms = parse_arms(args.arm)
    universe = tuple(baseline.cells(json.loads(args.arms[0][1].read_text())))
    args.cells = parse_cells(args.cells, universe)
    require(1 <= args.repeats <= 20, "--repeats must be between 1 and 20")
    require(args.window_seconds >= 0, "--window-seconds must be >= 0")
    return args


def main(argv=None):
    args = parse_args(argv)
    plan = build_plan(args)
    if not args.run:
        print(json.dumps(plan, indent=2))
        return plan
    result = execute(plan)
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        print("REFUSED:", error, file=sys.stderr)
        sys.exit(2)
