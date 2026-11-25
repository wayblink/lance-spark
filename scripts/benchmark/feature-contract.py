#!/usr/bin/env python3
"""Derive a feature contract from the frozen baseline contract; offline only.

Only these fields may differ from --baseline:
  image, image_digest, image_bundle_sha256, feature_commit, parent_image, feature_card
Everything else must stay semantically identical, which is verified by a canonical-JSON
comparison before anything is written. The derived file is written to a temporary name,
re-read, verified again (non-exempt fields vs baseline, exempt fields vs the intended
values, full byte hash), and only then renamed onto --out. Nothing touches the cluster,
docker or ssh.

Examples:
  ./feature-contract.py --image-json /tmp/feature-image.json --feature-card 3280 \
      --out /tmp/contract-feat.json
  ./feature-contract.py --image hub.i.basemind.com/wanganyang/sparkann-e2e:feat-3280 \
      --image-digest sha256:... --image-bundle-sha256 <64 hex> --feature-commit 5cbafef \
      --parent-image hub.i.basemind.com/wanganyang/sparkann-e2e@sha256:... \
      --feature-card 3280 --out /tmp/contract-feat.json
"""
import argparse
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import baseline
import feature_common


def require(ok, message):
    feature_common.require(ok, message)


def normalize_card(value):
    require(feature_common.CARD_RE.fullmatch(str(value).strip()) is not None,
            "feature_card must look like 3280 or #3280")
    return str(value).strip().lstrip("#")


def derive(base, image, image_digest, image_bundle_sha256, feature_commit,
           parent_image, feature_card):
    """Return baseline plus the six accounted fields, refusing anything else changed."""
    baseline.validate_contract(base)
    require(isinstance(image, str) and image and image == image.strip(), "image must be a tag-qualified repo")
    require("@" not in image, "pass the tag in image and the digest separately")
    feature_common.require_digest_ref(image + "@" + str(image_digest), "derived image")
    require(feature_common.is_sha256_hex(image_bundle_sha256),
            "image_bundle_sha256 must be 64 lowercase hex characters")
    require(feature_common.is_commit(feature_commit), "feature_commit must be 7-40 lowercase hex")
    feature_common.require_digest_ref(parent_image, "parent_image")
    card = normalize_card(feature_card)
    derived = dict(base)
    derived.update({"image": image, "image_digest": image_digest,
                    "image_bundle_sha256": image_bundle_sha256,
                    "feature_commit": feature_commit, "parent_image": parent_image,
                    "feature_card": card})
    verify_derived(base, derived)
    return derived


def verify_derived(base, derived):
    """Raise unless only accounted fields differ from the baseline contract."""
    diff = feature_common.differences(feature_common.strip_accounted(base),
                                      feature_common.strip_accounted(derived))
    require(not diff, "non-exempt contract fields changed: " + "; ".join(diff[:5]))
    for key in feature_common.ACCOUNTED_FIELDS:
        require(key in derived, "derived contract is missing " + key)
    require("@" not in str(derived["image"]), "image must not embed a digest")
    feature_common.require_digest_ref(derived["image"] + "@" + str(derived["image_digest"]),
                                      "derived image")
    require(feature_common.is_sha256_hex(derived["image_bundle_sha256"]),
            "image_bundle_sha256 must be 64 lowercase hex characters")
    require(feature_common.is_commit(derived["feature_commit"]),
            "feature_commit must be 7-40 lowercase hex")
    feature_common.require_digest_ref(derived["parent_image"], "parent_image")
    require(feature_common.CARD_RE.fullmatch(str(derived["feature_card"])) is not None,
            "feature_card must look like 3280")
    baseline.validate_contract(derived)
    return derived


def read_and_verify(path, base, expected=None):
    """Re-read a contract file and re-check both the exempt and the untouched fields."""
    loaded = json.loads(Path(path).read_text())
    verify_derived(base, loaded)
    if expected is not None:
        require(loaded == expected, "re-read contract differs from the intended one")
    return loaded


def write_contract(base, derived, out):
    """Write atomically: temp file, re-read and verify, then rename onto --out."""
    verify_derived(base, derived)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(derived, indent=2) + "\n"
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_text(payload)
    try:
        read_and_verify(tmp, base, expected=derived)
        require(feature_common.sha256_file(tmp) == feature_common.sha256_bytes(payload.encode()),
                "temporary contract bytes changed on disk")
        os.replace(str(tmp), str(out))
    finally:
        if tmp.exists():
            tmp.unlink()
    read_and_verify(out, base, expected=derived)
    return out


def pick(args, payload, flag, key):
    explicit = getattr(args, flag)
    return explicit if explicit is not None else payload.get(key)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=Path, default=HERE / "baseline-10m.json")
    parser.add_argument("--image-json", type=Path,
                        help="feature-image.py output; explicit flags still win")
    parser.add_argument("--image", help="repo:tag of the feature image")
    parser.add_argument("--image-digest", help="sha256:<64 hex> of the pushed image")
    parser.add_argument("--image-bundle-sha256", help="sha256 of the bundle jar inside it")
    parser.add_argument("--feature-commit", help="git rev-parse HEAD of the feature worktree")
    parser.add_argument("--parent-image", help="repo@sha256:<64 hex> parent image")
    parser.add_argument("--feature-card", help="card number, e.g. 3280")
    parser.add_argument("--out", type=Path, help="write the derived contract here")
    args = parser.parse_args(argv)

    require(args.baseline.is_file(), "baseline contract not found: " + str(args.baseline))
    base = json.loads(args.baseline.read_text())
    payload = json.loads(args.image_json.read_text()) if args.image_json else {}

    derived = derive(
        base,
        pick(args, payload, "image", "image"),
        pick(args, payload, "image_digest", "image_digest"),
        pick(args, payload, "image_bundle_sha256", "image_bundle_sha256"),
        pick(args, payload, "feature_commit", "feature_commit"),
        pick(args, payload, "parent_image", "parent_image"),
        pick(args, payload, "feature_card", "feature_card"),
    )
    summary = {"status": "DERIVED", "out": None,
               "contract_sha256": feature_common.sha256_bytes(
                   (json.dumps(derived, indent=2) + "\n").encode()),
               "baseline_sha256": feature_common.sha256_file(args.baseline),
               "image": derived["image"], "image_digest": derived["image_digest"],
               "parent_image": derived["parent_image"],
               "feature_commit": derived["feature_commit"],
               "feature_card": derived["feature_card"],
               "non_exempt_fields_unchanged": True,
               "same_image_digest_as_baseline":
                   derived["image_digest"] == base["image_digest"]}
    if args.out:
        path = write_contract(base, derived, args.out)
        summary["out"] = str(path)
        summary["out_sha256"] = feature_common.sha256_file(path)
        print(json.dumps(summary, indent=2))
    else:
        print(json.dumps(derived, indent=2))
        print(json.dumps(summary), file=sys.stderr)
    if derived["image_digest"] == base["image_digest"]:
        print("WARNING: derived image digest equals the baseline digest; the rebuild may be a "
              "no-op, so a comparison would measure the same jar.", file=sys.stderr)
    return summary


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        print("REFUSED:", error, file=sys.stderr)
        sys.exit(2)
