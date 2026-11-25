#!/usr/bin/env python3
"""Run independent pure-Python baseline invariant tests concurrently (no third-party packages)."""
import concurrent.futures
from pathlib import Path
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_baseline


def names(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from names(test)
        else:
            yield test.id().split("test_baseline.", 1)[-1]


def run(name):
    p = subprocess.run([sys.executable, "-m", "unittest", "test_baseline." + name],
                       cwd=Path(__file__).resolve().parent, capture_output=True, text=True)
    return name, p.returncode, p.stdout + p.stderr


suite = unittest.defaultTestLoader.loadTestsFromModule(test_baseline)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(run, names(suite)))
for name, rc, output in results:
    print(("PASS " if rc == 0 else "FAIL ") + name)
    if rc:
        print(output)
print(f"{sum(rc == 0 for _, rc, _ in results)}/{len(results)} passed; workers=4")
sys.exit(int(any(rc for _, rc, _ in results)))
