#!/usr/bin/env python3
"""Compare a core YAML suite run with plugin/src/yamlRestTest/core-denylist.txt.

    scripts/core-denylist.py default|proxy|lucene [results-dir]

Reads the JUnit XML the task wrote (plugin/build/test-results/<task>/ by
default) and prints:

  - the failures no entry for that mode covers, as denylist lines with the tag
    left as [?] and the first line of the failure as the reason, to tag and
    paste into the file (the build refuses an untagged line);
  - after a run with -Dinfino.core.skip=none, the entries for that mode whose
    tests that ran all passed, which can be removed;
  - the [bug] entries a [gap] or [cluster] entry already skips in full, which
    -Dinfino.core.skip=gaps cannot run.

A failure outside any YAML test (a cluster that never started, a suite that
could not load) is reported as uncovered too. The script exits 1 when any
failure is uncovered, so a run can be gated on it.

Entries are selected and matched as the build and the YAML runner do: a
pattern matches the end of a test's path, and * stands for any run of
characters within one path segment.
"""

import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DENYLIST = os.path.join(ROOT, "plugin", "src", "yamlRestTest", "core-denylist.txt")
TASKS = {
    "default": "coreYamlRestTest",
    "proxy": "coreYamlRestTestProxy",
    "lucene": "coreYamlRestTestLucene",
}
LINE = re.compile(r"^(.+?)\s+#\s*\[(gap|bug|cluster)(?::(default|proxy))?\]\s*\S.*$")
NAME = re.compile(r"\{yaml=(.*)\}$")


def regex_of(pattern):
    """A pattern as the runner reads it: the end of a path, * within one segment."""
    return re.compile(".*" + re.escape(pattern).replace(r"\*", "[^/]*"))


def entries(mode):
    """The denylist entries a task in this mode skips under -Dinfino.core.skip=all,
    as (line, kind, regex)."""
    out = []
    with open(DENYLIST, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            m = LINE.match(line)
            if not m or (m.group(2) == "cluster" and m.group(3)):
                sys.exit(f"core-denylist.txt: no [tag] and reason: {line}")
            pattern, kind, only = m.groups()
            if kind == "cluster" or (mode != "lucene" and only in (None, mode)):
                out.append((line, kind, regex_of(pattern)))
    return out


def results(directory):
    """Each test's path and outcome: passed, skipped, or the first line of its
    failure. A testcase that is not a YAML test is keyed by its class and name,
    and kept only when it failed."""
    out = {}
    for path in glob.glob(os.path.join(directory, "*.xml")):
        for case in ET.parse(path).getroot().iter("testcase"):
            m = NAME.search(case.get("name", ""))
            failed = case.find("failure")
            if failed is None:
                failed = case.find("error")
            if failed is not None:
                message = (
                    (failed.get("message") or failed.text or "failed")
                    .strip()
                    .splitlines()
                )
                key = (
                    m.group(1)
                    if m
                    else f"{case.get('classname', '?')}.{case.get('name', '?')}"
                )
                out[key] = message[0] if message else "failed"
            elif not m:
                continue
            elif case.find("skipped") is not None:
                out[m.group(1)] = "skipped"
            else:
                out[m.group(1)] = "passed"
    return out


def main():
    if len(sys.argv) not in (2, 3) or sys.argv[1] not in TASKS:
        sys.exit(__doc__)
    mode = sys.argv[1]
    directory = (
        sys.argv[2]
        if len(sys.argv) == 3
        else os.path.join(ROOT, "plugin", "build", "test-results", TASKS[mode])
    )
    tests = results(directory)
    if not tests:
        sys.exit(f"no YAML test results in {directory}")
    listed = entries(mode)

    uncovered = sorted(
        t
        for t, outcome in tests.items()
        if outcome not in ("passed", "skipped")
        and not any(r.fullmatch(t) for _, _, r in listed)
    )
    print(f"# {len(uncovered)} failures in {TASKS[mode]} that no entry covers")
    for t in uncovered:
        if "/" in t:
            print(f"{t}  # [?] {tests[t][:200]}")
        else:
            print(f"# not a YAML test, so no entry can cover it: {t}: {tests[t][:200]}")

    passing = []
    for line, _, regex in listed:
        # Tests the YAML file skips itself (by version or feature) never run,
        # so they neither keep an entry nor clear it.
        ran = [tests[t] for t in tests if regex.fullmatch(t) and tests[t] != "skipped"]
        if ran and all(outcome == "passed" for outcome in ran):
            passing.append(line)
    print(
        f"\n# {len(passing)} entries whose tests that ran all passed (run with -Dinfino.core.skip=none to see them all)"
    )
    for line in passing:
        print(line)

    hidden = []
    for line, kind, regex in listed:
        if kind != "bug":
            continue
        matched = [t for t in tests if regex.fullmatch(t)]
        wider = [
            other
            for other, k, r in listed
            if k != "bug" and matched and all(r.fullmatch(t) for t in matched)
        ]
        if wider:
            hidden.append((line, wider[0]))
    print(f"\n# {len(hidden)} [bug] entries a [gap] or [cluster] entry skips in full")
    for line, wider in hidden:
        print(f"{line}\n#   skipped by: {wider}")

    sys.exit(1 if uncovered else 0)


if __name__ == "__main__":
    main()
