"""UBSan-only test helpers: per-test log layout and failure collection.

Everything here is a no-op unless the process runs under UBSan (detected via
`UBSAN_OPTIONS` carrying a `log_path=`). Kept out of conftest.py / instance.py so
the normal, non-sanitized test path never touches it.
"""

import os
import shutil


def is_active() -> bool:
    """True when running under UBSan with per-test logging configured."""
    return "log_path=" in os.environ.get("UBSAN_OPTIONS", "")


def logs_base():
    """Base dir UBSan writes to (dirname of the UBSAN_OPTIONS log_path), or None."""
    for part in os.environ.get("UBSAN_OPTIONS", "").split(":"):
        if part.startswith("log_path="):
            return os.path.dirname(part[len("log_path=") :]) or "."
    return None


def _sanitize(s):
    s = "".join(c if c.isalnum() or c in "._-" else "_" for c in s).strip("_")
    return s or "unknown"


def log_subdir(base_dir, test_id):
    """`<base_dir>/<suite>/<case>` for a PYTEST_CURRENT_TEST / nodeid style id."""
    raw = test_id.split(" (")[0]
    file_part, _, case_part = raw.partition("::")
    suite = os.path.basename(file_part)
    if suite.endswith(".py"):
        suite = suite[:-3]
    return os.path.join(base_dir, _sanitize(suite)[:80], _sanitize(case_part)[:120])


def child_env():
    """Child env with a per-test UBSAN_OPTIONS log_path, or None if not under UBSan.

    Points log_path at `<base>/<suite>/<case>/ubsan` so each test gets its own
    folder of `ubsan.<pid>` files (no cross-test PID clobber). None lets Popen
    inherit os.environ unchanged.
    """
    opts = os.environ.get("UBSAN_OPTIONS", "")
    if "log_path=" not in opts:
        return None

    test_id = os.environ.get("PYTEST_CURRENT_TEST", "unknown")
    parts = opts.split(":")
    for i, part in enumerate(parts):
        if part.startswith("log_path="):
            base_dir = os.path.dirname(part[len("log_path=") :]) or "."
            sub_dir = log_subdir(base_dir, test_id)
            os.makedirs(sub_dir, exist_ok=True)
            parts[i] = f"log_path={os.path.join(sub_dir, 'ubsan')}"
    env = os.environ.copy()
    env["UBSAN_OPTIONS"] = ":".join(parts)
    return env


def collect_failure(item, report, log_dir):
    """Co-locate a failed test's report + server logs with its ubsan.<pid> findings."""
    base = logs_base()
    if not base:
        return
    sub_dir = log_subdir(base, item.nodeid)
    os.makedirs(sub_dir, exist_ok=True)
    with open(os.path.join(sub_dir, "test-failure.log"), "a") as fh:
        fh.write(f"\n===== {report.when} {report.outcome}: {report.nodeid} =====\n")
        fh.write((report.longreprtext or "") + "\n")
        for label, text in (
            ("captured stdout", report.capstdout),
            ("captured stderr", report.capstderr),
            ("captured log", report.caplog),
        ):
            if text:
                fh.write(f"----- {label} -----\n{text}\n")
    if log_dir and os.path.isdir(log_dir):
        for f in os.listdir(log_dir):
            src = os.path.join(log_dir, f)
            if os.path.isfile(src):
                try:
                    shutil.copy(src, sub_dir)
                except OSError:
                    pass
