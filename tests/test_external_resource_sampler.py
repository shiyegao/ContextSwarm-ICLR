from __future__ import annotations

import json
from pathlib import Path
import stat
import tempfile
import unittest

from scripts.external_resource_sampler import (
    ProcessSnapshotter,
    SCHEMA_VERSION,
    _parse_proc_stat,
    build_summary,
    run_sampler,
)


ROOT = Path(__file__).resolve().parents[1]


def _stat_line(
    pid: int,
    *,
    comm: str = "service",
    ppid: int = 1,
    pgrp: int | None = None,
    starttime: int = 100,
    utime: int = 10,
    stime: int = 5,
    threads: int = 1,
    rss_pages: int = 4,
) -> str:
    tail = ["0"] * 22
    tail[0] = "S"
    tail[1] = str(ppid)
    tail[2] = str(pgrp if pgrp is not None else pid)
    tail[11] = str(utime)
    tail[12] = str(stime)
    tail[17] = str(threads)
    tail[19] = str(starttime)
    tail[21] = str(rss_pages)
    return f"{pid} ({comm}) {' '.join(tail)}\n"


def _write_process(
    proc_root: Path,
    pid: int,
    *,
    comm: str = "service",
    ppid: int = 1,
    pgrp: int | None = None,
    starttime: int = 100,
    rss_kb: int = 16,
    pss_kb: int | None = 12,
    utime: int = 10,
    stime: int = 5,
    threads: int = 1,
    cgroup: str = "/shared",
) -> None:
    root = proc_root / str(pid)
    root.mkdir(parents=True)
    root.joinpath("stat").write_text(
        _stat_line(
            pid,
            comm=comm,
            ppid=ppid,
            pgrp=pgrp,
            starttime=starttime,
            utime=utime,
            stime=stime,
            threads=threads,
            rss_pages=max(0, rss_kb // 4),
        ),
        encoding="ascii",
    )
    root.joinpath("status").write_text(
        f"Name:\t{comm}\nVmRSS:\t{rss_kb} kB\nThreads:\t{threads}\n"
        "voluntary_ctxt_switches:\t3\nnonvoluntary_ctxt_switches:\t2\n",
        encoding="ascii",
    )
    if pss_kb is not None:
        root.joinpath("smaps_rollup").write_text(f"Pss:\t{pss_kb} kB\n", encoding="ascii")
    root.joinpath("cgroup").write_text(f"0::{cgroup}\n", encoding="ascii")


def _write_cgroup(cgroup_root: Path, relative: str, *, memory: int = 1000, peak: int = 1200) -> None:
    root = cgroup_root / relative.lstrip("/")
    root.mkdir(parents=True)
    root.joinpath("memory.current").write_text(str(memory), encoding="ascii")
    root.joinpath("memory.peak").write_text(str(peak), encoding="ascii")
    root.joinpath("pids.current").write_text("3", encoding="ascii")
    root.joinpath("pids.max").write_text("max", encoding="ascii")
    root.joinpath("memory.events").write_text("oom 1\n oom_kill 0\n", encoding="ascii")
    root.joinpath("cpu.stat").write_text(
        "usage_usec 10000\nuser_usec 7000\nsystem_usec 3000\n"
        "nr_periods 20\nnr_throttled 2\nthrottled_usec 400\n",
        encoding="ascii",
    )


class ExternalSamplerTests(unittest.TestCase):
    def test_proc_stat_handles_parenthesis_in_comm_and_uses_linux_indices(self) -> None:
        parsed = _parse_proc_stat(
            _stat_line(7, comm="worker)helper", ppid=2, pgrp=7, utime=20, stime=4, threads=3, rss_pages=8),
            pid=7,
            hz=100,
            page_size=4096,
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["comm"], "worker_helper")
        self.assertEqual(parsed["ppid"], 2)
        self.assertEqual(parsed["pgrp"], 7)
        self.assertEqual(parsed["thread_count"], 3)
        self.assertEqual(parsed["rss_bytes"], 8 * 4096)
        self.assertEqual(parsed["cpu_user_seconds"], 0.2)
        self.assertEqual(parsed["cpu_system_seconds"], 0.04)

    def test_snapshot_separates_layers_and_reports_cgroup_overlap(self) -> None:
        with tempfile.TemporaryDirectory(prefix="external-sampler-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            proc = root / "proc"
            cgroup = root / "cgroup"
            proc.mkdir()
            cgroup.mkdir()
            _write_cgroup(cgroup, "shared")
            _write_cgroup(cgroup, "worker", memory=500, peak=650)
            _write_process(proc, 101, comm="judge", pgrp=101, ppid=1, rss_kb=100, pss_kb=60, cgroup="/shared")
            _write_process(proc, 102, comm="lean", pgrp=101, ppid=101, rss_kb=80, pss_kb=40, cgroup="/shared")
            _write_process(proc, 999, comm="sampler", pgrp=101, ppid=101, rss_kb=20, pss_kb=10, cgroup="/shared")
            _write_process(proc, 201, comm="router", pgrp=201, ppid=1, rss_kb=50, pss_kb=30, cgroup="/shared")
            _write_process(proc, 301, comm="worker", pgrp=301, ppid=1, rss_kb=40, pss_kb=20, cgroup="/worker")
            snapshotter = ProcessSnapshotter(
                {"backend": [101], "router": [201], "worker": [301]},
                proc_root=proc,
                cgroup_root=cgroup,
                hz=100,
                page_size=4096,
                exclude_pids=(999,),
            )
            sample = snapshotter.snapshot(sample_index=0, monotonic_seconds=10.0)
            self.assertEqual(sample["schema_version"], SCHEMA_VERSION)
            self.assertEqual(sample["layers"]["backend"]["pid_count"], 2)
            self.assertFalse(sample["layers"]["backend"]["membership_truncated"])
            self.assertEqual(sample["layers"]["backend"]["rss_bytes"], 180 * 1024)
            self.assertEqual(sample["layers"]["backend"]["pss_bytes"], 100 * 1024)
            self.assertEqual(sample["layers"]["backend"]["cpu_time_seconds"], 0.3)
            self.assertEqual(sample["layers"]["worker"]["cgroup_memory_current_bytes"], 500)
            self.assertEqual(sample["layers"]["backend"]["cgroup_memory_current_bytes"], 1000)
            self.assertEqual(sample["layers"]["worker"]["cgroup_oom_kill_count"], 0)
            self.assertTrue(sample["layer_cgroup_overlap"])
            self.assertIn("layer_cgroup_overlap", sample["warnings"])
            observer = sample["observer"]
            self.assertGreaterEqual(observer["proc_rows_seen"], 5)
            self.assertGreaterEqual(observer["proc_stat_reads"], observer["proc_rows_seen"])
            self.assertEqual(
                observer["proc_stat_reads"],
                observer["proc_rows_seen"] + observer["detailed_process_reads"],
            )
            self.assertGreaterEqual(observer["candidate_pid_count"], 3)
            self.assertEqual(observer["detailed_process_reads"], observer["candidate_pid_count"])
            self.assertEqual(observer["detailed_process_count"], observer["candidate_pid_count"])
            self.assertEqual(observer["status_reads"], observer["detailed_process_count"])
            self.assertEqual(observer["smaps_rollup_reads"], observer["detailed_process_count"])
            self.assertGreaterEqual(observer["cgroup_file_reads"], 5)
            self.assertGreaterEqual(observer["snapshot_seconds"], 0)
            self.assertGreaterEqual(observer["snapshot_cpu_seconds"], 0)
            # No raw cgroup path or command line is serialized.
            rendered = json.dumps(sample, sort_keys=True)
            self.assertNotIn("/shared", rendered)
            self.assertNotIn("/worker", rendered)
            self.assertNotIn("--secret", rendered)

    def test_snapshot_reports_observer_wall_and_cpu_time(self) -> None:
        class Clock:
            def __init__(self, values: list[float]) -> None:
                self.values = iter(values)

            def __call__(self) -> float:
                return next(self.values)

        with tempfile.TemporaryDirectory(prefix="external-sampler-overhead-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            proc = root / "proc"
            cgroup = root / "cgroup"
            proc.mkdir()
            cgroup.mkdir()
            _write_process(proc, 101, pgrp=101, ppid=1, cgroup="/")
            snapshotter = ProcessSnapshotter(
                {"backend": [101]},
                proc_root=proc,
                cgroup_root=cgroup,
                hz=100,
                page_size=4096,
                observer_clock=Clock([10.0, 10.25]),
                observer_cpu_clock=Clock([2.0, 2.015]),
            )
            sample = snapshotter.snapshot(monotonic_seconds=10.0)
            self.assertEqual(sample["observer"]["snapshot_seconds"], 0.25)
            self.assertEqual(sample["observer"]["snapshot_cpu_seconds"], 0.015)

    def test_pid_reuse_is_fail_closed_for_recycled_member(self) -> None:
        with tempfile.TemporaryDirectory(prefix="external-sampler-reuse-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            proc = root / "proc"
            cgroup = root / "cgroup"
            proc.mkdir()
            cgroup.mkdir()
            _write_process(proc, 101, pgrp=101, starttime=10, pss_kb=1, cgroup="/")
            snapshotter = ProcessSnapshotter(
                {"backend": [101]}, proc_root=proc, cgroup_root=cgroup, hz=100, page_size=4096
            )
            first = snapshotter.snapshot(monotonic_seconds=1.0)
            self.assertEqual(first["layers"]["backend"]["pid_count"], 1)
            (proc / "101" / "stat").write_text(
                _stat_line(101, pgrp=999, starttime=20), encoding="ascii"
            )
            second = snapshotter.snapshot(monotonic_seconds=2.0)
            self.assertEqual(second["layers"]["backend"]["live_root_pid_count"], 0)
            self.assertTrue(any("root_pid_reused" in item for item in second["warnings"]))

    def test_run_sampler_writes_bounded_stream_and_baseline_delta(self) -> None:
        class Clock:
            now = 100.0

            def monotonic(self) -> float:
                return self.now

            def wait(self, seconds: float) -> None:
                self.now += seconds

        with tempfile.TemporaryDirectory(prefix="external-sampler-run-", dir=str(ROOT)) as temporary:
            root = Path(temporary)
            proc = root / "proc"
            cgroup = root / "cgroup"
            proc.mkdir()
            cgroup.mkdir()
            _write_process(proc, 101, pgrp=101, starttime=10, rss_kb=10, pss_kb=5, utime=10, stime=0, cgroup="/")
            clock = Clock()
            snapshotter = ProcessSnapshotter(
                {"backend": [101]}, proc_root=proc, cgroup_root=cgroup, hz=100, page_size=1024
            )
            # Mutate process counters after the baseline on the first wait.
            original_wait = clock.wait

            def wait(seconds: float) -> None:
                original_wait(seconds)
                (proc / "101" / "stat").write_text(
                    _stat_line(101, pgrp=101, starttime=10, utime=30, stime=2, rss_pages=20),
                    encoding="ascii",
                )

            summary = run_sampler(
                snapshotter,
                output_dir=root / "out",
                duration_seconds=1.0,
                interval_seconds=1.0,
                max_samples=2,
                monotonic=clock.monotonic,
                wall_clock=lambda: "2026-01-01T00:00:00+00:00",
                wait=wait,
            )
            self.assertEqual(summary["schema_version"], SCHEMA_VERSION)
            self.assertEqual(summary["run_window"]["sample_count"], 1)
            self.assertEqual(clock.now, 101.0)
            delta = summary["delta_from_baseline"]["layers"]["backend"]
            self.assertEqual(delta["cpu_time_seconds"], 0.22)
            self.assertGreater(delta["cpu_percent_one_core"], 0)
            self.assertIn("not per-job attribution", summary["interpretation"]["cpu"])
            observer_summary = summary["observer"]["run_window"]
            self.assertEqual(observer_summary["sample_count"], 1)
            self.assertGreater(observer_summary["total_proc_rows_seen"], 0)
            self.assertGreaterEqual(observer_summary["max_snapshot_seconds"], 0)
            self.assertIn("240-PID", summary["interpretation"]["observer"])
            output = root / "out"
            self.assertEqual(stat.S_IMODE((output / "samples.jsonl").stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE((output / "summary.json").stat().st_mode), 0o600)
            lines = (output / "samples.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[0])["schema_version"], SCHEMA_VERSION)
            self.assertEqual(json.loads(lines[1])["schema_version"], SCHEMA_VERSION)
            self.assertEqual(json.loads(lines[0])["kind"], "baseline")
            self.assertEqual(json.loads(lines[1])["kind"], "sample")

    def test_build_summary_handles_missing_cpu_and_empty_window(self) -> None:
        baseline = {
            "schema_version": SCHEMA_VERSION,
            "monotonic_seconds": 1.0,
            "host_cpu_count": 2,
            "layers": {"backend": {"cpu_time_seconds": None, "rss_bytes": 100}},
            "warnings": [],
        }
        summary = build_summary(
            baseline,
            [],
            started_at="a",
            finished_at="b",
        )
        self.assertEqual(summary["run_window"]["sample_count"], 0)
        self.assertIsNone(summary["delta_from_baseline"]["layers"]["backend"]["cpu_time_seconds"])
        self.assertIsNone(summary["delta_from_baseline"]["layers"]["backend"]["cpu_percent_one_core"])

    def test_build_summary_aggregates_observer_rows(self) -> None:
        baseline = {
            "schema_version": SCHEMA_VERSION,
            "monotonic_seconds": 1.0,
            "host_cpu_count": 1,
            "layers": {"backend": {"rss_bytes": 100, "cpu_time_seconds": 1.0}},
            "observer": {"snapshot_seconds": 0.1},
            "warnings": [],
        }
        samples = [
            {
                "monotonic_seconds": 2.0,
                "layers": {"backend": {"rss_bytes": 100, "cpu_time_seconds": 1.1}},
                "observer": {
                    "proc_rows_seen": 10,
                    "snapshot_seconds": 0.02,
                    "snapshot_cpu_seconds": 0.01,
                },
                "warnings": [],
            },
            {
                "monotonic_seconds": 3.0,
                "layers": {"backend": {"rss_bytes": 100, "cpu_time_seconds": 1.2}},
                "observer": {
                    "proc_rows_seen": 12,
                    "snapshot_seconds": 0.03,
                    "snapshot_cpu_seconds": 0.02,
                },
                "warnings": [],
            },
        ]
        summary = build_summary(baseline, samples, started_at="a", finished_at="b")
        window = summary["observer"]["run_window"]
        self.assertEqual(window["sample_count"], 2)
        self.assertEqual(window["total_proc_rows_seen"], 22)
        self.assertEqual(window["max_snapshot_seconds"], 0.03)
        self.assertEqual(window["mean_snapshot_cpu_seconds"], 0.015)


if __name__ == "__main__":
    unittest.main()
