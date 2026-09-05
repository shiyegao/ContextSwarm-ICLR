#!/usr/bin/env python3
"""Bounded host-side resource sampling for external Judge/Lean services.

This utility is intentionally outside the ContextSwarm runner.  It observes
already-running processes and cgroup-v2 counters; it never starts, stops, or
contacts a service.  Operators provide one or more root PIDs for each logical
layer (for example ``backend``, ``router`` and ``worker``).  A layer contains
the root's process group and descendants, while the report keeps layers
separate so warm Judge/router memory is not silently attributed to an agent.

The output is a bounded JSONL stream (``samples.jsonl``) plus a compact
``summary.json``.  Both files are task-local and contain no command lines,
environment values, endpoints, or process paths.  CPU counters are cumulative
process/cgroup counters; deltas are interval estimates for a *process group*,
not per-job attribution.

The implementation uses only the Python standard library so it can be copied
to a host which does not have the ContextSwarm package installed.  ``/proc``
and cgroup roots are injectable for offline tests.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import datetime as _datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence


SCHEMA_VERSION = "contextswarm_external_resource_profile_v1"
DEFAULT_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_SAMPLES = 720
DEFAULT_MAX_OUTPUT_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_PID_SAMPLE = 64
DEFAULT_MAX_ROOTS_PER_LAYER = 256
DEFAULT_MAX_OVERLAP_ENTRIES = 256
DEFAULT_MAX_MEMBERS_PER_LAYER = 100_000
_LAYER_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_KNOWN_LAYERS = ("backend", "router", "worker", "agent", "wrapper")
_NUMERIC_METRICS = (
    "pid_count",
    "root_pid_count",
    "live_root_pid_count",
    "thread_count",
    "rss_bytes",
    "pss_bytes",
    "cpu_user_seconds",
    "cpu_system_seconds",
    "cpu_time_seconds",
    "context_switches",
    "cgroup_count",
    "cgroup_memory_current_bytes",
    "cgroup_memory_peak_bytes",
    "cgroup_cpu_usage_usec",
    "cgroup_cpu_user_usec",
    "cgroup_cpu_system_usec",
    "cgroup_nr_periods",
    "cgroup_nr_throttled",
    "cgroup_cpu_throttled_usec",
    "cgroup_pids_current",
    "cgroup_pids_max",
    "cgroup_memory_events_count",
    "cgroup_oom_kill_count",
)
_PEAK_METRICS = (
    "pid_count",
    "thread_count",
    "rss_bytes",
    "pss_bytes",
    "cgroup_memory_current_bytes",
    "cgroup_memory_peak_bytes",
    "cgroup_pids_current",
    "cgroup_pids_max",
)
_OBSERVER_COUNT_METRICS = (
    "proc_rows_seen",
    "proc_stat_reads",
    "candidate_pid_count",
    "detailed_process_reads",
    "detailed_process_count",
    "status_reads",
    "smaps_rollup_reads",
    "cgroup_reads",
    "cgroup_file_reads",
)
_OBSERVER_DURATION_METRICS = ("snapshot_seconds", "snapshot_cpu_seconds")


def _utc_now() -> str:
    return _datetime.datetime.now(_datetime.timezone.utc).isoformat()


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number > 0 else None


def _safe_layer(value: Any) -> str:
    layer = str(value or "").strip().casefold()
    if not _LAYER_RE.fullmatch(layer):
        raise ValueError(f"invalid layer name: {layer!r}")
    return layer


def _hash_cgroup_path(value: str) -> str:
    """Return a stable, non-reversible cgroup identifier for one run."""

    return "cg:" + hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()[:16]


def _safe_comm(value: Any) -> str:
    """Keep process names useful without serializing arbitrary ``comm`` text."""

    text = str(value or "")[:64]
    safe = "".join(char if (char.isalnum() or char in "._-+@:") else "_" for char in text)
    return safe or "?"


def _read_text(path: Path, *, encoding: str = "ascii") -> str | None:
    try:
        return path.read_text(encoding=encoding)
    except (OSError, UnicodeError):
        return None


def _parse_proc_stat(text: str, *, pid: int, hz: int, page_size: int) -> dict[str, Any] | None:
    """Parse Linux ``/proc/<pid>/stat`` without being confused by ``)`` in comm."""

    line = text.strip()
    opening = line.find("(")
    closing = line.rfind(")")
    if opening <= 0 or closing <= opening or closing + 2 > len(line):
        return None
    try:
        parsed_pid = int(line[:opening])
        if parsed_pid != pid:
            return None
        comm = _safe_comm(line[opening + 1 : closing].replace("\x00", ""))
        fields = line[closing + 2 :].split()
        # The tail starts at field 3 (state), hence field N is index N - 3.
        if len(fields) < 22:
            return None
        ppid = int(fields[1])
        pgrp = int(fields[2])
        state = fields[0][:1] or "?"
        utime_ticks = max(0, int(fields[11]))
        stime_ticks = max(0, int(fields[12]))
        thread_count = max(0, int(fields[17]))
        starttime_ticks = max(0, int(fields[19]))
        rss_pages = max(0, int(fields[21]))
    except (IndexError, TypeError, ValueError, OverflowError):
        return None
    return {
        "pid": pid,
        "comm": comm,
        "state": state,
        "ppid": ppid,
        "pgrp": pgrp,
        "starttime_ticks": starttime_ticks,
        "thread_count": thread_count,
        "rss_bytes": rss_pages * page_size,
        "cpu_user_seconds": round(utime_ticks / max(1, hz), 6),
        "cpu_system_seconds": round(stime_ticks / max(1, hz), 6),
    }


def _parse_status(text: str) -> dict[str, int]:
    """Read bounded memory/context-switch fields from ``/proc/<pid>/status``."""

    result: dict[str, int] = {}
    names = {
        "VmRSS": "rss_bytes",
        "RssAnon": "rss_anon_bytes",
        "RssFile": "rss_file_bytes",
        "RssShmem": "rss_shmem_bytes",
        "Threads": "thread_count",
        "voluntary_ctxt_switches": "voluntary_context_switches",
        "nonvoluntary_ctxt_switches": "nonvoluntary_context_switches",
    }
    for line in text.splitlines():
        key, separator, tail = line.partition(":")
        if not separator or key not in names:
            continue
        fields = tail.strip().split()
        if not fields:
            continue
        try:
            number = int(fields[0])
        except (TypeError, ValueError):
            continue
        # Memory values in status are kB; counters are already scalar counts.
        result[names[key]] = max(0, number * 1024 if key.startswith(("Vm", "Rss")) else number)
    if "voluntary_context_switches" in result or "nonvoluntary_context_switches" in result:
        result["context_switches"] = result.get("voluntary_context_switches", 0) + result.get(
            "nonvoluntary_context_switches", 0
        )
    return result


def _parse_pss(text: str) -> int | None:
    for line in text.splitlines():
        key, separator, tail = line.partition(":")
        if key != "Pss" or not separator:
            continue
        fields = tail.strip().split()
        if not fields:
            return None
        try:
            return max(0, int(fields[0]) * 1024)
        except (TypeError, ValueError, OverflowError):
            return None
    return None


def _parse_cgroup_cpu_stat(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    names = {
        "usage_usec": "cgroup_cpu_usage_usec",
        "user_usec": "cgroup_cpu_user_usec",
        "system_usec": "cgroup_cpu_system_usec",
        "nr_periods": "cgroup_nr_periods",
        "nr_throttled": "cgroup_nr_throttled",
        "throttled_usec": "cgroup_cpu_throttled_usec",
    }
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 2 or fields[0] not in names:
            continue
        try:
            result[names[fields[0]]] = max(0, int(fields[1]))
        except (TypeError, ValueError, OverflowError):
            continue
    return result


def _parse_scalar_file(text: str | None) -> int | None:
    if text is None:
        return None
    value = text.strip()
    if value == "max":
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_cgroup_path(text: str | None) -> str | None:
    if not text:
        return None
    for line in text.splitlines():
        # cgroup v2 has one unified line: 0::<relative-path>.
        if not line.startswith("0::"):
            continue
        relative = line[3:].strip().lstrip("/")
        # ``0::/`` denotes the hierarchy root.  Keep a dedicated ``.``
        # marker so it can be read without ever allowing ``..`` traversal.
        if not relative:
            return "."
        if ".." in Path(relative).parts:
            return None
        return relative
    return None


def _proc_dirs(proc_root: Path) -> Iterable[int]:
    try:
        entries = proc_root.iterdir()
    except OSError:
        return ()
    pids: list[int] = []
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            pid = int(entry.name)
        except ValueError:
            continue
        if pid > 0 and entry.is_dir():
            pids.append(pid)
    return pids


class ProcessSnapshotter:
    """Read process and cgroup metrics for explicit logical-layer roots."""

    def __init__(
        self,
        selectors: Mapping[str, Sequence[int]],
        *,
        proc_root: Path = Path("/proc"),
        cgroup_root: Path = Path("/sys/fs/cgroup"),
        hz: int | None = None,
        page_size: int | None = None,
        max_pid_sample: int = DEFAULT_MAX_PID_SAMPLE,
        exclude_pids: Sequence[int] = (),
        max_roots_per_layer: int = DEFAULT_MAX_ROOTS_PER_LAYER,
        max_members_per_layer: int = DEFAULT_MAX_MEMBERS_PER_LAYER,
        observer_clock: Callable[[], float] | None = None,
        observer_cpu_clock: Callable[[], float] | None = None,
    ) -> None:
        if not selectors:
            raise ValueError("at least one layer selector is required")
        self.proc_root = Path(proc_root).expanduser().resolve()
        self.cgroup_root = Path(cgroup_root).expanduser().resolve()
        self.hz = int(hz or os.sysconf("SC_CLK_TCK"))
        self.page_size = int(page_size or os.sysconf("SC_PAGE_SIZE"))
        if self.hz <= 0 or self.page_size <= 0:
            raise ValueError("invalid system tick/page size")
        if max_pid_sample < 1 or max_pid_sample > 10_000:
            raise ValueError("max_pid_sample must be between 1 and 10000")
        if max_roots_per_layer < 1 or max_roots_per_layer > 10_000:
            raise ValueError("max_roots_per_layer must be between 1 and 10000")
        if max_members_per_layer < 1 or max_members_per_layer > 1_000_000:
            raise ValueError("max_members_per_layer must be between 1 and 1000000")
        self.max_pid_sample = max_pid_sample
        self.max_roots_per_layer = max_roots_per_layer
        self.max_members_per_layer = max_members_per_layer
        # ``perf_counter`` is deliberately injectable for deterministic
        # offline tests.  It measures the observer's own snapshot work and is
        # not mixed with the caller-supplied run timeline.
        self.observer_clock = observer_clock or time.perf_counter
        self.observer_cpu_clock = observer_cpu_clock or time.process_time
        self.exclude_pids = frozenset(
            pid for raw_pid in exclude_pids if (pid := _positive_int(raw_pid)) is not None
        )
        normalized: dict[str, tuple[int, ...]] = {}
        for raw_layer, raw_pids in selectors.items():
            layer = _safe_layer(raw_layer)
            pids: list[int] = []
            for raw_pid in raw_pids:
                pid = _positive_int(raw_pid)
                if pid is None:
                    raise ValueError(f"invalid PID for layer {layer!r}")
                if pid not in pids:
                    pids.append(pid)
            if not pids:
                raise ValueError(f"layer {layer!r} has no PIDs")
            if len(pids) > self.max_roots_per_layer:
                raise ValueError(
                    f"layer {layer!r} has too many root PIDs "
                    f"({len(pids)} > {self.max_roots_per_layer})"
                )
            normalized[layer] = tuple(pids)
        self.selectors = normalized
        # PID start time and process-group identity are captured on first sight
        # and then used to reject a recycled PID on later samples.
        self._root_identity: dict[tuple[str, int], dict[str, int]] = {}
        self._pid_identity: dict[int, int] = {}

    def _read_process(
        self,
        pid: int,
        *,
        detailed: bool = True,
        observer: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        if observer is not None:
            observer["proc_stat_reads"] += 1
            if detailed:
                observer["detailed_process_reads"] += 1
        base = self.proc_root / str(pid)
        stat_text = _read_text(base / "stat", encoding="utf-8")
        if stat_text is None:
            return None
        parsed = _parse_proc_stat(stat_text, pid=pid, hz=self.hz, page_size=self.page_size)
        if parsed is None:
            return None
        if observer is not None and detailed:
            observer["detailed_process_count"] += 1
        if not detailed:
            # The first pass only needs process-group/parent/start-time
            # identity.  Avoid opening status/smaps for every host process;
            # those files can be materially more expensive than stat.
            parsed["pss_bytes"] = None
            parsed["context_switches"] = 0
            parsed["cpu_time_seconds"] = round(
                float(parsed.get("cpu_user_seconds", 0.0))
                + float(parsed.get("cpu_system_seconds", 0.0)),
                6,
            )
            return parsed
        if observer is not None:
            observer["status_reads"] += 1
        status = _read_text(base / "status")
        if status is not None:
            parsed.update(_parse_status(status))
        if observer is not None:
            observer["smaps_rollup_reads"] += 1
        pss_text = _read_text(base / "smaps_rollup")
        parsed["pss_bytes"] = _parse_pss(pss_text) if pss_text is not None else None
        parsed["context_switches"] = parsed.get("context_switches", 0)
        # ``status`` can expose a more current thread count than stat.
        parsed["thread_count"] = max(0, int(parsed.get("thread_count", 0)))
        parsed["cpu_time_seconds"] = round(
            float(parsed.get("cpu_user_seconds", 0.0)) + float(parsed.get("cpu_system_seconds", 0.0)),
            6,
        )
        return parsed

    def _read_process_table(
        self,
        *,
        detailed: bool = True,
        observer: dict[str, int] | None = None,
    ) -> dict[int, dict[str, Any]]:
        table: dict[int, dict[str, Any]] = {}
        pids = list(_proc_dirs(self.proc_root))
        if observer is not None:
            observer["proc_rows_seen"] += len(pids)
        for pid in pids:
            process = self._read_process(pid, detailed=detailed, observer=observer)
            if process is not None:
                table[pid] = process
        return table

    def _cgroup_for(
        self,
        pid: int,
        cache: dict[str, dict[str, Any] | None],
        *,
        observer: dict[str, int] | None = None,
    ) -> dict[str, Any] | None:
        if observer is not None:
            observer["cgroup_reads"] += 1
        base = self.proc_root / str(pid)
        relative = _parse_cgroup_path(_read_text(base / "cgroup"))
        if relative is None:
            return None
        if relative in cache:
            return cache[relative]
        candidate = (
            self.cgroup_root
            if relative == "."
            else (self.cgroup_root / relative).resolve(strict=False)
        )
        try:
            candidate.relative_to(self.cgroup_root.resolve())
        except (OSError, RuntimeError, ValueError):
            cache[relative] = None
            return None
        if not candidate.is_dir():
            cache[relative] = None
            return None
        def read_cgroup_file(name: str) -> str | None:
            if observer is not None:
                observer["cgroup_file_reads"] += 1
            return _read_text(candidate / name)

        metrics: dict[str, Any] = {
            "cgroup_id": _hash_cgroup_path(relative),
            "memory_current_bytes": _parse_scalar_file(read_cgroup_file("memory.current")),
            "memory_peak_bytes": _parse_scalar_file(read_cgroup_file("memory.peak")),
            "pids_current": _parse_scalar_file(read_cgroup_file("pids.current")),
            "pids_max": _parse_scalar_file(read_cgroup_file("pids.max")),
            "memory_events_count": None,
            "oom_kill_count": None,
        }
        memory_events = read_cgroup_file("memory.events")
        if memory_events is not None:
            event_values: dict[str, int] = {}
            for line in memory_events.splitlines():
                fields = line.split()
                if len(fields) != 2:
                    continue
                try:
                    event_values[fields[0]] = max(0, int(fields[1]))
                except (TypeError, ValueError, OverflowError):
                    continue
            metrics["memory_events_count"] = sum(event_values.values()) if event_values else None
            metrics["oom_kill_count"] = event_values.get("oom_kill")
        cpu = _parse_cgroup_cpu_stat(read_cgroup_file("cpu.stat") or "")
        metrics.update(cpu)
        cache[relative] = metrics
        return metrics

    def _members_for(
        self,
        layer: str,
        roots: Sequence[int],
        table: Mapping[int, Mapping[str, Any]],
        warnings: list[str],
    ) -> tuple[list[Mapping[str, Any]], list[int], list[int], bool]:
        """Return selected processes, configured roots, and live roots.

        Membership is the union of each root's process group and descendants.
        The root's initial ``starttime``/``pgrp`` guards prevent PID reuse from
        silently moving an unrelated process into a layer after a restart.
        """

        pids_by_parent: dict[int, list[int]] = defaultdict(list)
        for process in table.values():
            pids_by_parent[int(process.get("ppid", 0))].append(int(process["pid"]))

        members: set[int] = set()
        live_roots: list[int] = []
        expected_groups: set[int] = set()
        for root in roots:
            key = (layer, root)
            current = table.get(root)
            identity = self._root_identity.get(key)
            if identity is None and current is not None:
                identity = {
                    "starttime_ticks": int(current["starttime_ticks"]),
                    "pgrp": int(current["pgrp"]),
                }
                self._root_identity[key] = identity
            if identity is None:
                warnings.append(f"{layer}:root_missing:{root}")
                continue
            if current is None:
                warnings.append(f"{layer}:root_missing:{root}")
                continue
            if current is not None and int(current["starttime_ticks"]) == identity["starttime_ticks"]:
                live_roots.append(root)
                expected_groups.add(identity["pgrp"])
            elif current is not None:
                warnings.append(f"{layer}:root_pid_reused:{root}")

        # A live root is always part of its own layer, even if it called
        # ``setsid`` and changed process group after the first observation.
        members.update(live_roots)
        for pid, process in table.items():
            if pid in self.exclude_pids and pid not in live_roots:
                continue
            if int(process.get("pgrp", -1)) in expected_groups:
                members.add(pid)
        # Descendant traversal matters when a helper creates a fresh process
        # group; retain it only while it remains reachable from a live root.
        queue = list(live_roots)
        visited_descendants: set[int] = set()
        while queue:
            parent = queue.pop()
            if parent in visited_descendants:
                continue
            visited_descendants.add(parent)
            for child in pids_by_parent.get(parent, ()):
                if child in self.exclude_pids and child not in live_roots:
                    continue
                if child not in members:
                    members.add(child)
                queue.append(child)

        membership_truncated = len(members) > self.max_members_per_layer
        if membership_truncated:
            warnings.append(f"{layer}:member_limit:{self.max_members_per_layer}")
        selected: list[Mapping[str, Any]] = []
        for pid in sorted(members)[: self.max_members_per_layer]:
            process = table.get(pid)
            if process is None:
                continue
            start = int(process["starttime_ticks"])
            previous = self._pid_identity.get(pid)
            if previous is not None and previous != start:
                # A recycled PID must never be attributed to the old layer.
                # Keep the warning, but fail closed for this sample.
                warnings.append(f"{layer}:member_pid_reused:{pid}")
                continue
            self._pid_identity[pid] = start
            selected.append(process)
        return selected, list(roots), live_roots, membership_truncated

    def _aggregate_layer(
        self,
        layer: str,
        roots: Sequence[int],
        table: Mapping[int, Mapping[str, Any]],
        cgroup_cache: dict[str, dict[str, Any] | None],
        warnings: list[str],
        observer: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        selected, configured_roots, live_roots, membership_truncated = self._members_for(
            layer, roots, table, warnings
        )
        state_counts: dict[str, int] = defaultdict(int)
        rss = 0
        pss_values: list[int] = []
        cpu_user = cpu_system = 0.0
        threads = context_switches = 0
        cgroups: dict[str, Mapping[str, Any]] = {}
        pid_sample: list[int] = []
        pgrp_ids: set[int] = set()
        for process in selected:
            pid = int(process["pid"])
            if len(pid_sample) < self.max_pid_sample:
                pid_sample.append(pid)
            pgrp_ids.add(int(process.get("pgrp", 0)))
            state_counts[str(process.get("state", "?"))] += 1
            rss += max(0, int(process.get("rss_bytes", 0)))
            pss = process.get("pss_bytes")
            if isinstance(pss, int):
                pss_values.append(max(0, pss))
            cpu_user += max(0.0, float(process.get("cpu_user_seconds", 0.0)))
            cpu_system += max(0.0, float(process.get("cpu_system_seconds", 0.0)))
            threads += max(0, int(process.get("thread_count", 0)))
            context_switches += max(0, int(process.get("context_switches", 0)))
            cgroup = self._cgroup_for(pid, cgroup_cache, observer=observer)
            if cgroup is not None:
                cgroups[str(cgroup["cgroup_id"])] = cgroup
        result: dict[str, Any] = {
            "pid_count": len(selected),
            "member_limit": self.max_members_per_layer,
            "membership_truncated": membership_truncated,
            "pid_sample": pid_sample,
            "pid_sample_truncated": len(selected) > len(pid_sample),
            "pgrp_ids": sorted(item for item in pgrp_ids if item > 0)[: self.max_pid_sample],
            "pgrp_ids_truncated": len(pgrp_ids) > self.max_pid_sample,
            "root_pids": list(configured_roots),
            "root_pid_count": len(configured_roots),
            "live_root_pids": list(live_roots),
            "live_root_pid_count": len(live_roots),
            "thread_count": threads,
            "rss_bytes": rss,
            "pss_bytes": sum(pss_values) if len(pss_values) == len(selected) else None,
            "cpu_user_seconds": round(cpu_user, 6),
            "cpu_system_seconds": round(cpu_system, 6),
            "cpu_time_seconds": round(cpu_user + cpu_system, 6),
            "context_switches": context_switches,
            "state_counts": dict(sorted(state_counts.items())),
            "cgroup_count": len(cgroups),
            "cgroup_ids": sorted(cgroups)[:DEFAULT_MAX_OVERLAP_ENTRIES],
            "cgroup_ids_truncated": len(cgroups) > DEFAULT_MAX_OVERLAP_ENTRIES,
            # Kept only until ``snapshot`` computes cross-layer overlap.  It
            # is removed before serialization so output remains bounded.
            "_member_pids": tuple(int(item["pid"]) for item in selected),
            "_member_cgroup_ids": tuple(sorted(cgroups)),
        }
        for metric in (
            "memory_current_bytes",
            "memory_peak_bytes",
            "pids_current",
            "cgroup_cpu_usage_usec",
            "cgroup_cpu_user_usec",
            "cgroup_cpu_system_usec",
            "cgroup_nr_periods",
            "cgroup_nr_throttled",
            "cgroup_cpu_throttled_usec",
            "memory_events_count",
            "oom_kill_count",
        ):
            values = [item[metric] for item in cgroups.values() if isinstance(item.get(metric), int)]
            if metric == "pids_current":
                # pids.current is a cgroup occupancy counter.  Summing unique
                # cgroups is useful per layer but can overlap across layers;
                # the report explicitly records overlap IDs.
                result["cgroup_pids_current"] = sum(values) if values else None
            elif metric == "memory_current_bytes":
                result["cgroup_memory_current_bytes"] = sum(values) if values else None
            elif metric == "memory_peak_bytes":
                result["cgroup_memory_peak_bytes"] = sum(values) if values else None
            elif metric == "memory_events_count":
                result["cgroup_memory_events_count"] = sum(values) if values else None
            elif metric == "oom_kill_count":
                result["cgroup_oom_kill_count"] = sum(values) if values else None
            else:
                result[metric] = sum(values) if values else None
        result.setdefault("cgroup_pids_current", None)
        result.setdefault("cgroup_memory_current_bytes", None)
        result.setdefault("cgroup_memory_peak_bytes", None)
        result.setdefault("cgroup_memory_events_count", None)
        result.setdefault("cgroup_oom_kill_count", None)
        pids_max_values = [
            item["pids_max"]
            for item in cgroups.values()
            if isinstance(item.get("pids_max"), int)
        ]
        result["cgroup_pids_max"] = sum(pids_max_values) if pids_max_values else None
        for metric in (
            "cgroup_cpu_usage_usec",
            "cgroup_cpu_user_usec",
            "cgroup_cpu_system_usec",
            "cgroup_nr_periods",
            "cgroup_nr_throttled",
            "cgroup_cpu_throttled_usec",
        ):
            result.setdefault(metric, None)
        return result

    def snapshot(self, *, sample_index: int = 0, monotonic_seconds: float | None = None) -> dict[str, Any]:
        """Take one scalar-only sample; never reads command lines or env vars."""

        observer_started = self.observer_clock()
        observer_cpu_started = self.observer_cpu_clock()
        observer_counts: dict[str, Any] = {metric: 0 for metric in _OBSERVER_COUNT_METRICS}
        warnings: list[str] = []
        # Read stat for the complete process table first (cheap and enough to
        # resolve pgrp/parent membership), then collect detailed counters only
        # for the selected members.  This bounds observer overhead on hosts
        # with many unrelated processes.
        table = self._read_process_table(detailed=False, observer=observer_counts)
        candidate_pids: set[int] = set()
        children: dict[int, list[int]] = defaultdict(list)
        for pid, process in table.items():
            children[int(process.get("ppid", 0))].append(pid)
        for layer, roots in self.selectors.items():
            for root in roots:
                identity = self._root_identity.get((layer, root))
                current = table.get(root)
                if identity is None and current is not None:
                    identity = {
                        "starttime_ticks": int(current["starttime_ticks"]),
                        "pgrp": int(current["pgrp"]),
                    }
                    self._root_identity[(layer, root)] = identity
                if identity is None or current is None:
                    continue
                if int(current["starttime_ticks"]) != identity["starttime_ticks"]:
                    continue
                expected_group = identity["pgrp"]
                candidate_pids.add(root)
                for pid, process in table.items():
                    if pid in self.exclude_pids and pid != root:
                        continue
                    if int(process.get("pgrp", -1)) == expected_group:
                        candidate_pids.add(pid)
                queue = [root]
                visited: set[int] = set()
                while queue:
                    parent = queue.pop()
                    if parent in visited:
                        continue
                    visited.add(parent)
                    for child in children.get(parent, ()):
                        if child in self.exclude_pids and child != root:
                            continue
                        candidate_pids.add(child)
                        queue.append(child)
        for pid in sorted(candidate_pids):
            observer_counts["candidate_pid_count"] += 1
            detailed = self._read_process(pid, detailed=True, observer=observer_counts)
            if detailed is not None:
                table[pid] = detailed
            else:
                # The stat-only row is stale if detailed files disappeared
                # between passes; do not charge a vanished process to a layer.
                table.pop(pid, None)
        cgroup_cache: dict[str, dict[str, Any] | None] = {}
        layers: dict[str, Any] = {}
        pid_layers: dict[int, list[str]] = defaultdict(list)
        cgroup_layers: dict[str, list[str]] = defaultdict(list)
        for layer, roots in self.selectors.items():
            layers[layer] = self._aggregate_layer(
                layer, roots, table, cgroup_cache, warnings, observer=observer_counts
            )
            for pid in layers[layer].get("_member_pids", ()):
                pid_layers[pid].append(layer)
            for cgroup_id in layers[layer].get("_member_cgroup_ids", ()):
                cgroup_layers[cgroup_id].append(layer)
            if layers[layer]["pss_bytes"] is None and layers[layer]["pid_count"]:
                warnings.append(f"{layer}:pss_unavailable")
            if layers[layer]["cgroup_count"] == 0 and layers[layer]["pid_count"]:
                warnings.append(f"{layer}:cgroup_unavailable")
        overlap_items = [
            (str(pid), sorted(names))
            for pid, names in sorted(pid_layers.items())
            if len(names) > 1
        ]
        overlaps = dict(overlap_items[:DEFAULT_MAX_OVERLAP_ENTRIES])
        overlap_truncated = len(overlap_items) > len(overlaps)
        cgroup_overlaps = {
            cgroup_id: sorted(names)
            for cgroup_id, names in sorted(cgroup_layers.items())
            if len(names) > 1
        }
        if overlaps:
            warnings.append("layer_membership_overlap")
        if overlap_truncated:
            warnings.append("layer_membership_overlap_truncated")
        if cgroup_overlaps:
            warnings.append("layer_cgroup_overlap")
        if any(layer.get("cgroup_ids_truncated") for layer in layers.values()):
            warnings.append("cgroup_ids_truncated")
        if len(cgroup_overlaps) > DEFAULT_MAX_OVERLAP_ENTRIES:
            cgroup_overlaps = dict(list(cgroup_overlaps.items())[:DEFAULT_MAX_OVERLAP_ENTRIES])
            warnings.append("layer_cgroup_overlap_truncated")
        for layer in layers.values():
            layer.pop("_member_pids", None)
            layer.pop("_member_cgroup_ids", None)
        observer_elapsed = max(0.0, self.observer_clock() - observer_started)
        observer_cpu_elapsed = max(0.0, self.observer_cpu_clock() - observer_cpu_started)
        # Keep observer accounting separate from service-layer metrics.  These
        # fields are bounded counters for diagnosing sampler overhead and must
        # never be interpreted as Judge/agent CPU or memory.
        observer_counts.update(
            {
                "snapshot_seconds": round(observer_elapsed, 6),
                "snapshot_cpu_seconds": round(observer_cpu_elapsed, 6),
            }
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "sample",
            "sample_index": max(0, int(sample_index)),
            "at": _utc_now(),
            "monotonic_seconds": _finite_float(monotonic_seconds),
            "host_cpu_count": max(1, int(os.cpu_count() or 1)),
            "layers": layers,
            "layer_membership_overlap": overlaps,
            "layer_cgroup_overlap": cgroup_overlaps,
            "observer": observer_counts,
            "warnings": sorted(set(warnings)),
        }


def _numeric_delta(current: Any, baseline: Any) -> int | float | None:
    if current is None or baseline is None:
        return None
    try:
        value = current - baseline
    except (TypeError, ValueError):
        return None
    if isinstance(value, float):
        return round(value, 6) if math.isfinite(value) else None
    return value if isinstance(value, int) else None


def _layer_delta(baseline: Mapping[str, Any], final: Mapping[str, Any], elapsed: float) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for metric in _NUMERIC_METRICS:
        result[metric] = _numeric_delta(final.get(metric), baseline.get(metric))
    elapsed = max(0.0, float(elapsed))
    cpu_delta = result.get("cpu_time_seconds")
    if isinstance(cpu_delta, (int, float)) and elapsed > 0:
        result["cpu_percent_one_core"] = round(100.0 * float(cpu_delta) / elapsed, 6)
        host_cpus = max(1, int(final.get("_host_cpu_count", 1)))
        result["cpu_percent_host_capacity"] = round(
            100.0 * float(cpu_delta) / (elapsed * host_cpus),
            6,
        )
        result["cpu_delta_quality"] = (
            "decreased_due_to_membership_change_or_counter_reset"
            if float(cpu_delta) < 0
            else "process_group_interval_estimate"
        )
    else:
        result["cpu_percent_one_core"] = None
        result["cpu_percent_host_capacity"] = None
        result["cpu_delta_quality"] = "unavailable"
    state_delta: dict[str, int] = {}
    before = baseline.get("state_counts") or {}
    after = final.get("state_counts") or {}
    for state in sorted(set(before) | set(after)):
        value = _numeric_delta(after.get(state, 0), before.get(state, 0))
        if isinstance(value, int):
            state_delta[state] = value
    result["state_counts"] = state_delta
    return result


def _peaks(samples: Sequence[Mapping[str, Any]], layer: str) -> dict[str, Any]:
    rows = [sample.get("layers", {}).get(layer, {}) for sample in samples]
    result: dict[str, Any] = {}
    for metric in _PEAK_METRICS:
        values = [row.get(metric) for row in rows if isinstance(row.get(metric), (int, float))]
        result[metric] = max(values) if values else None
    return result


def _observer_window_summary(
    total: Mapping[str, Any],
    peaks: Mapping[str, Any],
    *,
    sample_count: int,
) -> dict[str, Any]:
    """Return bounded aggregate overhead accounting for the observer itself."""

    result: dict[str, Any] = {"sample_count": max(0, int(sample_count))}
    for metric in (*_OBSERVER_COUNT_METRICS, *_OBSERVER_DURATION_METRICS):
        value = total.get(metric)
        result[f"total_{metric}"] = (
            round(float(value), 6)
            if metric in _OBSERVER_DURATION_METRICS and isinstance(value, (int, float))
            else int(value)
            if isinstance(value, int)
            else None
        )
        peak = peaks.get(metric)
        result[f"max_{metric}"] = (
            round(float(peak), 6)
            if metric in _OBSERVER_DURATION_METRICS and isinstance(peak, (int, float))
            else int(peak)
            if isinstance(peak, int)
            else None
        )
    count = result["sample_count"]
    for metric in (*_OBSERVER_COUNT_METRICS, *_OBSERVER_DURATION_METRICS):
        total_value = result[f"total_{metric}"]
        result[f"mean_{metric}"] = (
            round(float(total_value) / count, 6)
            if count and isinstance(total_value, (int, float))
            else None
        )
    return result


def _collect_observer_summary(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate fixed-width observer counters from post-baseline samples."""

    totals: dict[str, Any] = {metric: 0 for metric in (*_OBSERVER_COUNT_METRICS, *_OBSERVER_DURATION_METRICS)}
    peaks: dict[str, Any] = {}
    observed = 0
    for sample in samples:
        observer = sample.get("observer")
        if not isinstance(observer, Mapping):
            continue
        observed += 1
        for metric in (*_OBSERVER_COUNT_METRICS, *_OBSERVER_DURATION_METRICS):
            value = observer.get(metric)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                continue
            totals[metric] = totals.get(metric, 0) + value
            previous = peaks.get(metric)
            if previous is None or value > previous:
                peaks[metric] = value
    return _observer_window_summary(totals, peaks, sample_count=observed)


def _update_observer_aggregate(
    sample: Mapping[str, Any],
    totals: dict[str, Any],
    peaks: dict[str, Any],
) -> bool:
    """Fold one sample into fixed-width totals/peaks; return whether observed."""

    observer = sample.get("observer")
    if not isinstance(observer, Mapping):
        return False
    for metric in (*_OBSERVER_COUNT_METRICS, *_OBSERVER_DURATION_METRICS):
        value = observer.get(metric)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        totals[metric] = totals.get(metric, 0) + value
        previous = peaks.get(metric)
        if previous is None or value > previous:
            peaks[metric] = value
    return True


def build_summary(
    baseline: Mapping[str, Any],
    samples: Sequence[Mapping[str, Any]],
    *,
    started_at: str,
    finished_at: str,
    status: str = "complete",
    configured_duration_seconds: float = 0.0,
    configured_interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    warnings: Iterable[str] = (),
    sample_count: int | None = None,
    first_sample: Mapping[str, Any] | None = None,
    last_sample: Mapping[str, Any] | None = None,
    provided_peaks: Mapping[str, Mapping[str, Any]] | None = None,
    observer_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the baseline/run-window/delta ledger consumed by analysis."""

    ordered = list(samples)
    first = first_sample or (ordered[0] if ordered else baseline)
    final = last_sample or (ordered[-1] if ordered else baseline)
    baseline_mono = _finite_float(baseline.get("monotonic_seconds"))
    final_mono = _finite_float(final.get("monotonic_seconds"))
    elapsed = max(0.0, (final_mono - baseline_mono) if baseline_mono is not None and final_mono is not None else 0.0)
    window_first_mono = _finite_float(first.get("monotonic_seconds"))
    window_elapsed = max(
        0.0,
        (final_mono - window_first_mono)
        if window_first_mono is not None and final_mono is not None
        else 0.0,
    )
    all_warnings = set(str(item) for item in warnings if item)
    for sample in [baseline, *ordered]:
        all_warnings.update(str(item) for item in sample.get("warnings", ()) if item)
    host_cpu_count = max(1, int(final.get("host_cpu_count") or baseline.get("host_cpu_count") or 1))
    deltas: dict[str, Any] = {}
    peaks: dict[str, Any] = {}
    for layer in baseline.get("layers", {}):
        base_layer = baseline.get("layers", {}).get(layer, {})
        final_layer = final.get("layers", {}).get(layer, {})
        final_for_delta = dict(final_layer)
        final_for_delta["_host_cpu_count"] = host_cpu_count
        deltas[layer] = _layer_delta(base_layer, final_for_delta, elapsed)
        if provided_peaks is None:
            peak_values_for_layer = _peaks(ordered or [baseline], layer)
        else:
            peak_values_for_layer = dict(provided_peaks.get(layer, {}))
        peaks[layer] = peak_values_for_layer
    if observer_summary is None:
        observer_summary = _collect_observer_summary(ordered)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "summary",
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "configured_duration_seconds": configured_duration_seconds,
        "configured_interval_seconds": configured_interval_seconds,
        "host_cpu_count": host_cpu_count,
        "baseline": baseline,
        "run_window": {
            "sample_count": max(0, int(sample_count if sample_count is not None else len(ordered))),
            "first": first,
            "last": final,
            "elapsed_seconds": round(window_elapsed, 6),
            "peaks": peaks,
        },
        "delta_from_baseline": {
            "elapsed_seconds": round(elapsed, 6),
            "layers": deltas,
        },
        "observer": {
            "baseline": dict(baseline.get("observer") or {}),
            "run_window": dict(observer_summary or {"sample_count": 0}),
        },
        "interpretation": {
            "cpu": (
                "Cumulative process/cgroup CPU counters are differenced over the "
                "observed window; this is process-group CPU, not per-job attribution."
            ),
            "memory": (
                "RSS is summed across selected processes and may double-count shared "
                "pages; PSS is reported only when available. Cgroup memory counters "
                "can overlap when layers share a cgroup."
            ),
            "baseline": (
                "Baseline captures warm service state before the run window. Use "
                "delta_from_baseline for growth; use run_window.peaks for absolute pressure."
            ),
            "layers": (
                "Layer membership is explicit from operator-provided roots. "
                "Backend/router/worker service memory is not assigned to agent or "
                "wrapper unless those roots are explicitly selected as their own layers."
            ),
            "observer": (
                "Observer fields measure sampler work (proc enumeration, detailed reads, "
                "and wall/CPU time), not service CPU or memory. They are bounded per "
                "sample and must be compared on the same host, interval, and selector "
                "contract; a local 240-PID observation is not portable capacity evidence."
            ),
        },
        "warnings": sorted(all_warnings),
    }


class _JsonlWriter:
    def __init__(self, output_dir: Path, *, max_bytes: int) -> None:
        self.output_dir = output_dir
        self.max_bytes = max_bytes
        self.path = output_dir / "samples.jsonl"
        self.handle: Any | None = None
        self.bytes_written = 0

    def open(self) -> None:
        # Never append to a previous task.  Mixing windows would make the
        # baseline and deltas non-auditable; callers should choose a fresh
        # output directory for each run.
        self.handle = _secure_open_create(self.path, mode=0o600)
        self.bytes_written = 0

    def write(self, row: Mapping[str, Any]) -> bool:
        if self.handle is None:
            raise RuntimeError("writer is not open")
        encoded = (json.dumps(row, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
        if self.bytes_written + len(encoded) > self.max_bytes:
            return False
        self.handle.write(encoded.decode("utf-8"))
        self.handle.flush()
        self.bytes_written += len(encoded)
        return True

    def close(self) -> None:
        if self.handle is not None:
            self.handle.flush()
            self.handle.close()
            self.handle = None


def _ensure_output_dir(path: Path) -> Path:
    path = Path(path).expanduser()
    if path.exists():
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"output path is not a directory: {path}")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            raise PermissionError(f"output directory is not owned by current user: {path}")
    else:
        path.mkdir(parents=True, mode=0o700)
    try:
        os.chmod(path, 0o700)
    except OSError:
        pass
    return path.resolve()


def _secure_open_create(path: Path, *, mode: int) -> Any:
    if path.exists() or path.is_symlink():
        raise ValueError(f"refusing existing output: {path}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, mode)
    os.fchmod(fd, mode)
    return os.fdopen(fd, "a", encoding="utf-8", buffering=1)


def _write_summary(output_dir: Path, summary: Mapping[str, Any]) -> Path:
    destination = output_dir / "summary.json"
    if destination.exists() or destination.is_symlink():
        raise ValueError(f"refusing existing output: {destination}")
    temp = output_dir / ".summary.json.tmp"
    if temp.exists() or temp.is_symlink():
        raise ValueError(f"summary temporary path already exists: {temp}")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0), 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=True, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, destination)
        os.chmod(destination, 0o600)
    finally:
        try:
            if temp.exists():
                temp.unlink()
        except OSError:
            pass
    return destination


def run_sampler(
    snapshotter: ProcessSnapshotter,
    *,
    output_dir: Path,
    duration_seconds: float = 0.0,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    max_samples: int = DEFAULT_MAX_SAMPLES,
    max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES,
    monotonic: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], str] = _utc_now,
    wait: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Collect a bounded baseline and run window, returning the summary."""

    duration = _finite_float(duration_seconds)
    interval = _finite_float(interval_seconds)
    if duration is None or duration < 0 or duration > 7 * 24 * 3600:
        raise ValueError("duration_seconds must be between 0 and 604800")
    if interval is None or interval < 0.1 or interval > 60:
        raise ValueError("interval_seconds must be between 0.1 and 60")
    if not isinstance(max_samples, int) or max_samples < 1 or max_samples > 100_000:
        raise ValueError("max_samples must be between 1 and 100000")
    if not isinstance(max_output_bytes, int) or max_output_bytes < 1024:
        raise ValueError("max_output_bytes must be at least 1024")
    target = _ensure_output_dir(output_dir)
    writer = _JsonlWriter(target, max_bytes=max_output_bytes)
    writer.open()
    started_at = wall_clock()
    baseline_mono = monotonic()
    baseline = snapshotter.snapshot(sample_index=0, monotonic_seconds=baseline_mono)
    baseline["at"] = started_at
    sample_count = 0
    first_sample: dict[str, Any] | None = None
    last_sample: dict[str, Any] | None = None
    # A warm baseline is reported separately.  Keep run-window peaks empty
    # until the first post-baseline sample so a zero-duration invocation does
    # not accidentally present baseline pressure as run-window pressure.
    peak_values: dict[str, dict[str, Any]] = {
        layer: {} for layer in baseline.get("layers", {})
    }
    observer_totals: dict[str, Any] = {
        metric: 0 for metric in (*_OBSERVER_COUNT_METRICS, *_OBSERVER_DURATION_METRICS)
    }
    observer_peaks: dict[str, Any] = {}
    observer_sample_count = 0
    warnings: set[str] = set()
    status = "complete"
    try:
        if not writer.write(
            {
                "schema_version": SCHEMA_VERSION,
                "kind": "baseline",
                "sample": baseline,
            }
        ):
            warnings.add("output_limit_before_baseline")
            status = "output_limit"
        else:
            next_deadline = baseline_mono
            sample_index = 0
            window_deadline = baseline_mono + duration
            while sample_index < max_samples:
                now = monotonic()
                if now >= window_deadline:
                    break
                next_deadline = min(next_deadline + interval, window_deadline)
                delay = max(0.0, next_deadline - now)
                if delay <= 0.0:
                    break
                if wait is not None:
                    wait(delay)
                else:
                    time.sleep(delay)
                after_wait = monotonic()
                if after_wait <= now:
                    warnings.add("clock_did_not_advance")
                    status = "clock_stalled"
                    break
                sample_index += 1
                sample = snapshotter.snapshot(sample_index=sample_index, monotonic_seconds=after_wait)
                if not writer.write(sample):
                    warnings.add("output_limit")
                    status = "output_limit"
                    break
                sample_count += 1
                if _update_observer_aggregate(sample, observer_totals, observer_peaks):
                    observer_sample_count += 1
                if first_sample is None:
                    first_sample = sample
                last_sample = sample
                for layer, row in sample.get("layers", {}).items():
                    destination = peak_values.setdefault(layer, {})
                    for metric in _PEAK_METRICS:
                        value = row.get(metric)
                        if not isinstance(value, (int, float)):
                            continue
                        if value > destination.get(metric, value):
                            destination[metric] = value
                warnings.update(str(item) for item in sample.get("warnings", ()) if item)
            if sample_index >= max_samples and monotonic() < window_deadline:
                warnings.add("sample_limit")
                status = "sample_limit"
            if sample_count == 0 and duration > 0:
                # A zero-duration run is intentionally one baseline sample;
                # a positive run with no window sample signals interrupted
                # scheduling or a broken wait implementation.
                warnings.add("run_window_empty")
    except KeyboardInterrupt:
        status = "interrupted"
        warnings.add("interrupted")
    finally:
        writer.close()
    finished_at = wall_clock()
    summary = build_summary(
        baseline,
        (),
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        configured_duration_seconds=duration,
        configured_interval_seconds=interval,
        warnings=warnings,
        sample_count=sample_count,
        first_sample=first_sample,
        last_sample=last_sample,
        provided_peaks=peak_values,
        observer_summary=_observer_window_summary(
            observer_totals,
            observer_peaks,
            sample_count=observer_sample_count,
        ),
    )
    _write_summary(target, summary)
    return summary


def _parse_layer_arg(value: str) -> tuple[str, list[int]]:
    layer_text, separator, pid_text = str(value).partition("=")
    if not separator:
        raise argparse.ArgumentTypeError("layer selector must be NAME=PID[,PID...]")
    try:
        layer = _safe_layer(layer_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    pids: list[int] = []
    for token in pid_text.split(","):
        pid = _positive_int(token.strip())
        if pid is None:
            raise argparse.ArgumentTypeError(f"invalid PID in layer selector {value!r}")
        if pid not in pids:
            pids.append(pid)
    if not pids:
        raise argparse.ArgumentTypeError("layer selector has no PID")
    return layer, pids


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sample explicit backend/router/worker process groups and cgroup-v2 counters."
    )
    parser.add_argument(
        "--layer",
        action="append",
        type=_parse_layer_arg,
        metavar="NAME=PID[,PID...]",
        help="explicit logical layer root PID(s); repeat for backend/router/worker/agent/wrapper",
    )
    for layer in _KNOWN_LAYERS:
        parser.add_argument(
            f"--{layer}-pid",
            action="append",
            type=int,
            default=[],
            metavar="PID",
            help=argparse.SUPPRESS,
        )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--duration", type=float, default=0.0, metavar="SECONDS")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS, metavar="SECONDS")
    parser.add_argument("--max-samples", type=int, default=DEFAULT_MAX_SAMPLES)
    parser.add_argument("--max-output-bytes", type=int, default=DEFAULT_MAX_OUTPUT_BYTES)
    parser.add_argument("--max-pid-sample", type=int, default=DEFAULT_MAX_PID_SAMPLE)
    # These roots are primarily for offline tests and a container with a
    # mounted procfs; normal host invocation uses the kernel defaults.
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"), help=argparse.SUPPRESS)
    parser.add_argument("--cgroup-root", type=Path, default=Path("/sys/fs/cgroup"), help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    selectors: dict[str, list[int]] = defaultdict(list)
    for layer, pids in args.layer or ():
        selectors[layer].extend(pids)
    for layer in _KNOWN_LAYERS:
        values = getattr(args, f"{layer}_pid")
        if values:
            selectors[layer].extend(values)
    selectors = {layer: list(dict.fromkeys(pids)) for layer, pids in selectors.items()}
    if not selectors:
        parser.error("provide at least one --layer NAME=PID (backend/router/worker recommended)")
    try:
        snapshotter = ProcessSnapshotter(
            selectors,
            proc_root=args.proc_root,
            cgroup_root=args.cgroup_root,
            max_pid_sample=args.max_pid_sample,
            exclude_pids=(os.getpid(),),
        )
        summary = run_sampler(
            snapshotter,
            output_dir=args.output_dir,
            duration_seconds=args.duration,
            interval_seconds=args.interval,
            max_samples=args.max_samples,
            max_output_bytes=args.max_output_bytes,
        )
    except (OSError, PermissionError, ValueError, RuntimeError) as exc:
        print(f"external sampler failed: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": summary.get("status"),
                "sample_count": summary.get("run_window", {}).get("sample_count", 0),
                "warning_count": len(summary.get("warnings", ())),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
