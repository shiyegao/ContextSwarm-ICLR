from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest

from contextswarm_mini.config import ConfigError, SelectionConfig, load_config
from contextswarm_mini.cps import CPSStore, make_policy
from contextswarm_mini.evaluator import MockEvaluator
from contextswarm_mini.models import AgentResult
from contextswarm_mini.runner import (
    RunLogger,
    _initialize_selection_runtime,
    _run_task_workers,
    _selection_capabilities,
)
from contextswarm_mini.selection_runtime import SelectionRuntime
from contextswarm_mini.selection_store import SelectionStore
import contextswarm_mini.runner as runner_module


ROOT = Path(__file__).resolve().parents[1]


def _selection_config() -> SelectionConfig:
    return SelectionConfig(
        enabled=True,
        selector_name="random",
        selector_version="figure3_v1",
        visibility="project_shared",
        trace_slot_limit=3,
        context_token_budget=4096,
        tokenizer="unicode_word_v1",
        seed=17,
        tie_break="trace_id_asc",
        policy_params={"sample_without_replacement": True},
        direct_messages=False,
        candidate_transfer=False,
    )


class _RecordingBroker:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    @contextmanager
    def session(self, **kwargs):
        self.calls.append(dict(kwargs))
        yield {
            "CONTEXTSWARM_JUDGE_URL": "http://127.0.0.1:1/test-token",
            "CONTEXTSWARM_BROKER_DEADLINE_EPOCH_MS": "9999999999999",
        }


class _RecordingPi:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def run(self, **kwargs):
        self.calls.append(dict(kwargs))
        now = "2026-01-01T00:00:00+00:00"
        return AgentResult(
            agent_id=str(kwargs["actor_id"]),
            task_id=str(kwargs["task_id"]),
            episode=int(kwargs["episode"]),
            returncode=0,
            started_at=now,
            finished_at=now,
        )


class RunnerSelectionIntegrationTests(unittest.TestCase):
    def test_selection_runtime_requires_cps_and_exposes_one_project_wide_path(self):
        config = replace(load_config("configs/smoke.toml", ROOT), selection=_selection_config())
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            logger = RunLogger(run_dir)
            with self.assertRaisesRegex(ConfigError, "requires a CPS store"):
                _initialize_selection_runtime(config, run_dir, logger)

            cps_store = CPSStore(run_dir / "cps.sqlite3")
            task_piece = cps_store.create_piece(
                task_id="task-a",
                author="worker-a",
                kind="handoff",
                title="task A trace",
                body="local result",
            )
            other_task_piece = cps_store.create_piece(
                task_id="task-b",
                author="worker-b",
                kind="handoff",
                title="task B trace",
                body="cross-task result",
            )
            runtime = _initialize_selection_runtime(
                config,
                run_dir,
                logger,
                cps_store=cps_store,
                run_id="integration-run",
            )
            self.assertIsInstance(runtime, SelectionRuntime)
            assert runtime is not None
            selection_store = runtime.selection_store
            self.assertIsInstance(selection_store, SelectionStore)
            self.assertIsNot(runtime, selection_store)

            metadata = json.loads((run_dir / "selection_runtime.json").read_text())
            self.assertEqual(metadata["selection_config_id"], config.selection.selection_config_id)
            self.assertEqual(
                metadata["registered_selector_config_id"], runtime.selector_config_id
            )
            self.assertTrue(metadata["trace_search"]["fail_closed"])
            self.assertEqual(metadata["trace_search"]["status"], "available")
            self.assertTrue((run_dir / "selection.sqlite3").exists())

            direct = runtime.search(
                task_id="task-a",
                actor_id="search-worker",
                query="result",
                request_key="direct-search",
            )
            expected_trace_ids = {task_piece["id"], other_task_piece["id"]}
            self.assertEqual(
                {item["trace_id"] for item in direct["items"]}, expected_trace_ids
            )

            digest = runtime.digest(
                task_id="task-a",
                actor_id="digest-worker",
                query="result",
                episode=2,
            )
            self.assertIn("task A trace", digest)
            self.assertIn("task B trace", digest)

            broker_result = runtime.broker_search(
                SimpleNamespace(
                    actor_id="broker-worker",
                    episode=4,
                    candidates={"task-a": object()},
                ),
                "result",
                3,
            )
            self.assertEqual(
                {item["trace_id"] for item in broker_result["items"]},
                expected_trace_ids,
            )

            request_keys = (
                "direct-search",
                "integration-run:digest:task-a:digest-worker:2",
                broker_result["request_key"],
            )
            for request_key in request_keys:
                chain = selection_store.get_search_by_request_key(request_key)
                self.assertIsNotNone(chain)
                assert chain is not None
                self.assertIsNotNone(chain["exposure"])
                if request_key == broker_result["request_key"]:
                    self.assertEqual(chain["search_event"]["query"]["episode"], 4)
                self.assertEqual(
                    {item["trace_id"] for item in chain["items"]},
                    expected_trace_ids,
                )
                for item in chain["items"]:
                    attribution = selection_store.attribution_chain(
                        item["exposure_item_id"]
                    )
                    self.assertIsNotNone(attribution)
                    assert attribution is not None
                    self.assertEqual(
                        attribution["search_event"]["search_event_id"],
                        chain["search_event"]["search_event_id"],
                    )

    def test_task_worker_passes_isolated_selection_capabilities(self):
        base = load_config("configs/smoke.toml", ROOT)
        config = replace(base, selection=_selection_config(), episodes_per_task=1, max_tasks=1)
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            logger = RunLogger(run_dir)
            cps_store = CPSStore(run_dir / "cps.sqlite3")
            tasks = runner_module.load_tasks(config)
            local_piece = cps_store.create_piece(
                task_id=tasks[0].slug,
                author="local-worker",
                kind="note",
                title="local trace",
                body="local body",
            )
            remote_piece = cps_store.create_piece(
                task_id="another-task",
                author="remote-worker",
                kind="note",
                title="project trace",
                body="project body",
            )
            selection_runtime = _initialize_selection_runtime(
                config,
                run_dir,
                logger,
                cps_store=cps_store,
                run_id="worker-run",
            )
            assert selection_runtime is not None
            selection_store = selection_runtime.selection_store
            broker = _RecordingBroker()
            pi = _RecordingPi()
            policy = make_policy(config.communication, cps_store)
            evaluator = MockEvaluator()
            results = _run_task_workers(
                config,
                tasks,
                run_dir,
                logger,
                evaluator,
                pi,
                policy,
                mock_agent=False,
                deadline=runner_module.time.monotonic() + 2.0,
                evaluator_gate=threading.BoundedSemaphore(1),
                judge_broker=broker,
                selection_store=selection_store,
                selection_runtime=selection_runtime,
            )
            self.assertEqual(len(results), 1)
            self.assertEqual(len(broker.calls), 1)
            self.assertEqual(_selection_capabilities(config), (True, False, False))
            self.assertFalse(broker.calls[0]["direct_messages_allowed"])
            self.assertEqual(broker.calls[0]["episode"], 1)
            self.assertTrue(broker.calls[0]["selection_enabled"])
            self.assertIs(broker.calls[0]["selection_store"], selection_store)
            selection_search = broker.calls[0]["selection_search"]
            self.assertTrue(callable(selection_search))
            self.assertFalse(pi.calls[0]["direct_messages"])
            self.assertTrue(pi.calls[0]["selection_enabled"])
            self.assertTrue(pi.calls[0]["communication_enabled"])

            actor = str(pi.calls[0]["actor_id"])
            digest_key = f"worker-run:digest:{tasks[0].slug}:{actor}:1"
            digest_chain = selection_store.get_search_by_request_key(digest_key)
            self.assertIsNotNone(digest_chain)
            assert digest_chain is not None
            self.assertEqual(
                {item["trace_id"] for item in digest_chain["items"]},
                {local_piece["id"], remote_piece["id"]},
            )
            self.assertIsNotNone(digest_chain["exposure"])

            callback_result = selection_search(
                SimpleNamespace(
                    actor_id=actor,
                    candidates={tasks[0].slug: object()},
                ),
                "project",
                3,
            )
            callback_chain = selection_store.get_search_by_request_key(
                callback_result["request_key"]
            )
            self.assertIsNotNone(callback_chain)
            assert callback_chain is not None
            self.assertEqual(
                callback_chain["search_event"]["actor_id"], actor
            )
            self.assertEqual(
                {item["trace_id"] for item in callback_chain["items"]},
                {local_piece["id"], remote_piece["id"]},
            )

    def test_legacy_capabilities_keep_direct_messages_and_transfer(self):
        config = load_config("configs/smoke.toml", ROOT)
        self.assertEqual(_selection_capabilities(config), (False, True, True))

    def test_formal_figure4_keeps_selector_isolation_and_candidate_handoff(self):
        config = load_config("configs/figure4_formal_icpc/uniform_refill.toml", ROOT)
        self.assertEqual(_selection_capabilities(config), (True, False, True))

    def test_candidate_handoff_remains_forbidden_outside_formal_figure4(self):
        base = load_config("configs/smoke.toml", ROOT)
        selection = replace(_selection_config(), candidate_transfer=True)
        config = replace(base, selection=selection)
        with self.assertRaisesRegex(ConfigError, "outside formal Figure 4"):
            _selection_capabilities(config)

    def test_formal_figure4_still_rejects_direct_messages(self):
        base = load_config("configs/figure4_formal_icpc/uniform_refill.toml", ROOT)
        selection = replace(base.selection, direct_messages=True)
        config = replace(base, selection=selection)
        with self.assertRaisesRegex(ConfigError, "disable direct messages"):
            _selection_capabilities(config)


if __name__ == "__main__":
    unittest.main()
