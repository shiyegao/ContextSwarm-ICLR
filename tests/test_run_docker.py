from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
RUN_DOCKER = ROOT / "scripts" / "run_docker.sh"


class RunDockerManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        (ROOT / "runs").mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix=".test-run-docker-",
            dir=ROOT / "runs",
        )
        self.temp = Path(self.temporary.name)
        self.bin_dir = self.temp / "bin"
        self.bin_dir.mkdir()
        self.capture = self.temp / "docker-argv.json"
        self.inspect_capture = self.temp / "docker-inspects.jsonl"

        fake_docker = self.bin_dir / "docker"
        fake_docker.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, pathlib, sys\n"
            "args = sys.argv[1:]\n"
            "if args[:2] == ['image', 'inspect']:\n"
            "    with pathlib.Path(os.environ['FAKE_DOCKER_INSPECT_CAPTURE']).open(\n"
            "        'a', encoding='utf-8'\n"
            "    ) as stream:\n"
            "        stream.write(json.dumps(args) + '\\n')\n"
            "    fmt = args[args.index('--format') + 1]\n"
            "    if fmt == '{{.Id}}':\n"
            "        print('sha256:' + ('1' * 64))\n"
            "    else:\n"
            "        print('2' * 40)\n"
            "else:\n"
            "    pathlib.Path(os.environ['FAKE_DOCKER_CAPTURE']).write_text(\n"
            "        json.dumps(args), encoding='utf-8'\n"
            "    )\n",
            encoding="utf-8",
        )
        fake_docker.chmod(0o755)

        self.fake_nurouter = self.bin_dir / "fake-nurouter"
        self.fake_nurouter.write_text(
            "#!/usr/bin/env sh\nprintf '%s\\n' 'fake-nurouter 1.0'\n",
            encoding="utf-8",
        )
        self.fake_nurouter.chmod(0o755)

        self.parent_manifest = self.temp / "parent.toml"
        self.child_manifest = self.temp / "child.toml"
        self._write_parent_manifest(
            image="research/contextswarm-mini:paper",
            memory_mb=65_536,
        )
        self.child_manifest.write_text(
            'extends = ["parent.toml"]\n',
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_parent_manifest(
        self,
        *,
        image: str,
        memory_mb: int,
        network: str = "host",
    ) -> None:
        self.parent_manifest.write_text(
            'extends = ["../../configs/smoke.toml"]\n\n'
            "[docker]\n"
            f'image = "{image}"\n'
            f"memory_mb = {memory_mb}\n"
            f'network = "{network}"\n',
            encoding="utf-8",
        )

    def _run(
        self,
        *,
        config: Path | None = None,
        **overrides: str,
    ) -> subprocess.CompletedProcess[str]:
        if self.capture.exists():
            self.capture.unlink()
        if self.inspect_capture.exists():
            self.inspect_capture.unlink()
        env = os.environ.copy()
        env.pop("CONTEXTSWARM_MINI_IMAGE", None)
        env.pop("CONTEXTSWARM_MINI_MEMORY", None)
        for profiling_env in (
            "CONTEXTSWARM_PROFILE",
            "CONTEXTSWARM_RESOURCE_PROFILING",
            "CONTEXTSWARM_PROFILING",
            "CONTEXTSWARM_PROFILE_HEARTBEAT_SECONDS",
            "CONTEXTSWARM_PROFILE_INTERVAL_SECONDS",
            "CONTEXTSWARM_PROFILE_PATH",
        ):
            env.pop(profiling_env, None)
        env.update(overrides)
        env.update(
            {
                "PATH": f"{self.bin_dir}:{env['PATH']}",
                "FAKE_DOCKER_CAPTURE": str(self.capture),
                "FAKE_DOCKER_INSPECT_CAPTURE": str(self.inspect_capture),
                "CONTEXTSWARM_NUROUTER_BINARY": str(self.fake_nurouter),
            }
        )
        return subprocess.run(
            [
                "/bin/bash",
                str(RUN_DOCKER),
                "--config",
                str((config or self.child_manifest).relative_to(ROOT)),
                "--mock-agent",
            ],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )

    def _captured_argv(self) -> list[str]:
        return json.loads(self.capture.read_text(encoding="utf-8"))

    def _captured_inspects(self) -> list[list[str]]:
        return [
            json.loads(line)
            for line in self.inspect_capture.read_text(encoding="utf-8").splitlines()
        ]

    def test_inherited_manifest_resources_reach_actual_docker_argv(self) -> None:
        result = self._run()

        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self._captured_argv()
        self.assertEqual(argv[0], "run")
        self.assertEqual(argv[argv.index("--memory") + 1], "65536m")
        self.assertEqual(argv[argv.index("--network") + 1], "host")
        self.assertNotIn("--add-host", argv)
        config_index = argv.index("--config")
        self.assertEqual(config_index - 1, argv.index("sha256:" + ("1" * 64)))
        self.assertEqual(
            self._captured_inspects()[0][-1],
            "research/contextswarm-mini:paper",
        )
        self.assertEqual(
            argv[config_index + 1],
            str(self.child_manifest.relative_to(ROOT)),
        )

    def test_operator_environment_overrides_manifest_resources(self) -> None:
        result = self._run(
            CONTEXTSWARM_MINI_IMAGE="registry.example:5000/paper/mini:operator",
            CONTEXTSWARM_MINI_MEMORY="64g",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self._captured_argv()
        self.assertEqual(argv[argv.index("--memory") + 1], "64g")
        self.assertEqual(argv[argv.index("--config") - 1], "sha256:" + ("1" * 64))
        self.assertEqual(
            self._captured_inspects()[0][-1],
            "registry.example:5000/paper/mini:operator",
        )

    def test_opt_in_profiling_environment_is_forwarded_by_name_only(self) -> None:
        private_profile_path = str(self.temp / "operator-private-profile.jsonl")
        result = self._run(
            CONTEXTSWARM_PROFILE="1",
            CONTEXTSWARM_RESOURCE_PROFILING="1",
            CONTEXTSWARM_PROFILING="1",
            CONTEXTSWARM_PROFILE_HEARTBEAT_SECONDS="0.25",
            CONTEXTSWARM_PROFILE_INTERVAL_SECONDS="2",
            CONTEXTSWARM_PROFILE_PATH=private_profile_path,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self._captured_argv()
        for variable in (
            "CONTEXTSWARM_PROFILE",
            "CONTEXTSWARM_RESOURCE_PROFILING",
            "CONTEXTSWARM_PROFILING",
            "CONTEXTSWARM_PROFILE_HEARTBEAT_SECONDS",
            "CONTEXTSWARM_PROFILE_INTERVAL_SECONDS",
            "CONTEXTSWARM_PROFILE_PATH",
        ):
            variable_index = argv.index(variable)
            self.assertEqual(argv[variable_index - 1], "-e")
            self.assertNotIn(f"{variable}=", argv)
        self.assertNotIn("0.25", argv)
        self.assertNotIn("2", argv)
        self.assertNotIn(private_profile_path, argv)
        self.assertNotIn(private_profile_path, result.stdout)
        self.assertNotIn(private_profile_path, result.stderr)

    def test_pid_limit_has_cps48_headroom_and_remains_operator_overridable(self) -> None:
        result = self._run()

        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self._captured_argv()
        self.assertEqual(argv[argv.index("--pids-limit") + 1], "2048")

        overridden = self._run(CONTEXTSWARM_MINI_PIDS_LIMIT="3072")

        self.assertEqual(overridden.returncode, 0, overridden.stderr)
        argv = self._captured_argv()
        self.assertEqual(argv[argv.index("--pids-limit") + 1], "3072")

    def test_bridge_network_is_manifest_selected_with_host_gateway_alias(self) -> None:
        self._write_parent_manifest(
            image="research/contextswarm-mini:paper",
            memory_mb=65_536,
            network="bridge",
        )

        result = self._run()

        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self._captured_argv()
        self.assertEqual(argv[argv.index("--network") + 1], "bridge")
        self.assertEqual(
            argv[argv.index("--add-host") + 1],
            "host.docker.internal:host-gateway",
        )

    def test_invalid_manifest_network_fails_before_docker(self) -> None:
        self._write_parent_manifest(
            image="research/contextswarm-mini:paper",
            memory_mb=65_536,
            network="experiment-net",
        )

        result = self._run()

        self.assertEqual(result.returncode, 2)
        self.assertIn("docker.network must be host or bridge", result.stderr)
        self.assertFalse(self.capture.exists())

    def test_unsafe_manifest_and_operator_values_fail_before_docker(self) -> None:
        sentinel = self.temp / "must-not-exist"
        cases = (
            (
                {"CONTEXTSWARM_MINI_IMAGE": f"image:tag$(touch {sentinel})"},
                "invalid Docker image",
            ),
            (
                {"CONTEXTSWARM_MINI_MEMORY": "64g --privileged"},
                "invalid Docker memory",
            ),
        )
        for overrides, expected_error in cases:
            with self.subTest(overrides=overrides):
                result = self._run(**overrides)
                self.assertEqual(result.returncode, 2)
                self.assertIn(expected_error, result.stderr)
                self.assertFalse(self.capture.exists())
                self.assertFalse(sentinel.exists())

        self._write_parent_manifest(image="--privileged", memory_mb=65_536)
        result = self._run()
        self.assertEqual(result.returncode, 2)
        self.assertIn("invalid Docker image", result.stderr)
        self.assertFalse(self.capture.exists())

    def test_untracked_manifest_outside_runs_is_not_container_visible(self) -> None:
        manifest = ROOT / ".untracked-launch-manifest.toml"
        manifest.write_text(
            'extends = ["configs/smoke.toml"]\n',
            encoding="utf-8",
        )
        try:
            result = self._run(config=manifest)
        finally:
            manifest.unlink(missing_ok=True)

        self.assertEqual(result.returncode, 2)
        self.assertIn("tracked or located below runs", result.stderr)
        self.assertFalse(self.capture.exists())


if __name__ == "__main__":
    unittest.main()
