from pathlib import Path

from perf_harness.config import load_experiment


def test_managed_agent_profile_loads_canonical_case() -> None:
    perf_dir = Path(__file__).parents[1]

    experiment, _ = load_experiment(perf_dir / "managed-agent-turn.yaml")

    assert [case.id for case in experiment.cases] == ["sandbox_turn"]
