from pathlib import Path

import pytest
from perf_harness.config import load_experiment

_PERF_DIR = Path(__file__).parents[1]


@pytest.mark.parametrize(
    ("config_name", "case_ids"),
    [
        ("managed-agent-turn.yaml", ["sandbox_turn"]),
        ("managed-agent-scale-out.yaml", ["plain_turn"]),
        ("managed-agent-mixed-soak.yaml", ["plain_turn", "sandbox_turn"]),
    ],
)
def test_managed_agent_profile_loads_canonical_cases(
    config_name: str,
    case_ids: list[str],
) -> None:
    experiment, _ = load_experiment(_PERF_DIR / config_name)

    assert [case.id for case in experiment.cases] == case_ids


def test_mixed_soak_keeps_profile_local_case_weights() -> None:
    experiment, _ = load_experiment(_PERF_DIR / "managed-agent-mixed-soak.yaml")

    assert experiment.mix.overrides == {"plain_turn": 4.0, "sandbox_turn": 1.0}
