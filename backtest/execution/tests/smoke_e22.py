from __future__ import annotations

import ast
import builtins
import importlib.util
import io
import os
import socket
import subprocess
import sys
import urllib.request
from contextlib import ExitStack, redirect_stdout
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pandas as pd


FORBIDDEN_IMPORT_PREFIXES = (
    "backtest.execution.testnet_command_gateway",
    "backtest.execution.testnet_submit_adapter",
    "backtest.execution.testnet_real_submit",
    "backtest.execution.end_to_end_testnet_drill",
    "backtest.execution.bybit_read_only_adapter",
    "backtest.execution.exchange_read_adapter",
    "backtest.execution.exchange_reconciler",
)


def _row(
    key: str = "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|E22.2",
    *,
    symbol: str = "BTCUSDT",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "model": "RANGE_TOP_SHORT_V2",
        "side": "SHORT",
        "canonical_setup_key": key,
        "setup_id": f"setup-{symbol}-e22-2",
        "setup_created_ts": "2026-07-21T10:00:00+00:00",
        "signal_ts": "2026-07-21T10:15:00+00:00",
        "visible_ts": "2026-07-21T10:15:00+00:00",
        "wait_confirm_ts": "2026-07-21T10:15:00+00:00",
        "intended_entry_ts": "2026-07-21T10:30:00+00:00",
        "entry_window_expires_ts": "2026-07-21T11:00:00+00:00",
        "selected_for_execution": True,
        "execution_rank": 1,
        "selection_reason": "signal_score_rank_1",
        "entry": Decimal("100"),
        "sl": Decimal("101"),
        "tp": Decimal("97"),
        "planned_rr": Decimal("3"),
        "risk_pct": Decimal("0.01"),
        "reward_pct": Decimal("0.03"),
        "risk_distance": Decimal("1"),
        "reward_distance": Decimal("3"),
        "authorized_qty": Decimal("0.5"),
        "authorized_notional": Decimal("50"),
        "authorized_risk_usd": Decimal("0.5"),
        "risk_snapshot_id": "risk-1",
        "position_snapshot_id": "position-1",
        "authority_waterfall_id": "authority-1",
    }


def _guard_import(original_import):
    def guarded(name, globals=None, locals=None, fromlist=(), level=0):
        absolute_name = name
        if level > 0 and globals is not None and globals.get("__package__"):
            absolute_name = importlib.util.resolve_name(
                "." * level + name,
                str(globals["__package__"]),
            )
        if absolute_name.startswith(FORBIDDEN_IMPORT_PREFIXES):
            raise AssertionError(f"forbidden execution path imported: {absolute_name}")
        return original_import(name, globals, locals, fromlist, level)

    return guarded


def _mutation_guards():
    original_path_open = Path.open
    original_open = builtins.open

    def reject_mode(mode):
        if any(flag in str(mode) for flag in ("w", "a", "x", "+")):
            raise AssertionError(f"filesystem mutation reached: mode={mode}")

    def guarded_path_open(self, mode="r", *args, **kwargs):
        reject_mode(mode)
        return original_path_open(self, mode, *args, **kwargs)

    def guarded_open(file, mode="r", *args, **kwargs):
        reject_mode(mode)
        return original_open(file, mode, *args, **kwargs)

    return (
        patch.object(Path, "open", guarded_path_open),
        patch.object(Path, "write_text", side_effect=AssertionError("Path.write_text mutation reached")),
        patch.object(Path, "write_bytes", side_effect=AssertionError("Path.write_bytes mutation reached")),
        patch.object(Path, "mkdir", side_effect=AssertionError("Path.mkdir mutation reached")),
        patch.object(builtins, "open", guarded_open),
    )


def _clean_probe_main() -> int:
    sys.dont_write_bytecode = True
    original_import = builtins.__import__
    with ExitStack() as stack:
        stack.enter_context(patch.object(builtins, "__import__", _guard_import(original_import)))
        import backtest.execution.shadow_integration_boundary as boundary

        for guard in _mutation_guards():
            stack.enter_context(guard)
        stack.enter_context(
            patch.object(socket, "create_connection", side_effect=AssertionError("exchange socket reached"))
        )
        stack.enter_context(
            patch.object(urllib.request, "urlopen", side_effect=AssertionError("exchange HTTP reached"))
        )
        result = boundary.evaluate_ats_shadow_admission(
            _row("BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|CLEAN_IMPORT"),
            cycle_ts="2026-07-21T10:30:00+00:00",
            checked_at_utc="2026-07-21T10:30:00+00:00",
            paper_mode=True,
        )
    assert result.paper_completed
    assert not result.paper_failed
    assert not result.paper_duplicate_blocked
    print("CLEAN_E22_IMPORT_AND_MUTATION_PROBE_OK")
    return 0


def _clean_import_reachability_probe() -> None:
    env = dict(os.environ)
    env["ATS_E22_CLEAN_PROBE"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        [sys.executable, "-m", "backtest.execution.tests.smoke_e22"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        raise AssertionError(
            "clean E22 import/mutation probe failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    assert "CLEAN_E22_IMPORT_AND_MUTATION_PROBE_OK" in completed.stdout


def _load_shell_wrapper():
    shell_path = Path("backtest/journal/live_observation_shell.py")
    source = shell_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(shell_path))
    wanted = {"_run_executor_shadow_diagnostic", "_emit_with_optional_executor_shadow"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted]
    assert {node.name for node in nodes} == wanted

    emit_calls = []

    def fake_emit_observation_rows(**kwargs):
        emit_calls.append(kwargs)
        return 1

    namespace = {"pd": pd, "_emit_observation_rows": fake_emit_observation_rows}
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, str(shell_path), "exec"), namespace)
    return namespace["_emit_with_optional_executor_shadow"], emit_calls


def _forced_executor_failure(*args, **kwargs):
    raise RuntimeError("sentinel_paper_executor_failure")


def main() -> int:
    _clean_import_reachability_probe()

    import backtest.execution.shadow_integration_boundary as boundary
    from backtest.execution.decision_consumer import consume_decision_row
    from backtest.execution.execution_event_ledger import ExecutionEventLedger, SIMULATED_FILLED
    from backtest.execution.execution_identity_registry import ExecutionIdentityRegistry
    from backtest.execution.execution_simulator import (
        ACKED,
        EXCHANGE_UNAVAILABLE,
        PARTIALLY_FILLED,
        REJECTED,
        TIMEOUT_UNKNOWN,
    )
    from backtest.execution.paper_executor import PaperExecutionRequest, PaperExecutor
    from backtest.execution.recovery_simulator import (
        BLOCK_AND_RECONCILE,
        EXCHANGE_UNAVAILABLE_FAIL_CLOSED,
        MARK_PARTIAL_PENDING,
        MARK_REJECTED_TERMINAL,
        WAIT_FOR_RECHECK,
    )

    boundary._SESSION_PAPER_EXECUTOR = None
    row = _row()
    before = deepcopy(row)
    kwargs = {
        "cycle_ts": "2026-07-21T10:30:00+00:00",
        "checked_at_utc": "2026-07-21T10:30:00+00:00",
        "paper_mode": True,
    }

    original_import = builtins.__import__
    with ExitStack() as stack:
        stack.enter_context(patch.object(builtins, "__import__", _guard_import(original_import)))
        for guard in _mutation_guards():
            stack.enter_context(guard)
        stack.enter_context(
            patch.object(socket, "create_connection", side_effect=AssertionError("exchange socket reached"))
        )
        stack.enter_context(
            patch.object(urllib.request, "urlopen", side_effect=AssertionError("exchange HTTP reached"))
        )
        opened = boundary.evaluate_ats_shadow_admission(row, **kwargs)
        executor = boundary._SESSION_PAPER_EXECUTOR
        assert executor is not None
        first_events = executor.ledger.events
        first_snapshot = executor.ledger.rebuild_snapshot(str(row["canonical_setup_key"]))
        duplicate = boundary.evaluate_ats_shadow_admission(row, **kwargs)
        second_events = executor.ledger.events

    assert row == before
    assert opened.paper_requested and opened.paper_started and opened.paper_completed
    assert not opened.paper_failed and not opened.paper_duplicate_blocked
    assert opened.paper_state == "FILLED_CONFIRMED" and opened.paper_event_count == 5
    assert duplicate.paper_requested and duplicate.paper_started and duplicate.paper_completed
    assert not duplicate.paper_failed and duplicate.paper_duplicate_blocked
    assert duplicate.paper_state == "FILLED_CONFIRMED"
    assert duplicate.paper_event_count == opened.paper_event_count
    assert second_events == first_events

    identity_events = [event for event in second_events if event.canonical_setup_key == row["canonical_setup_key"]]
    assert len(identity_events) == 5 and identity_events[-1].event_type == SIMULATED_FILLED
    assert [event.sequence for event in second_events] == list(range(1, len(second_events) + 1))
    snapshot = executor.ledger.rebuild_snapshot(str(row["canonical_setup_key"]))
    assert snapshot == first_snapshot and snapshot.current_state == "FILLED_CONFIRMED"
    assert executor.identity_registry.contains(str(row["canonical_setup_key"]))

    boundary._SESSION_PAPER_EXECUTOR = None
    non_paper = boundary.evaluate_ats_shadow_admission(
        _row(),
        cycle_ts=kwargs["cycle_ts"],
        checked_at_utc=kwargs["checked_at_utc"],
        paper_mode=False,
    )
    assert non_paper.accepted_by_ingress and non_paper.mechanical_allowed
    assert not non_paper.paper_requested and boundary._SESSION_PAPER_EXECUTOR is None

    wrapper, emit_calls = _load_shell_wrapper()
    boundary._SESSION_PAPER_EXECUTOR = None
    diagnostic_output = io.StringIO()
    with redirect_stdout(diagnostic_output):
        first_written = wrapper(
            out_csv="executor-local-smoke.csv",
            symbol="BTCUSDT",
            latest_ts=kwargs["cycle_ts"],
            out_df=pd.DataFrame([_row()]),
            cycle_ts=kwargs["cycle_ts"],
            paper_mode=True,
        )
        second_written = wrapper(
            out_csv="executor-local-smoke.csv",
            symbol="BTCUSDT",
            latest_ts=kwargs["cycle_ts"],
            out_df=pd.DataFrame([_row()]),
            cycle_ts=kwargs["cycle_ts"],
            paper_mode=True,
        )
    diagnostics = diagnostic_output.getvalue()
    assert diagnostics.count("paper_duplicate_blocked=0") == 1
    assert diagnostics.count("paper_duplicate_blocked=1") == 1
    assert first_written == 1 and second_written == 1 and len(emit_calls) == 2

    failure_wrapper, failure_emit_calls = _load_shell_wrapper()
    with patch.object(boundary, "evaluate_ats_shadow_admission", side_effect=_forced_executor_failure):
        written = failure_wrapper(
            out_csv="executor-local-smoke.csv",
            symbol="BTCUSDT",
            latest_ts=kwargs["cycle_ts"],
            out_df=pd.DataFrame([_row()]),
            cycle_ts=kwargs["cycle_ts"],
            paper_mode=True,
        )
    assert written == 1 and len(failure_emit_calls) == 1

    legacy_executor = PaperExecutor(
        ledger=ExecutionEventLedger(),
        identity_registry=ExecutionIdentityRegistry(),
    )
    legacy_expectations = {
        ACKED: WAIT_FOR_RECHECK,
        REJECTED: MARK_REJECTED_TERMINAL,
        PARTIALLY_FILLED: MARK_PARTIAL_PENDING,
        TIMEOUT_UNKNOWN: BLOCK_AND_RECONCILE,
        EXCHANGE_UNAVAILABLE: EXCHANGE_UNAVAILABLE_FAIL_CLOSED,
    }
    for index, (outcome, expected_state) in enumerate(legacy_expectations.items(), start=1):
        legacy_row = {
            key: str(value)
            for key, value in _row(f"LEGACY|{outcome}|{index}", symbol=f"LEGACY{index}").items()
        }
        legacy_row["schema_version"] = "ATS_EXECUTION_INTENT_V1"
        legacy_row["cycle_ts"] = kwargs["cycle_ts"]
        legacy_snapshot = legacy_executor.run(
            consume_decision_row(legacy_row),
            PaperExecutionRequest(
                simulation_outcome=outcome,
                checked_at_utc=kwargs["checked_at_utc"],
                simulated_at_utc=kwargs["checked_at_utc"],
                decided_at_utc=kwargs["checked_at_utc"],
            ),
        )
        assert legacy_snapshot.current_state == expected_state

    shell_source = Path("backtest/journal/live_observation_shell.py").read_text(encoding="utf-8")
    assert '"--executor_paper_mode"' in shell_source
    assert "paper_mode=True" not in shell_source
    assert shell_source.count("evaluate_ats_shadow_admission(") == 1
    assert "paper_duplicate_blocked=" in shell_source

    print(
        "SMOKE_E22_2_OK "
        "paper_open=1 paper_fill=1 session_duplicate_prevention=1 "
        "duplicate_event_count_unchanged=1 duplicate_diagnostic=1 "
        "ledger_ordering=1 executor_snapshot=1 shell_continuation_after_executor_failure=1 "
        "paper_cli_authority=1 non_paper_path_unchanged=1 journal_mutation_sentinel=1 "
        "gateway_unreachable=1 testnet_unreachable=1 live_unreachable=1 exchange_unreachable=1"
    )
    return 0


if __name__ == "__main__":
    if os.environ.get("ATS_E22_CLEAN_PROBE") == "1":
        raise SystemExit(_clean_probe_main())
    raise SystemExit(main())
