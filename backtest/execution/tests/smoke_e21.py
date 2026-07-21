from __future__ import annotations

import ast
import builtins
import importlib
import socket
import urllib.request
from copy import deepcopy
from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import backtest.execution.shadow_integration_boundary as boundary
from backtest.execution.shadow_integration_boundary import evaluate_ats_shadow_admission


FORBIDDEN_IMPORT_PREFIXES = (
    "backtest.execution.paper_executor",
    "backtest.execution.command_gateway",
    "backtest.execution.intent_journal",
    "backtest.execution.testnet_real_submit",
    "backtest.execution.exchange",
)


def _row() -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "model": "RANGE_TOP_SHORT_V2",
        "side": "SHORT",
        "canonical_setup_key": "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|1",
        "setup_id": "setup-1",
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


def _forbidden_import_guard(original_import):
    def guarded(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith(FORBIDDEN_IMPORT_PREFIXES):
            raise AssertionError(f"forbidden executor path imported: {name}")
        return original_import(name, globals, locals, fromlist, level)

    return guarded


def _unexpected_failure(*args, **kwargs):
    raise RuntimeError("sentinel_executor_failure")




class _RowExtractionFailure:
    @property
    def empty(self):
        raise RuntimeError("sentinel_row_extraction_failure")


def _load_shell_boundary_functions():
    shell_path = Path("backtest/journal/live_observation_shell.py")
    source = shell_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(shell_path))
    wanted = {"_run_executor_shadow_diagnostic", "_emit_with_optional_executor_shadow"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted]
    assert {node.name for node in nodes} == wanted

    emit_calls = []

    def fake_emit_observation_rows(**kwargs):
        emit_calls.append(kwargs)
        return 7

    namespace = {
        "pd": __import__("pandas"),
        "_emit_observation_rows": fake_emit_observation_rows,
    }
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, str(shell_path), "exec"), namespace)
    return namespace, emit_calls


def _assert_emit_continues_after_shadow_failure(*, out_df, failure_kind: str) -> None:
    namespace, emit_calls = _load_shell_boundary_functions()
    emit_wrapper = namespace["_emit_with_optional_executor_shadow"]

    if failure_kind == "executor":
        with patch.object(boundary, "evaluate_ats_shadow_admission", side_effect=_unexpected_failure):
            written = emit_wrapper(
                out_csv="executor-local-smoke.csv",
                symbol="BTCUSDT",
                latest_ts="2026-07-21T10:30:00+00:00",
                out_df=out_df,
                cycle_ts="2026-07-21T10:30:00+00:00",
            )
    elif failure_kind == "row_extraction":
        written = emit_wrapper(
            out_csv="executor-local-smoke.csv",
            symbol="BTCUSDT",
            latest_ts="2026-07-21T10:30:00+00:00",
            out_df=out_df,
            cycle_ts="2026-07-21T10:30:00+00:00",
        )
    else:
        raise AssertionError(f"unsupported failure kind: {failure_kind}")

    assert written == 7
    assert len(emit_calls) == 1
    assert emit_calls[0]["df_e"] is out_df


def main() -> int:
    row = _row()
    before = deepcopy(row)
    kwargs = {
        "cycle_ts": "2026-07-21T10:30:00+00:00",
        "checked_at_utc": "2026-07-21T10:30:00+00:00",
    }

    original_import = builtins.__import__
    with (
        patch.object(builtins, "__import__", _forbidden_import_guard(original_import)),
        patch.object(Path, "open", side_effect=AssertionError("journal/file mutation reached")),
        patch.object(Path, "write_text", side_effect=AssertionError("file mutation reached")),
        patch.object(Path, "write_bytes", side_effect=AssertionError("file mutation reached")),
        patch.object(socket, "create_connection", side_effect=AssertionError("exchange socket reached")),
        patch.object(urllib.request, "urlopen", side_effect=AssertionError("exchange HTTP reached")),
    ):
        first = evaluate_ats_shadow_admission(row, **kwargs)
        second = evaluate_ats_shadow_admission(row, **kwargs)

    assert first == second
    assert first.accepted_by_ingress and first.mechanical_allowed
    assert row == before

    missing_rank = _row()
    del missing_rank["execution_rank"]
    blocked_rank = evaluate_ats_shadow_admission(missing_rank, **kwargs)
    assert not blocked_rank.accepted_by_ingress
    assert "execution_rank" in blocked_rank.reason

    missing_selection = _row()
    del missing_selection["selection_reason"]
    blocked_selection = evaluate_ats_shadow_admission(missing_selection, **kwargs)
    assert not blocked_selection.accepted_by_ingress
    assert "selection_reason" in blocked_selection.reason

    expired = _row()
    expired["entry_window_expires_ts"] = "2026-07-21T10:29:00+00:00"
    veto = evaluate_ats_shadow_admission(expired, **kwargs)
    assert veto.accepted_by_ingress
    assert not veto.mechanical_allowed
    assert veto.reason == "intent_expired"

    with patch.object(boundary.ExecutionIntent, "from_row", side_effect=_unexpected_failure):
        internal_error = evaluate_ats_shadow_admission(_row(), **kwargs)
    assert not internal_error.accepted_by_ingress
    assert not internal_error.mechanical_allowed
    assert internal_error.severity == "CRITICAL"
    assert internal_error.reason.startswith("EXECUTOR_SHADOW_INTERNAL_ERROR:RuntimeError:")

    immutable = False
    try:
        first.reason = "changed"  # type: ignore[misc]
    except FrozenInstanceError:
        immutable = True
    assert immutable

    _assert_emit_continues_after_shadow_failure(
        out_df=__import__("pandas").DataFrame([_row()]),
        failure_kind="executor",
    )
    row_failure = _RowExtractionFailure()
    _assert_emit_continues_after_shadow_failure(
        out_df=row_failure,
        failure_kind="row_extraction",
    )

    shell_path = Path("backtest/journal/live_observation_shell.py")
    shell_source = shell_path.read_text(encoding="utf-8")
    assert shell_source.count("evaluate_ats_shadow_admission(") == 1
    helper_index = shell_source.index("def _run_executor_shadow_diagnostic(")
    try_index = shell_source.index("    try:", helper_index)
    validation_index = shell_source.index("if out_df is None or out_df.empty:", try_index)
    extraction_index = shell_source.index("shadow_source_row = dict(out_df.iloc[0])", validation_index)
    import_index = shell_source.index("from backtest.execution.shadow_integration_boundary import", extraction_index)
    hook_index = shell_source.index("evaluate_ats_shadow_admission(", import_index)
    diagnostic_index = shell_source.index("diagnostic = (", hook_index)
    except_index = shell_source.index("    except Exception as exc:", diagnostic_index)
    wrapper_index = shell_source.index("def _emit_with_optional_executor_shadow(")
    shadow_call_index = shell_source.index("_run_executor_shadow_diagnostic(", wrapper_index)
    emit_index = shell_source.index("return _emit_observation_rows(", shadow_call_index)
    assert try_index < validation_index < extraction_index < import_index < hook_index < diagnostic_index < except_index
    assert wrapper_index < shadow_call_index < emit_index
    assert "[EXECUTOR_SHADOW_INTERNAL_ERROR]" in shell_source

    print(
        "SMOKE_E21_2_OK "
        "validated_path=1 missing_rank_fail_closed=1 missing_selection_fail_closed=1 "
        "mechanical_veto=1 unexpected_internal_error_diagnostic=1 shell_continuation_guard=1 runtime_emit_after_executor_failure=1 runtime_emit_after_row_extraction_failure=1 "
        "deterministic=1 source_immutable=1 result_immutable=1 "
        "journal_mutation_sentinel=1 exchange_call_sentinel=1 "
        "gateway_import_sentinel=1 paper_executor_import_sentinel=1 "
        "single_boundary_call=1"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
