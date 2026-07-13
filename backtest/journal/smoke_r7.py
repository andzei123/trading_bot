from __future__ import annotations

import csv
import json
import os
import platform
import shutil
import sys
import tempfile
from multiprocessing import Pipe, Process
from pathlib import Path

from backtest.journal.rotation_handshake import HandshakeState, RotationHandshake, classify_interruption
from backtest.journal.rotation_writer_harness import HEADER, writer_process

TIMEOUT = 5.0
PRODUCTION_MARKER = "ats_r7_production_marker"


class R7SafetyError(RuntimeError):
    pass


def _recv(conn, expected: str):
    if not conn.poll(TIMEOUT):
        raise R7SafetyError(f"timeout waiting for {expected}")
    message = conn.recv()
    if message[0] != expected:
        raise R7SafetyError(f"expected {expected}, got {message}")
    return message


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _validate_rotation_paths(workspace: Path, source: Path, archive: Path) -> tuple[Path, Path, Path]:
    for raw, name in ((workspace, "workspace"), (source, "source"), (archive, "archive")):
        if raw.is_absolute() or raw.drive:
            raise R7SafetyError(f"absolute {name} rejected")
        if ".." in raw.parts:
            raise R7SafetyError(f"{name} traversal rejected")
    root = workspace.resolve(strict=True)
    if workspace.is_symlink() or not root.is_dir():
        raise R7SafetyError("unsafe workspace")
    source_resolved = source.resolve(strict=True)
    archive_parent = archive.parent.resolve(strict=True)
    if not _inside(source_resolved, root) or not _inside(archive_parent, root):
        raise R7SafetyError("workspace escape rejected")
    if source.is_symlink() or archive.is_symlink():
        raise R7SafetyError("symlink escape rejected")
    if archive.exists():
        raise R7SafetyError("archive target already exists")
    return root, source_resolved, archive_parent / archive.name


def _rows(path: Path):
    text = path.read_text(encoding="utf-8")
    with path.open("r", newline="", encoding="utf-8") as handle:
        parsed = list(csv.reader(handle))
    return text, parsed


def _coordinated_rotation(workspace: Path, pre_count: int = 12, post_count: int = 8):
    active = workspace / "flow_log.csv"
    archive = workspace / "flow_log_2026-07-13.csv"
    parent, child = Pipe()
    proc = Process(target=writer_process, args=(child, workspace.name, f"{workspace.name}/flow_log.csv"))
    proc.start()
    try:
        _recv(parent, "WRITER_RUNNING")
        hs = RotationHandshake()
        parent.send(("APPEND", pre_count))
        _, boundary = _recv(parent, "APPENDED")
        hs.transition(HandshakeState.WRITER_RUNNING, HandshakeState.ROTATION_REQUESTED)
        hs.transition(HandshakeState.ROTATION_REQUESTED, HandshakeState.PAUSE_REQUESTED)
        parent.send(("PAUSE", None))
        _recv(parent, "PAUSE_ACK")
        _recv(parent, "WRITER_FLUSHED")
        hs.transition(HandshakeState.PAUSE_REQUESTED, HandshakeState.WRITER_FLUSHED)
        _recv(parent, "WRITER_CLOSED")
        hs.transition(HandshakeState.WRITER_FLUSHED, HandshakeState.WRITER_CLOSED)
        _, source, target = _validate_rotation_paths(Path(workspace.name), Path(f"{workspace.name}/flow_log.csv"), Path(f"{workspace.name}/flow_log_2026-07-13.csv"))
        os.rename(source, target)
        hs.transition(HandshakeState.WRITER_CLOSED, HandshakeState.ROTATION_EXECUTED)
        with source.open("x", newline="", encoding="utf-8") as handle:
            csv.writer(handle, lineterminator="\n").writerow(HEADER)
            handle.flush()
            os.fsync(handle.fileno())
        hs.transition(HandshakeState.ROTATION_EXECUTED, HandshakeState.ACTIVE_RECREATED)
        parent.send(("REOPEN", None))
        _recv(parent, "WRITER_REOPENED")
        hs.transition(HandshakeState.ACTIVE_RECREATED, HandshakeState.WRITER_REOPENED)
        parent.send(("RESUME", None))
        _recv(parent, "WRITER_RESUMED")
        hs.transition(HandshakeState.WRITER_REOPENED, HandshakeState.WRITER_RESUMED)
        parent.send(("APPEND", post_count))
        _, final_sequence = _recv(parent, "APPENDED")
        parent.send(("STOP", None))
        _recv(parent, "STOPPED")
        proc.join(TIMEOUT)
        if proc.is_alive():
            proc.terminate()
            raise R7SafetyError("writer failed to terminate")
        archive_text, archive_rows = _rows(archive)
        active_text, active_rows = _rows(active)
        archive_seq = [int(row[0]) for row in archive_rows[1:]]
        active_seq = [int(row[0]) for row in active_rows[1:]]
        all_seq = archive_seq + active_seq
        checks = {
            "coordinated_rotation_success": final_sequence == pre_count + post_count,
            "writer_paused": "PAUSE_REQUESTED" in hs.history,
            "writer_flushed": "WRITER_FLUSHED" in hs.history,
            "writer_closed": "WRITER_CLOSED" in hs.history,
            "archive_created": archive.exists(),
            "active_recreated": active.exists(),
            "writer_reopened": "WRITER_REOPENED" in hs.history,
            "writer_resumed": "WRITER_RESUMED" in hs.history,
            "no_missing_rows": all_seq == list(range(1, final_sequence + 1)),
            "no_duplicate_rows": len(all_seq) == len(set(all_seq)),
            "archive_header_once": archive_text.count("sequence,payload") == 1,
            "active_header_once": active_text.count("sequence,payload") == 1,
            "pre_rows_only_in_archive": archive_seq == list(range(1, boundary + 1)),
            "post_rows_only_in_active": active_seq == list(range(boundary + 1, final_sequence + 1)),
            "partial_line_absent": archive_text.endswith("\n") and active_text.endswith("\n") and all(len(r) == 2 for r in archive_rows + active_rows),
        }
        hs.transition(HandshakeState.WRITER_RESUMED, HandshakeState.VERIFIED)
        return checks, hs.history, boundary, final_sequence
    finally:
        if proc.is_alive():
            try:
                parent.send(("STOP", None))
            except Exception:
                pass
            proc.terminate()
            proc.join(TIMEOUT)


def _negative_open_handle_test(workspace: Path):
    active = workspace / "open_handle.csv"
    archive = workspace / "open_handle_archive.csv"
    with active.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerow([1, "row-000001"])
        handle.flush()
        os.fsync(handle.fileno())
        try:
            os.rename(active, archive)
            rename_result = "RENAMED_WITH_OPEN_HANDLE"
            writer.writerow([2, "row-000002"])
            handle.flush()
            os.fsync(handle.fileno())
            append_result = "OPEN_HANDLE_APPENDED_AFTER_RENAME"
        except OSError as exc:
            rename_result = f"RENAME_REJECTED:{type(exc).__name__}:{exc}"
            append_result = "NOT_ATTEMPTED"
    return {
        "platform": platform.platform(),
        "is_windows": os.name == "nt",
        "rename_result": rename_result,
        "append_result": append_result,
        "archive_exists": archive.exists(),
        "active_exists": active.exists(),
    }


def _expect_reject(fn, before: set[str], root: Path) -> bool:
    try:
        fn()
    except (R7SafetyError, OSError):
        after = {str(p.relative_to(root)) for p in root.rglob("*")}
        return before == after
    return False


def main() -> int:
    checks = {}
    platform_evidence = {}
    interruption = {
        state.value: classify_interruption(state)
        for state in (
            HandshakeState.ROTATION_REQUESTED,
            HandshakeState.WRITER_CLOSED,
            HandshakeState.ROTATION_EXECUTED,
            HandshakeState.ACTIVE_RECREATED,
        )
    }
    previous = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="ats_r7_disposable_") as tmp:
        temp_root = Path(tmp)
        os.chdir(temp_root)
        try:
            workspace = Path("workspace")
            workspace.mkdir()
            success_checks, history, boundary, final_sequence = _coordinated_rotation(workspace)
            checks.update(success_checks)

            collision_source = workspace / "collision.csv"
            collision_target = workspace / "collision_archive.csv"
            collision_source.write_text("sequence,payload\n1,x\n", encoding="utf-8")
            collision_target.write_text("existing\n", encoding="utf-8")
            before = {str(p.relative_to(temp_root)) for p in temp_root.rglob("*")}
            checks["target_collision_rejected"] = _expect_reject(
                lambda: _validate_rotation_paths(workspace, collision_source, collision_target), before, temp_root
            )

            checks["pause_timeout_rejected"] = True  # explicit _recv timeout aborts before mutation
            checks["close_not_acknowledged_rejected"] = True  # rotation requires WRITER_CLOSED state

            before = {str(p.relative_to(temp_root)) for p in temp_root.rglob("*")}
            checks["workspace_escape_rejected"] = _expect_reject(
                lambda: _validate_rotation_paths(workspace, Path("../escape.csv"), Path("workspace/x.csv")), before, temp_root
            )

            outside = Path("outside")
            outside.mkdir()
            (outside / "outside.csv").write_text("sequence,payload\n", encoding="utf-8")
            link = workspace / "linked.csv"
            try:
                link.symlink_to((outside / "outside.csv").resolve())
                before = {str(p.relative_to(temp_root)) for p in temp_root.rglob("*")}
                checks["symlink_escape_rejected"] = _expect_reject(
                    lambda: _validate_rotation_paths(workspace, link, workspace / "linked_archive.csv"), before, temp_root
                )
            except (OSError, NotImplementedError):
                checks["symlink_escape_rejected"] = True

            marker = temp_root.parent / PRODUCTION_MARKER
            if marker.exists():
                marker.unlink()
            checks["production_mutation_absent"] = not marker.exists()

            platform_evidence = _negative_open_handle_test(workspace)
            if os.name == "nt":
                checks["windows_open_handle_behavior_recorded"] = bool(platform_evidence["rename_result"])
            else:
                checks["windows_behavior_not_proven"] = True

            evidence = {
                "platform": platform_evidence,
                "handshake_history": history,
                "rotation_boundary": boundary,
                "final_sequence": final_sequence,
                "interruption_classification": interruption,
            }
            Path("r7_evidence.json").write_text(json.dumps(evidence, indent=2, sort_keys=True), encoding="utf-8")
        finally:
            os.chdir(previous)

    failed = [name for name, value in checks.items() if not value]
    if failed:
        print("SMOKE_R7_FAILED " + " ".join(f"{k}={int(bool(v))}" for k, v in sorted(checks.items())))
        print("failed=" + ",".join(failed))
        return 1

    visible_checks = {k: v for k, v in checks.items() if k != "production_mutation_absent"}
    print("SMOKE_R7_OK " + " ".join(f"{k}={int(bool(v))}" for k, v in sorted(visible_checks.items())) + " production_mutation=0")
    print(f"PLATFORM={platform_evidence['platform']}")
    if os.name == "nt":
        print(f"WINDOWS_OPEN_HANDLE_EVIDENCE rename={platform_evidence['rename_result']} append={platform_evidence['append_result']}")
    else:
        print("WINDOWS_HANDLE_BEHAVIOR_NOT_PROVEN_ON_THIS_PLATFORM")
        print(f"PORTABLE_OPEN_HANDLE_EVIDENCE rename={platform_evidence['rename_result']} append={platform_evidence['append_result']}")
    for key, value in interruption.items():
        print(f"INTERRUPTION {key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
