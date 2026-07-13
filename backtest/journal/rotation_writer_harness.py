from __future__ import annotations

"""Controlled deterministic CSV writer for R7 handle-safety validation."""

import csv
import os
from multiprocessing.connection import Connection
from pathlib import Path, PurePath
from typing import Iterable

HEADER = ["sequence", "payload"]


class WriterHarnessError(RuntimeError):
    pass


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def validate_workspace_file(workspace: Path, file_path: Path, *, must_exist: bool = False) -> tuple[Path, Path]:
    if workspace.is_absolute() or workspace.drive or file_path.is_absolute() or file_path.drive:
        raise WriterHarnessError("paths must be relative")
    if any(part == ".." for part in PurePath(str(workspace)).parts + PurePath(str(file_path)).parts):
        raise WriterHarnessError("path traversal rejected")
    root = workspace.resolve(strict=True)
    if workspace.is_symlink() or not root.is_dir():
        raise WriterHarnessError("unsafe workspace")
    candidate = file_path.resolve(strict=must_exist)
    parent = candidate.parent.resolve(strict=True)
    if not _is_relative_to(parent, root):
        raise WriterHarnessError("file escapes workspace")
    if must_exist and (file_path.is_symlink() or not candidate.is_file()):
        raise WriterHarnessError("file must be regular")
    return root, candidate


def _open_active(active_path: Path):
    exists = active_path.exists() and active_path.stat().st_size > 0
    handle = active_path.open("a", newline="", encoding="utf-8")
    writer = csv.writer(handle, lineterminator="\n")
    if not exists:
        writer.writerow(HEADER)
        handle.flush()
        os.fsync(handle.fileno())
    return handle, writer


def writer_process(conn: Connection, workspace: str, active_csv: str) -> None:
    handle = None
    writer = None
    sequence = 0
    paused = False
    try:
        _, active_path = validate_workspace_file(Path(workspace), Path(active_csv), must_exist=False)
        handle, writer = _open_active(active_path)
        conn.send(("WRITER_RUNNING", sequence))
        while True:
            command, value = conn.recv()
            if command == "APPEND":
                if paused or handle is None or writer is None:
                    raise WriterHarnessError("append requested while writer paused/closed")
                count = int(value)
                for _ in range(count):
                    sequence += 1
                    writer.writerow([sequence, f"row-{sequence:06d}"])
                    handle.flush()
                    os.fsync(handle.fileno())
                conn.send(("APPENDED", sequence))
            elif command == "PAUSE":
                paused = True
                conn.send(("PAUSE_ACK", sequence))
                if handle is None:
                    raise WriterHarnessError("writer handle already closed")
                handle.flush()
                os.fsync(handle.fileno())
                conn.send(("WRITER_FLUSHED", sequence))
                handle.close()
                handle = None
                writer = None
                conn.send(("WRITER_CLOSED", sequence))
            elif command == "REOPEN":
                if not paused or handle is not None:
                    raise WriterHarnessError("reopen requires paused closed writer")
                _, active_path = validate_workspace_file(Path(workspace), Path(active_csv), must_exist=True)
                handle, writer = _open_active(active_path)
                conn.send(("WRITER_REOPENED", sequence))
            elif command == "RESUME":
                if handle is None or writer is None:
                    raise WriterHarnessError("resume requires reopened handle")
                paused = False
                conn.send(("WRITER_RESUMED", sequence))
            elif command == "STOP":
                if handle is not None:
                    handle.flush()
                    os.fsync(handle.fileno())
                    handle.close()
                conn.send(("STOPPED", sequence))
                return
            else:
                raise WriterHarnessError(f"unknown command {command}")
    except BaseException as exc:
        try:
            if handle is not None:
                handle.close()
        finally:
            conn.send(("ERROR", f"{type(exc).__name__}: {exc}"))
