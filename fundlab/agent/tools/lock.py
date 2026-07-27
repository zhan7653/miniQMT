"""A small cross-process advisory lock for one account's Agent review."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO


class AgentRunLocked(RuntimeError):
    """Another process is already reviewing this account."""


@dataclass
class AgentRunLock:
    root: Path
    account_id: str
    _handle: BinaryIO | None = field(init=False, default=None, repr=False)

    @property
    def path(self) -> Path:
        return Path(self.root) / ".locks" / f"{self.account_id}.lock"

    def __enter__(self) -> "AgentRunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - Windows is the production host
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise AgentRunLocked(
                f"Agent review already running for account {self.account_id}"
            ) from exc
        self._handle = handle
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:  # pragma: no cover - Windows is the production host
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._handle = None
            # On Windows an open lock file cannot be unlinked, so a competing
            # process that reached it in the unlock/cleanup gap keeps this
            # removal safely from succeeding.  POSIX unlink semantics would
            # permit split lock identities, so retain the file there.
            if os.name == "nt":
                try:
                    self.path.unlink()
                    self.path.parent.rmdir()
                except OSError:
                    pass
