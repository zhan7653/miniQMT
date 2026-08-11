"""Small durable file-publication primitives.

The helpers deliberately distinguish replacement from immutable publication.
``os.replace`` is safe only for a named mutable pointer/checkpoint; immutable
artifacts are published with a hard link so a concurrent winner cannot be
clobbered after an optimistic existence check.
"""

from __future__ import annotations

import errno
import os
from pathlib import Path
import tempfile


def fsync_parent_directory(path: Path) -> bool:
    """Flush a directory entry when the platform permits it.

    Windows does not generally allow opening a directory for ``fsync``.  That
    documented limitation returns ``False``; other failures remain visible so
    callers never mistake an I/O error for a durable publication.
    """

    try:
        descriptor = os.open(path.parent, os.O_RDONLY)
    except OSError as exc:
        if os.name == "nt" and (isinstance(exc, PermissionError) or exc.winerror in {1, 5, 6, 267}):
            return False
        raise
    try:
        os.fsync(descriptor)
    except OSError as exc:
        if os.name == "nt" and exc.winerror in {1, 5, 6}:
            return False
        if exc.errno in {errno.EINVAL, errno.ENOTSUP}:
            return False
        raise
    finally:
        os.close(descriptor)
    return True


def _durable_temporary(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return temporary


def atomic_replace_bytes(path: Path, payload: bytes) -> None:
    """Durably stage bytes and atomically replace a mutable target."""

    temporary = _durable_temporary(path, payload)
    try:
        os.replace(temporary, path)
        fsync_parent_directory(path)
    finally:
        temporary.unlink(missing_ok=True)


def publish_immutable_bytes(path: Path, payload: bytes) -> None:
    """Publish immutable bytes without ever replacing a concurrent winner."""

    temporary = _durable_temporary(path, payload)
    try:
        try:
            # Linking is an atomic create-if-absent operation on the same volume.
            # It also works on supported Windows NTFS volumes.
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise ValueError(f"Immutable file collision: {path}") from None
        else:
            fsync_parent_directory(path)
    finally:
        temporary.unlink(missing_ok=True)
