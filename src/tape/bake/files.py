"""File operations the archive relies on: content hashes and atomic replacement.

Responsibility: hash a file's content, and replace a file or a directory so that a crash leaves
either the old or the new one, never a mixture (docs/ENGINEERING_STANDARDS.md 3.7).

Invariants: a hash is the lowercase hex SHA-256 of the whole file, read in bounded chunks; an
atomic write reaches the disk (``fsync``) before it is renamed into place, and the rename is
synced through the parent directory; a directory swap never deletes the old directory until the
new one is in place.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path
from typing import Final

__all__ = ["replace_dir", "sha256_file", "sync_dir", "write_atomic"]

_CHUNK_BYTES: Final = 1 << 20
_TEMPORARY_SUFFIX: Final = ".tmp"
_OLD_SUFFIX: Final = ".old"


def sha256_file(path: Path) -> str:
    """The SHA-256 of a file's content.

    Args:
        path: The file.

    Returns:
        Lowercase hex digest.

    Raises:
        OSError: If the file cannot be read.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def sync_dir(path: Path) -> None:
    """Flush a directory's entries to disk, so a rename or unlink inside it survives a crash.

    Raises:
        OSError: If the directory cannot be opened or synced.
    """
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_atomic(path: Path, data: bytes) -> None:
    """Replace a file's content whole: write a synced temporary sibling, then rename it.

    Args:
        path: The file; its parent directories are created when missing.
        data: The new content.

    Raises:
        OSError: If the file cannot be written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + _TEMPORARY_SUFFIX)
    with temporary.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    sync_dir(path.parent)


def replace_dir(target: Path, replacement: Path | None) -> None:
    """Put a finished directory in place of another, or remove the other when there is none.

    The old directory is renamed aside before the new one is renamed in, and deleted last, so a
    crash leaves the old directory, the new one, or both, and a later run repairs it.

    Args:
        target: Where the directory belongs.
        replacement: The finished directory, on the same file system; ``None`` removes ``target``.

    Raises:
        OSError: If a rename or removal fails.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    old = target.with_name(target.name + _OLD_SUFFIX)
    if old.exists():
        shutil.rmtree(old)
    if target.exists():
        target.replace(old)
    if replacement is not None:
        replacement.replace(target)
    sync_dir(target.parent)
    if old.exists():
        shutil.rmtree(old)
