"""Protected secret readers. No logging, defaults, mutation or value hashing.

The root is an operator-controlled per-service mount, not a caller-provided
HTTP path. The host boundary must protect its ancestors and mount authority.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import stat
from typing import Mapping


class SecretReadError(RuntimeError):
    def __init__(self, code: str, name: str = ''):
        self.code = code
        self.name = name if re.fullmatch(r'[A-Z][A-Z0-9_]*', name) else ''
        super().__init__(code + (':' + self.name if self.name else ''))


@dataclass(frozen=True)
class FilePolicy:
    root: Path = Path('/run/secrets')
    owner_uids: tuple[int, ...] = (0,)
    readable_gid: int | None = None
    maximum_bytes: int = 1024 * 1024


def _check(info, policy: FilePolicy, *, directory: bool):
    mode = stat.S_IMODE(info.st_mode)
    regular = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not regular or info.st_uid not in policy.owner_uids:
        raise SecretReadError('secret_file_authority_rejected')
    if mode & 0o022:
        raise SecretReadError('secret_file_writable_by_other_principal')
    if not directory:
        if info.st_nlink != 1 or mode & 0o007 or mode & 0o111:
            raise SecretReadError('secret_file_permissions_rejected')
        if mode & 0o040 and (policy.readable_gid is None or info.st_gid != policy.readable_gid):
            raise SecretReadError('secret_file_group_rejected')


def read_secret_bytes(path: str | Path, policy: FilePolicy = FilePolicy()) -> bytes:
    """Read without following any symlink within the trusted service root."""
    root, target = Path(policy.root), Path(path)
    if (not root.is_absolute() or root == Path('/') or '..' in root.parts
            or not target.is_absolute() or '..' in target.parts
            or not policy.owner_uids or policy.maximum_bytes < 1):
        raise SecretReadError('secret_file_scope_rejected')
    try:
        parts = target.relative_to(root).parts
    except ValueError:
        raise SecretReadError('secret_file_scope_rejected') from None
    if not parts:
        raise SecretReadError('secret_file_scope_rejected')
    directory_fd = None
    file_fd = None
    try:
        directory_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        _check(os.fstat(directory_fd), policy, directory=True)
        for part in parts[:-1]:
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory_fd)
            try:
                _check(os.fstat(next_fd), policy, directory=True)
            except BaseException:
                os.close(next_fd)
                raise
            os.close(directory_fd)
            directory_fd = next_fd
        # NONBLOCK prevents a malicious FIFO from hanging before fstat rejects it.
        file_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory_fd)
        before = os.fstat(file_fd)
        _check(before, policy, directory=False)
        if not 0 < before.st_size <= policy.maximum_bytes:
            raise SecretReadError('secret_file_size_rejected')
        chunks, count = [], 0
        while True:
            chunk = os.read(file_fd, min(65536, policy.maximum_bytes + 1 - count))
            if not chunk:
                break
            chunks.append(chunk)
            count += len(chunk)
            if count > policy.maximum_bytes:
                raise SecretReadError('secret_file_size_rejected')
        after = os.fstat(file_fd)
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns) or count != before.st_size:
            raise SecretReadError('secret_file_changed_during_read')
        return b''.join(chunks)
    except OSError:
        raise SecretReadError('secret_file_unavailable') from None
    finally:
        if file_fd is not None:
            os.close(file_fd)
        if directory_fd is not None:
            os.close(directory_fd)


def secret_text(name: str, *, environ: Mapping[str, str] | None = None,
                policy: FilePolicy = FilePolicy(), allow_legacy_environment: bool = False) -> str:
    """Resolve NAME_FILE, rejecting ambiguity; preserve whitespace and encoding.

Legacy fallback is opt-in for a qualified transition, never the default.
SSS_ENFORCED=1 prohibits it regardless of that opt-in. Existing applications
must deliberately adopt this function; simply mounting a file does nothing.
"""
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*', name):
        raise SecretReadError('invalid_secret_name')
    env = os.environ if environ is None else environ
    file_name = name + '_FILE'
    if name in env and file_name in env:
        raise SecretReadError('secret_sources_conflict', name)
    if file_name in env:
        try:
            if not env[file_name]:
                raise SecretReadError('secret_file_unavailable')
            return read_secret_bytes(env[file_name], policy).decode('utf-8')
        except UnicodeError:
            raise SecretReadError('secret_encoding_rejected', name) from None
        except SecretReadError as exc:
            raise SecretReadError(exc.code, name) from None
    if allow_legacy_environment and env.get('SSS_ENFORCED', '0') == '0' and env.get(name):
        if len(env[name].encode('utf-8')) > policy.maximum_bytes:
            raise SecretReadError('secret_file_size_rejected', name)
        return env[name]
    raise SecretReadError('secret_reference_required', name)
