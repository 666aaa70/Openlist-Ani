"""Durable post-organization copy (and optional source deletion) for OpenList."""

from __future__ import annotations

import asyncio
import posixpath
import re
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import replace
from typing import Any

from openlist_ani.application.organization import OrganizationError, StorageEntry
from openlist_ani.application.ports import OrganizationResult
from openlist_ani.domain import DownloadJob
from openlist_ani.logger import logger

from .client import OpenListClient
from .models import OpenlistTask, OpenlistTaskState
from .storage import OpenListStorageOperations
from .workflow import _join_openlist_path

_TERMINAL_FAILURE_STATES = frozenset(
    {
        OpenlistTaskState.CANCELED,
        OpenlistTaskState.ERRORED,
        OpenlistTaskState.FAILED,
    }
)
_SUPPORTED_COPY_MODES = frozenset({"copy", "aria2"})
_COPY_TEMP_ROOT_DIR = ".oani-copy-tmp"
_ARIA2_TOOL_NAME = "aria2"
_ARIA2_MIN_SPLIT_SIZE = "1M"
_DEFAULT_COPY_THREADS = 8
_MAX_COPY_THREADS = 16
_SAFE_TOKEN_RE = re.compile(r"[^A-Za-z0-9_-]+")


def normalize_openlist_path(path: str) -> str:
    """Normalize an absolute OpenList POSIX path without trailing slashes."""
    raw = str(path or "/").strip().replace("\\", "/")
    if not raw:
        return "/"
    if not raw.startswith("/"):
        raw = f"/{raw}"
    normalized = posixpath.normpath(raw)
    if normalized.startswith("//"):
        return "/" + normalized.lstrip("/")
    return normalized


def normalize_copy_target_paths(paths: Iterable[str]) -> tuple[str, ...]:
    """Return deduplicated, normalized OpenList destination root paths."""
    normalized: list[str] = []
    for raw in paths:
        stripped = str(raw or "").strip()
        if not stripped:
            continue
        candidate = normalize_openlist_path(stripped)
        if candidate not in normalized:
            normalized.append(candidate)
    return tuple(normalized)


def _relative_subdir(base_path: str, src_dir: str) -> str:
    """Extract the ``<anime_name>/Season <season>`` subdirectory under *base_path*."""
    norm_base = normalize_openlist_path(base_path)
    norm_src = normalize_openlist_path(src_dir)
    if norm_src == norm_base:
        return ""
    prefix = "/" if norm_base == "/" else f"{norm_base}/"
    if not norm_src.startswith(prefix):
        raise OrganizationError(
            f"Organized source directory '{src_dir}' escapes base path '{base_path}'"
        )
    return norm_src[len(prefix) :].strip("/")


def _make_staging_token(job_id: str, target_index: int, file_index: int) -> str:
    safe_job = _SAFE_TOKEN_RE.sub("-", str(job_id or "job")).strip("-")[:16] or "job"
    return f"{safe_job}-t{target_index}-f{file_index}-{uuid.uuid4().hex[:8]}"


def _resolve_raw_url(file_info: dict[str, Any] | None, base_url: str) -> str:
    if not isinstance(file_info, dict):
        return ""
    raw_url = str(file_info.get("raw_url") or "").strip()
    if not raw_url:
        return ""
    if raw_url.startswith("/") and base_url:
        return f"{base_url.rstrip('/')}{raw_url}"
    return raw_url


def _select_staged_file(
    entries: tuple[StorageEntry, ...],
    expected_name: str,
    expected_size: int,
) -> StorageEntry | None:
    files = [entry for entry in entries if not entry.is_directory]
    if not files:
        return None
    if len(files) == 1:
        return files[0]
    exact_name = next((entry for entry in files if entry.name == expected_name), None)
    if exact_name is not None:
        return exact_name
    if expected_size > 0:
        matching_size = [entry for entry in files if entry.size == expected_size]
        if len(matching_size) == 1:
            return matching_size[0]
    return None


class OpenListPostCopyExecutor:
    """Copy organized files to OpenList target folders and optionally clean source."""

    _CHECKPOINT_VERSION = 1
    _MAX_POLL_ERRORS = 3
    _TRANSFER_CHECK_MAX_RETRIES = 3

    def __init__(
        self,
        client: OpenListClient,
        storage: OpenListStorageOperations,
        *,
        copy_target_paths: Iterable[str],
        copy_mode: str = "copy",
        copy_threads: int = _DEFAULT_COPY_THREADS,
        aria2_rpc_url: str = "",
        aria2_rpc_secret: str = "",
        delete_source_after_copy: bool = True,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        poll_interval_seconds: float = 5.0,
        verify_retries: int = 3,
    ) -> None:
        normalized_mode = str(copy_mode or "copy").strip().lower()
        if normalized_mode not in _SUPPORTED_COPY_MODES:
            raise ValueError(
                f"Unsupported OpenList copy_mode '{copy_mode}'. "
                f"Supported modes: {', '.join(sorted(_SUPPORTED_COPY_MODES))}."
            )
        self._client = client
        self._storage = storage
        self._copy_target_paths = normalize_copy_target_paths(copy_target_paths)
        self._copy_mode = normalized_mode
        self._copy_threads = max(1, min(_MAX_COPY_THREADS, int(copy_threads)))
        self._aria2_rpc_url = str(aria2_rpc_url or "").strip()
        self._aria2_rpc_secret = str(aria2_rpc_secret or "")
        self._delete_source_after_copy = bool(delete_source_after_copy)
        self._sleep = sleep
        self._poll_interval_seconds = max(0.0, float(poll_interval_seconds))
        self._verify_retries = max(1, int(verify_retries))
        self._aria2_rpc_applied = False
        self._aria2_rpc_warned = False

    @property
    def enabled(self) -> bool:
        return bool(self._copy_target_paths)

    @property
    def target_paths(self) -> tuple[str, ...]:
        return self._copy_target_paths

    @property
    def copy_mode(self) -> str:
        return self._copy_mode

    @property
    def copy_threads(self) -> int:
        return self._copy_threads

    @property
    def delete_source_after_copy(self) -> bool:
        return self._delete_source_after_copy

    async def execute(
        self,
        job: DownloadJob,
        results: tuple[OrganizationResult, ...],
        *,
        saved_state: dict[str, Any] | None = None,
        save_state: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> tuple[OrganizationResult, ...]:
        if not self._copy_target_paths:
            return results

        base_path = normalize_openlist_path(str(job.artifact.get("base_path") or "/"))
        state = self._load_or_init_state(saved_state)

        async def persist() -> None:
            if save_state is not None:
                await save_state(_copy_state(state))

        updated_results: list[OrganizationResult] = []
        for result in results:
            if result.state != "completed" or not result.final_path:
                updated_results.append(result)
                continue
            updated = await self._process_item(
                job_id=job.id,
                result=result,
                base_path=base_path,
                state=state,
                persist=persist,
            )
            updated_results.append(updated)

        return tuple(updated_results)

    def _load_or_init_state(self, saved_state: dict[str, Any] | None) -> dict[str, Any]:
        if (
            isinstance(saved_state, dict)
            and saved_state.get("version") == self._CHECKPOINT_VERSION
            and isinstance(saved_state.get("items"), dict)
        ):
            return _copy_state(saved_state)
        return {"version": self._CHECKPOINT_VERSION, "items": {}}

    async def _process_item(
        self,
        *,
        job_id: str,
        result: OrganizationResult,
        base_path: str,
        state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> OrganizationResult:
        src_video = normalize_openlist_path(result.final_path or "")
        src_dir, video_name = posixpath.split(src_video)
        sidecar_names: list[str] = []
        for sidecar_path in result.sidecar_paths:
            norm_sidecar = normalize_openlist_path(sidecar_path)
            sidecar_dir, sidecar_name = posixpath.split(norm_sidecar)
            if sidecar_dir != src_dir:
                raise OrganizationError(
                    f"Sidecar directory '{sidecar_dir}' does not match video directory '{src_dir}'"
                )
            sidecar_names.append(sidecar_name)
        filenames = [video_name, *sidecar_names]
        relative_dir = _relative_subdir(base_path, src_dir)

        items = state["items"]
        item_state = items.get(result.item_key)
        if not isinstance(item_state, dict):
            item_state = await self._create_item_state(
                item_key=result.item_key,
                src_dir=src_dir,
                relative_dir=relative_dir,
                filenames=filenames,
            )
            items[result.item_key] = item_state
            await persist()

        if item_state.get("state") != "completed":
            for target_index, target_state in enumerate(item_state["targets"]):
                await self._process_target(
                    job_id=job_id,
                    target_index=target_index,
                    item_state=item_state,
                    target_state=target_state,
                    persist=persist,
                )

            all_completed = bool(item_state["targets"]) and all(
                target.get("state") == "completed" for target in item_state["targets"]
            )
            if not all_completed:
                raise OrganizationError(
                    f"Post-copy did not complete for all targets of {result.item_key}"
                )

            if self._delete_source_after_copy and not item_state.get("source_deleted"):
                await self._storage.remove_files(
                    str(item_state["src_dir"]),
                    tuple(item_state["filenames"]),
                )
                item_state["source_deleted"] = True
                logger.info(
                    f"Deleted {len(filenames)} source file(s) from {src_dir} "
                    "after OpenList copy succeeded"
                )

            item_state["state"] = "completed"
            await persist()

        if not self._delete_source_after_copy:
            return result

        primary_dst_dir = str(item_state["targets"][0]["dst_dir"])
        return replace(
            result,
            final_path=_join_openlist_path(primary_dst_dir, video_name),
            sidecar_paths=tuple(
                _join_openlist_path(primary_dst_dir, name) for name in sidecar_names
            ),
        )

    async def _create_item_state(
        self,
        *,
        item_key: str,
        src_dir: str,
        relative_dir: str,
        filenames: list[str],
    ) -> dict[str, Any]:
        expected_sizes = await self._capture_source_sizes(src_dir, filenames)
        targets = []
        for target_root in self._copy_target_paths:
            dst_dir = _join_openlist_path(target_root, relative_dir)
            if normalize_openlist_path(dst_dir) == normalize_openlist_path(src_dir):
                raise OrganizationError(
                    f"Refusing to copy onto the same OpenList directory: '{dst_dir}'"
                )
            targets.append(
                {
                    "target_root": target_root,
                    "dst_dir": dst_dir,
                    "state": "pending",
                    "task_ids": [],
                }
            )
        return {
            "item_key": item_key,
            "src_dir": src_dir,
            "filenames": list(filenames),
            "expected_sizes": expected_sizes,
            "targets": targets,
            "source_deleted": False,
            "state": "pending",
        }

    async def _capture_source_sizes(
        self, src_dir: str, filenames: list[str]
    ) -> dict[str, int]:
        entries = await self._storage.list_directory(src_dir)
        by_name = {entry.name: entry for entry in entries if not entry.is_directory}
        sizes: dict[str, int] = {}
        for name in filenames:
            entry = by_name.get(name)
            if entry is None:
                raise OrganizationError(
                    f"Source file missing before OpenList copy: {src_dir}/{name}"
                )
            sizes[name] = max(0, int(entry.size))
        return sizes

    async def _process_target(
        self,
        *,
        job_id: str,
        target_index: int,
        item_state: dict[str, Any],
        target_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        if target_state.get("state") == "completed":
            return

        src_dir = str(item_state["src_dir"])
        target_root = str(target_state["target_root"])
        dst_dir = str(target_state["dst_dir"])
        filenames = [str(name) for name in item_state["filenames"]]
        expected_sizes = {
            str(k): int(v) for k, v in item_state.get("expected_sizes", {}).items()
        }

        if normalize_openlist_path(src_dir) == normalize_openlist_path(dst_dir):
            raise OrganizationError(
                f"Refusing to copy onto the same OpenList directory: '{dst_dir}'"
            )

        if self._copy_mode == "aria2":
            await self._process_aria2_target(
                job_id=job_id,
                target_index=target_index,
                src_dir=src_dir,
                target_root=target_root,
                dst_dir=dst_dir,
                filenames=filenames,
                expected_sizes=expected_sizes,
                target_state=target_state,
                persist=persist,
            )
            return

        if target_state.get("state") == "pending":
            await self._submit_copy(
                src_dir=src_dir,
                target_root=target_root,
                dst_dir=dst_dir,
                filenames=filenames,
                expected_sizes=expected_sizes,
                target_state=target_state,
                persist=persist,
            )
            if target_state.get("state") == "completed":
                return

        if target_state.get("state") == "submitted":
            task_ids = [str(tid) for tid in target_state.get("task_ids", []) if tid]
            if task_ids:
                await self._wait_for_copy_tasks(
                    task_ids=task_ids,
                    src_dir=src_dir,
                    dst_dir=dst_dir,
                    target_state=target_state,
                    persist=persist,
                )
            await self._verify_target_files(
                dst_dir=dst_dir,
                filenames=filenames,
                expected_sizes=expected_sizes,
                target_state=target_state,
                persist=persist,
            )
            target_state["state"] = "completed"
            target_state["task_ids"] = []
            await persist()
            logger.info(
                f"OpenList copy completed: {src_dir} -> {dst_dir} "
                f"({len(filenames)} file(s))"
            )

    async def _process_aria2_target(
        self,
        *,
        job_id: str,
        target_index: int,
        src_dir: str,
        target_root: str,
        dst_dir: str,
        filenames: list[str],
        expected_sizes: dict[str, int],
        target_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        await self._ensure_target_directory(target_root, dst_dir)
        dst_entries = await self._storage.list_directory(dst_dir)
        by_name = {entry.name: entry for entry in dst_entries}

        files_state = target_state.get("files")
        if not isinstance(files_state, dict):
            files_state = {}
            target_state["files"] = files_state

        changed = False
        for name in filenames:
            entry = by_name.get(name)
            if entry is not None and entry.is_directory:
                raise OrganizationError(
                    f"Cannot copy '{name}' to '{dst_dir}': a directory with the same name exists"
                )
            if isinstance(files_state.get(name), dict):
                continue
            expected = expected_sizes.get(name, 0)
            if entry is not None and (expected <= 0 or entry.size == expected):
                files_state[name] = {"state": "completed"}
            else:
                files_state[name] = {"state": "pending"}
            changed = True

        if changed:
            await persist()

        pending_submission = any(
            files_state[name].get("state") in {"pending", "submitting"}
            for name in filenames
        )
        if pending_submission:
            await self._configure_aria2_threads()

        if all(files_state[name].get("state") == "completed" for name in filenames):
            await self._verify_target_files(
                dst_dir=dst_dir,
                filenames=filenames,
                expected_sizes=expected_sizes,
                target_state=target_state,
                persist=persist,
            )
            target_state["state"] = "completed"
            target_state["task_ids"] = []
            await persist()
            logger.info(
                f"OpenList copy already up to date in {dst_dir} "
                f"({len(filenames)} file(s))"
            )
            return

        for file_index, filename in enumerate(filenames):
            file_state = files_state[filename]
            if file_state.get("state") == "completed":
                continue
            await self._process_aria2_file(
                job_id=job_id,
                target_index=target_index,
                file_index=file_index,
                src_dir=src_dir,
                target_root=target_root,
                dst_dir=dst_dir,
                filename=filename,
                expected_size=expected_sizes.get(filename, 0),
                file_state=file_state,
                persist=persist,
            )

        await self._verify_target_files(
            dst_dir=dst_dir,
            filenames=filenames,
            expected_sizes=expected_sizes,
            target_state=target_state,
            persist=persist,
        )
        target_state["state"] = "completed"
        target_state["task_ids"] = []
        await persist()
        logger.info(
            f"OpenList aria2 copy completed ({self._copy_threads} threads): "
            f"{src_dir} -> {dst_dir} ({len(filenames)} file(s))"
        )

    async def _configure_aria2_threads(self) -> bool:
        rpc_url = self._aria2_rpc_url
        rpc_secret = self._aria2_rpc_secret

        if not rpc_url:
            settings = await self._client.get_admin_settings(
                ["aria2_uri", "aria2_secret"]
            )
            if settings:
                rpc_url = settings.get("aria2_uri", "").strip()
                if not rpc_secret:
                    rpc_secret = settings.get("aria2_secret", "")

        if not rpc_url:
            if not self._aria2_rpc_warned:
                self._aria2_rpc_warned = True
                logger.warning(
                    f"Could not discover Aria2 RPC URL from OpenList to apply "
                    f"copy_threads={self._copy_threads}; continuing with Aria2 defaults. "
                    f"Set split={self._copy_threads} and "
                    f"max-connection-per-server={self._copy_threads} in aria2.conf "
                    "or configure [downloader.openlist] aria2_rpc_url."
                )
            return False

        options = {
            "split": str(self._copy_threads),
            "max-connection-per-server": str(self._copy_threads),
            "min-split-size": _ARIA2_MIN_SPLIT_SIZE,
        }
        ok = await self._client.configure_aria2_options(
            rpc_url, options, secret=rpc_secret
        )
        if not ok and self._aria2_rpc_url and not rpc_secret:
            settings = await self._client.get_admin_settings(["aria2_secret"])
            discovered_secret = (settings or {}).get("aria2_secret", "")
            if discovered_secret:
                ok = await self._client.configure_aria2_options(
                    rpc_url, options, secret=discovered_secret
                )
        if ok:
            if not self._aria2_rpc_applied:
                self._aria2_rpc_applied = True
                self._aria2_rpc_warned = False
                logger.info(
                    f"Configured Aria2 global options for copy: "
                    f"split={self._copy_threads}, "
                    f"max-connection-per-server={self._copy_threads}, "
                    f"min-split-size={_ARIA2_MIN_SPLIT_SIZE}"
                )
            else:
                logger.debug(
                    f"Refreshed Aria2 global options: split={self._copy_threads}, "
                    f"max-connection-per-server={self._copy_threads}"
                )
            return True

        self._aria2_rpc_applied = False
        if not self._aria2_rpc_warned:
            self._aria2_rpc_warned = True
            logger.warning(
                f"Could not apply Aria2 copy_threads={self._copy_threads} via RPC "
                f"({rpc_url}); continuing with OpenList Aria2 defaults. To enforce "
                f"{self._copy_threads}-thread downloads, set split={self._copy_threads} "
                f"and max-connection-per-server={self._copy_threads} in aria2.conf "
                "or configure [downloader.openlist] aria2_rpc_url."
            )
        return False

    async def _process_aria2_file(
        self,
        *,
        job_id: str,
        target_index: int,
        file_index: int,
        src_dir: str,
        target_root: str,
        dst_dir: str,
        filename: str,
        expected_size: int,
        file_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        while file_state.get("state") != "completed":
            current = str(file_state.get("state") or "pending")
            if current == "pending":
                staging_token = _make_staging_token(job_id, target_index, file_index)
                staging_dir = _join_openlist_path(
                    target_root, _COPY_TEMP_ROOT_DIR, staging_token
                )
                await self._ensure_target_directory(target_root, staging_dir)
                file_state.clear()
                file_state.update(
                    {
                        "state": "submitting",
                        "staging_token": staging_token,
                        "staging_dir": staging_dir,
                        "task_id": "",
                    }
                )
                await persist()
                continue

            staging_token = str(file_state.get("staging_token") or "")
            staging_dir = str(file_state.get("staging_dir") or "")
            if not staging_token or not staging_dir:
                file_state.clear()
                file_state["state"] = "pending"
                await persist()
                continue

            if current == "submitting":
                await self._submit_aria2_file(
                    src_dir=src_dir,
                    target_root=target_root,
                    dst_dir=dst_dir,
                    filename=filename,
                    staging_token=staging_token,
                    staging_dir=staging_dir,
                    file_state=file_state,
                    persist=persist,
                )
                continue

            if current == "downloading":
                task_id = str(file_state.get("task_id") or "")
                if task_id:
                    await self._wait_for_aria2_download_task(
                        task_id=task_id,
                        src_dir=src_dir,
                        target_root=target_root,
                        dst_dir=dst_dir,
                        filename=filename,
                        staging_token=staging_token,
                        staging_dir=staging_dir,
                        file_state=file_state,
                        persist=persist,
                    )
                file_state["state"] = "transferring"
                await persist()
                continue

            if current == "transferring":
                await self._wait_for_aria2_transfer_task(
                    src_dir=src_dir,
                    target_root=target_root,
                    dst_dir=dst_dir,
                    filename=filename,
                    expected_size=expected_size,
                    staging_token=staging_token,
                    staging_dir=staging_dir,
                    file_state=file_state,
                    persist=persist,
                )
                file_state["state"] = "finalizing"
                await persist()
                continue

            if current == "finalizing":
                await self._finalize_aria2_file(
                    target_root=target_root,
                    dst_dir=dst_dir,
                    filename=filename,
                    expected_size=expected_size,
                    staging_token=staging_token,
                    staging_dir=staging_dir,
                    file_state=file_state,
                    persist=persist,
                )
                file_state["state"] = "completed"
                file_state["task_id"] = ""
                await persist()
                return

            file_state.clear()
            file_state["state"] = "pending"
            await persist()

    async def _submit_aria2_file(
        self,
        *,
        src_dir: str,
        target_root: str,
        dst_dir: str,
        filename: str,
        staging_token: str,
        staging_dir: str,
        file_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        existing = await self._find_existing_aria2_download_task(
            staging_token, staging_dir
        )
        if existing is not None:
            logger.info(
                f"Resuming existing OpenList aria2 copy task for '{filename}' -> "
                f"{dst_dir}: {existing.id}"
            )
            file_state["task_id"] = existing.id
            file_state["state"] = "downloading"
            await persist()
            return

        src_path = _join_openlist_path(src_dir, filename)
        file_info = await self._client.get_file_info(src_path)
        raw_url = _resolve_raw_url(file_info, getattr(self._client, "base_url", ""))
        if not raw_url:
            await self._reset_aria2_file_state(
                target_root=target_root,
                staging_token=staging_token,
                file_state=file_state,
                persist=persist,
            )
            raise OrganizationError(
                f"Failed to retrieve raw_url from OpenList for '{src_path}'"
            )

        logger.info(
            f"OpenList aria2 copy started ({self._copy_threads} threads): "
            f"{src_path} -> {dst_dir}"
        )
        tasks = await self._client.add_offline_download(
            urls=[raw_url],
            path=staging_dir,
            tool=_ARIA2_TOOL_NAME,
            delete_policy="delete_always",
        )
        if not tasks or not tasks[0].id:
            await self._reset_aria2_file_state(
                target_root=target_root,
                staging_token=staging_token,
                file_state=file_state,
                persist=persist,
            )
            raise OrganizationError(
                f"Failed to start OpenList aria2 copy for '{src_path}' -> '{dst_dir}'"
            )

        file_state["task_id"] = tasks[0].id
        file_state["state"] = "downloading"
        await persist()

    async def _find_existing_aria2_download_task(
        self, staging_token: str, staging_dir: str
    ) -> OpenlistTask | None:
        undone = await self._client.get_offline_download_undone()
        done = await self._client.get_offline_download_done()
        if undone is None or done is None:
            raise OrganizationError(
                f"Cannot safely reconcile uncertain OpenList aria2 copy submission for '{staging_dir}'"
            )
        for task in (*undone, *done):
            if not task.id:
                continue
            task_text = f"{task.name or ''} {task.status or ''}"
            if staging_token in task_text or staging_dir in task_text:
                return task
        return None

    async def _wait_for_aria2_download_task(
        self,
        *,
        task_id: str,
        src_dir: str,
        target_root: str,
        dst_dir: str,
        filename: str,
        staging_token: str,
        staging_dir: str,
        file_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        consecutive_poll_errors = 0
        logged_milestones: dict[str, int] = {}
        while True:
            undone = await self._client.get_offline_download_undone()
            if undone is None:
                consecutive_poll_errors += 1
                if consecutive_poll_errors >= self._MAX_POLL_ERRORS:
                    raise OrganizationError(
                        f"Failed to poll OpenList undone aria2 download tasks for {dst_dir}"
                    )
                await self._sleep(self._poll_interval_seconds)
                continue

            matching = next((task for task in undone if task.id == task_id), None)
            if matching is not None:
                consecutive_poll_errors = 0
                self._log_task_progress(
                    matching, dst_dir, logged_milestones, label="aria2 download"
                )
                if self._is_complete_progress(
                    matching.progress
                ) and await self._aria2_transfer_task_exists(
                    staging_token, staging_dir
                ):
                    logger.debug(
                        f"OpenList aria2 download task {task_id} is at 100% and "
                        "transfer task exists; advancing to transfer"
                    )
                    return
                await self._sleep(self._poll_interval_seconds)
                continue

            done = await self._client.get_offline_download_done()
            if done is None:
                consecutive_poll_errors += 1
                if consecutive_poll_errors >= self._MAX_POLL_ERRORS:
                    raise OrganizationError(
                        f"Failed to poll OpenList done aria2 download tasks for {dst_dir}"
                    )
                await self._sleep(self._poll_interval_seconds)
                continue

            consecutive_poll_errors = 0
            finished = next((task for task in done if task.id == task_id), None)
            if finished is None:
                logger.warning(
                    f"OpenList aria2 download task {task_id} not found in undone/done "
                    f"lists; checking transfer/staging for '{filename}'"
                )
                return

            if finished.state == OpenlistTaskState.SUCCEEDED:
                return

            self._log_terminal_copy_failure(finished, src_dir, dst_dir)
            await self._reset_aria2_file_state(
                target_root=target_root,
                staging_token=staging_token,
                file_state=file_state,
                persist=persist,
            )
            details = finished.error or finished.status or finished.state
            raise OrganizationError(
                f"OpenList aria2 download task {task_id} failed for '{filename}' "
                f"({src_dir} -> {dst_dir}): {details}"
            )

    async def _aria2_transfer_task_exists(
        self, staging_token: str, staging_dir: str
    ) -> bool:
        undone = await self._client.get_offline_download_transfer_undone()
        if undone and self._match_transfer_task(undone, staging_token, staging_dir):
            return True
        done = await self._client.get_offline_download_transfer_done()
        return bool(
            done and self._match_transfer_task(done, staging_token, staging_dir)
        )

    @staticmethod
    def _match_transfer_task(
        tasks: list[OpenlistTask], staging_token: str, staging_dir: str
    ) -> OpenlistTask | None:
        for task in tasks:
            name = task.name or ""
            if staging_token in name or staging_dir in name:
                return task
        return None

    async def _wait_for_aria2_transfer_task(
        self,
        *,
        src_dir: str,
        target_root: str,
        dst_dir: str,
        filename: str,
        expected_size: int,
        staging_token: str,
        staging_dir: str,
        file_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        consecutive_poll_errors = 0
        not_found_count = 0
        logged_milestones: dict[str, int] = {}

        while True:
            undone = await self._client.get_offline_download_transfer_undone()
            if undone is None:
                consecutive_poll_errors += 1
                if consecutive_poll_errors >= self._MAX_POLL_ERRORS:
                    raise OrganizationError(
                        f"Failed to poll OpenList undone aria2 transfer tasks for {dst_dir}"
                    )
                await self._sleep(self._poll_interval_seconds)
                continue

            matching_undone = self._match_transfer_task(
                undone, staging_token, staging_dir
            )
            if matching_undone is not None:
                consecutive_poll_errors = 0
                not_found_count = 0
                self._log_task_progress(
                    matching_undone,
                    dst_dir,
                    logged_milestones,
                    label="aria2 transfer",
                )
                await self._sleep(self._poll_interval_seconds)
                continue

            done = await self._client.get_offline_download_transfer_done()
            if done is None:
                consecutive_poll_errors += 1
                if consecutive_poll_errors >= self._MAX_POLL_ERRORS:
                    raise OrganizationError(
                        f"Failed to poll OpenList done aria2 transfer tasks for {dst_dir}"
                    )
                await self._sleep(self._poll_interval_seconds)
                continue

            consecutive_poll_errors = 0
            matching_done = self._match_transfer_task(done, staging_token, staging_dir)
            if matching_done is not None:
                if matching_done.state == OpenlistTaskState.SUCCEEDED:
                    return
                self._log_terminal_copy_failure(matching_done, src_dir, dst_dir)
                await self._reset_aria2_file_state(
                    target_root=target_root,
                    staging_token=staging_token,
                    file_state=file_state,
                    persist=persist,
                )
                details = (
                    matching_done.error or matching_done.status or matching_done.state
                )
                raise OrganizationError(
                    f"OpenList aria2 transfer task {matching_done.id} failed for "
                    f"'{filename}' ({src_dir} -> {dst_dir}): {details}"
                )

            staged_entries = await self._client.list_files(staging_dir)
            if staged_entries:
                storage_entries = tuple(
                    StorageEntry(
                        name=entry.name,
                        is_directory=bool(getattr(entry, "is_dir", False)),
                        size=(
                            entry.size
                            if isinstance(getattr(entry, "size", None), int)
                            else 0
                        ),
                    )
                    for entry in staged_entries
                )
                staged = _select_staged_file(storage_entries, filename, expected_size)
                if staged is not None and (
                    expected_size <= 0 or staged.size == expected_size
                ):
                    return

            not_found_count += 1
            if not_found_count >= self._TRANSFER_CHECK_MAX_RETRIES:
                logger.debug(
                    f"No transfer task found for {staging_token} after "
                    f"{self._TRANSFER_CHECK_MAX_RETRIES} checks; verifying staging directory"
                )
                return

            await self._sleep(self._poll_interval_seconds)

    async def _finalize_aria2_file(
        self,
        *,
        target_root: str,
        dst_dir: str,
        filename: str,
        expected_size: int,
        staging_token: str,
        staging_dir: str,
        file_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        dst_entries = await self._storage.list_directory(dst_dir)
        existing_dst = next(
            (entry for entry in dst_entries if entry.name == filename), None
        )
        if existing_dst is not None and existing_dst.is_directory:
            raise OrganizationError(
                f"Cannot copy '{filename}' to '{dst_dir}': a directory with the same name exists"
            )
        if existing_dst is not None and (
            expected_size <= 0 or existing_dst.size == expected_size
        ):
            await self._safe_cleanup_staging_dir(target_root, staging_token)
            return

        staged: StorageEntry | None = None
        last_problem = f"no downloaded file found in '{staging_dir}'"
        for attempt in range(self._verify_retries):
            staging_entries = await self._storage.list_directory(staging_dir)
            candidate = _select_staged_file(staging_entries, filename, expected_size)
            if candidate is None:
                last_problem = f"no downloaded file found in '{staging_dir}'"
            elif expected_size > 0 and candidate.size != expected_size:
                last_problem = (
                    f"size mismatch for '{filename}' "
                    f"(expected {expected_size}, got {candidate.size})"
                )
            else:
                staged = candidate
                break
            if attempt + 1 < self._verify_retries:
                await self._storage._refresh_wait()

        if staged is None:
            await self._reset_aria2_file_state(
                target_root=target_root,
                staging_token=staging_token,
                file_state=file_state,
                persist=persist,
            )
            raise OrganizationError(
                f"OpenList aria2 copy verification failed for '{filename}': {last_problem}"
            )

        if staged.name != filename:
            logger.debug(
                f"Renaming aria2 copied file '{staged.name}' -> '{filename}' "
                f"in {staging_dir}"
            )
            await self._storage.rename(
                _join_openlist_path(staging_dir, staged.name),
                filename,
            )

        if existing_dst is not None:
            await self._storage.remove_files(dst_dir, (filename,))

        await self._storage.move(staging_dir, dst_dir, (filename,))
        await self._safe_cleanup_staging_dir(target_root, staging_token)

    async def _reset_aria2_file_state(
        self,
        *,
        target_root: str,
        staging_token: str,
        file_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        await self._safe_cleanup_staging_dir(target_root, staging_token)
        file_state.clear()
        file_state["state"] = "pending"
        await persist()

    async def _safe_cleanup_staging_dir(
        self, target_root: str, staging_token: str
    ) -> None:
        if (
            not staging_token
            or "/" in staging_token
            or "\\" in staging_token
            or staging_token in {".", ".."}
        ):
            return
        temp_root = _join_openlist_path(target_root, _COPY_TEMP_ROOT_DIR)
        try:
            entries = await self._client.list_files(temp_root)
            if not entries or not any(entry.name == staging_token for entry in entries):
                return
            removed = await self._client.remove_path(temp_root, [staging_token])
            if not removed:
                logger.warning(
                    f"Failed to remove aria2 copy staging directory '{temp_root}/{staging_token}'"
                )
        except Exception as exc:
            logger.warning(
                f"Failed to clean up aria2 copy staging directory '{temp_root}/{staging_token}': {exc}"
            )

    async def _submit_copy(
        self,
        *,
        src_dir: str,
        target_root: str,
        dst_dir: str,
        filenames: list[str],
        expected_sizes: dict[str, int],
        target_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        await self._ensure_target_directory(target_root, dst_dir)
        dst_entries = await self._storage.list_directory(dst_dir)
        by_name = {entry.name: entry for entry in dst_entries}

        missing_names: list[str] = []
        needs_overwrite = False
        for name in filenames:
            entry = by_name.get(name)
            if entry is not None and entry.is_directory:
                raise OrganizationError(
                    f"Cannot copy '{name}' to '{dst_dir}': a directory with the same name exists"
                )
            expected = expected_sizes.get(name, 0)
            if entry is not None and (expected <= 0 or entry.size == expected):
                continue
            missing_names.append(name)
            if entry is not None:
                needs_overwrite = True

        if not missing_names:
            undone_ids = await self._find_matching_undone_task_ids(
                src_dir, dst_dir, filenames
            )
            if undone_ids:
                target_state["task_ids"] = undone_ids
                target_state["state"] = "submitted"
                await persist()
                return
            target_state["state"] = "completed"
            target_state["task_ids"] = []
            await persist()
            logger.info(
                f"OpenList copy already up to date in {dst_dir} "
                f"({len(filenames)} file(s))"
            )
            return

        # If a previous attempt crashed between POST /api/fs/copy and checkpoint
        # persistence, attach to the already-running OpenList task instead of
        # starting a duplicate copy.
        existing_undone_ids = await self._find_matching_undone_task_ids(
            src_dir, dst_dir, missing_names
        )
        if existing_undone_ids:
            logger.info(
                f"Resuming existing OpenList copy task(s) for {dst_dir}: "
                f"{existing_undone_ids}"
            )
            target_state["task_ids"] = existing_undone_ids
            target_state["state"] = "submitted"
            await persist()
            return

        logger.info(
            f"OpenList copy started: {src_dir} -> {dst_dir} "
            f"({len(missing_names)} file(s))"
        )
        ok, tasks = await self._client.copy_file(
            src_dir,
            dst_dir,
            missing_names,
            overwrite=needs_overwrite,
        )
        if not ok:
            raise OrganizationError(
                f"Failed to start OpenList copy from '{src_dir}' to '{dst_dir}'"
            )

        task_ids = [task.id for task in tasks if task.id]
        if not task_ids:
            task_ids = await self._find_matching_undone_task_ids(
                src_dir, dst_dir, missing_names
            )
        target_state["task_ids"] = task_ids
        target_state["state"] = "submitted"
        await persist()

    async def _ensure_target_directory(self, target_root: str, dst_dir: str) -> None:
        ancestor = normalize_openlist_path(target_root)
        while ancestor != "/":
            if await self._client.list_files(ancestor) is not None:
                break
            ancestor = normalize_openlist_path(posixpath.dirname(ancestor))
        await self._storage.ensure_directory(ancestor, dst_dir)

    async def _find_matching_undone_task_ids(
        self,
        src_dir: str,
        dst_dir: str,
        filenames: list[str],
    ) -> list[str]:
        undone = await self._client.get_copy_tasks_undone()
        if not undone:
            return []
        matched: list[str] = []
        dst_leaf = posixpath.basename(dst_dir.rstrip("/"))
        for task in undone:
            task_text = f"{task.name or ''} {task.status or ''}"
            if not task.id:
                continue
            if any(name in task_text for name in filenames) and (
                dst_dir in task_text
                or (dst_leaf and dst_leaf in task_text)
                or src_dir in task_text
            ):
                matched.append(task.id)
        return list(dict.fromkeys(matched))

    async def _wait_for_copy_tasks(
        self,
        *,
        task_ids: list[str],
        src_dir: str,
        dst_dir: str,
        target_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        remaining = set(task_ids)
        consecutive_poll_errors = 0
        logged_milestones: dict[str, int] = {}
        while remaining:
            undone = await self._client.get_copy_tasks_undone()
            if undone is None:
                consecutive_poll_errors += 1
                if consecutive_poll_errors >= self._MAX_POLL_ERRORS:
                    raise OrganizationError(
                        f"Failed to poll OpenList undone copy tasks for {dst_dir}"
                    )
                await self._sleep(self._poll_interval_seconds)
                continue

            undone_by_id = {task.id: task for task in undone if task.id}
            still_running = [tid for tid in remaining if tid in undone_by_id]
            for tid in still_running:
                self._log_task_progress(undone_by_id[tid], dst_dir, logged_milestones)
            settled_candidates = remaining - set(still_running)

            if settled_candidates:
                done = await self._client.get_copy_tasks_done()
                if done is None:
                    consecutive_poll_errors += 1
                    if consecutive_poll_errors >= self._MAX_POLL_ERRORS:
                        raise OrganizationError(
                            f"Failed to poll OpenList done copy tasks for {dst_dir}"
                        )
                    await self._sleep(self._poll_interval_seconds)
                    continue
                consecutive_poll_errors = 0
                done_by_id = {task.id: task for task in done if task.id}
                settled_any = False
                for tid in list(settled_candidates):
                    finished = done_by_id.get(tid)
                    if finished is None:
                        # Task vanished from both lists (e.g. cleared or server restarted);
                        # target file verification below will validate whether it succeeded.
                        logger.warning(
                            f"OpenList copy task {tid} not found in undone/done lists; "
                            f"verifying target directory {dst_dir}"
                        )
                        remaining.discard(tid)
                        settled_any = True
                        continue
                    self._log_terminal_copy_failure(finished, src_dir, dst_dir)
                    if finished.state == OpenlistTaskState.SUCCEEDED:
                        remaining.discard(tid)
                        settled_any = True
                    else:
                        target_state["state"] = "pending"
                        target_state["task_ids"] = []
                        await persist()
                        details = finished.error or finished.status or finished.state
                        raise OrganizationError(
                            f"OpenList copy task {tid} failed ({src_dir} -> {dst_dir}): "
                            f"{details}"
                        )
                if settled_any and remaining:
                    target_state["task_ids"] = sorted(remaining)
                    await persist()
            else:
                consecutive_poll_errors = 0

            if remaining:
                await self._sleep(self._poll_interval_seconds)

    @staticmethod
    def _is_complete_progress(progress: Any) -> bool:
        if progress is None:
            return False
        try:
            return float(progress) >= 100.0
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _log_task_progress(
        task: OpenlistTask,
        dst_dir: str,
        logged_milestones: dict[str, int],
        *,
        label: str = "copy",
    ) -> None:
        if task.progress is None:
            return
        try:
            progress = float(task.progress)
        except (TypeError, ValueError):
            return
        milestone = int(progress // 25) * 25
        if milestone >= 25 and milestone > logged_milestones.get(task.id, 0):
            logged_milestones[task.id] = milestone
            logger.info(
                f"OpenList {label} in progress ({progress:.0f}%): "
                f"task={task.id}, dst={dst_dir}"
            )

    @staticmethod
    def _log_terminal_copy_failure(
        finished: OpenlistTask,
        src_dir: str,
        dst_dir: str,
    ) -> None:
        if finished.state in _TERMINAL_FAILURE_STATES:
            logger.warning(
                f"OpenList copy task {finished.id} ended in {finished.state} "
                f"for {src_dir} -> {dst_dir}: {finished.error or finished.status}"
            )

    async def _verify_target_files(
        self,
        *,
        dst_dir: str,
        filenames: list[str],
        expected_sizes: dict[str, int],
        target_state: dict[str, Any],
        persist: Callable[[], Awaitable[None]],
    ) -> None:
        last_problem = ""
        for attempt in range(self._verify_retries):
            entries = await self._storage.list_directory(dst_dir)
            last_problem = _check_entries_complete(entries, filenames, expected_sizes)
            if not last_problem:
                return
            if attempt + 1 < self._verify_retries:
                await self._storage._refresh_wait()

        target_state["state"] = "pending"
        target_state["task_ids"] = []
        if "files" in target_state:
            target_state["files"] = {}
        await persist()
        raise OrganizationError(
            f"OpenList copy verification failed for '{dst_dir}': {last_problem}"
        )


def _check_entries_complete(
    entries: tuple[StorageEntry, ...],
    filenames: list[str],
    expected_sizes: dict[str, int],
) -> str:
    by_name = {entry.name: entry for entry in entries}
    for name in filenames:
        entry = by_name.get(name)
        if entry is None or entry.is_directory:
            return f"missing file '{name}'"
        expected = expected_sizes.get(name, 0)
        if expected > 0 and entry.size != expected:
            return f"size mismatch for '{name}' (expected {expected}, got {entry.size})"
    return ""


def _copy_target_state(target: dict[str, Any]) -> dict[str, Any]:
    copied = {
        **target,
        "task_ids": list(target.get("task_ids", [])),
    }
    if isinstance(target.get("files"), dict):
        copied["files"] = {
            str(name): (dict(fstate) if isinstance(fstate, dict) else fstate)
            for name, fstate in target["files"].items()
        }
    return copied


def _copy_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": state.get("version", 1),
        "items": {
            key: {
                **item,
                "filenames": list(item.get("filenames", [])),
                "expected_sizes": dict(item.get("expected_sizes", {})),
                "targets": [
                    _copy_target_state(target) for target in item.get("targets", [])
                ],
            }
            for key, item in state.get("items", {}).items()
        },
    }
