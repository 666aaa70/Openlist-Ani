"""Durable post-organization copy (and optional source deletion) for OpenList."""

from __future__ import annotations

import asyncio
import posixpath
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


class OpenListPostCopyExecutor:
    """Copy organized files to OpenList target folders and optionally clean source."""

    _CHECKPOINT_VERSION = 1
    _MAX_POLL_ERRORS = 3

    def __init__(
        self,
        client: OpenListClient,
        storage: OpenListStorageOperations,
        *,
        copy_target_paths: Iterable[str],
        delete_source_after_copy: bool = True,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        poll_interval_seconds: float = 5.0,
        verify_retries: int = 3,
    ) -> None:
        self._client = client
        self._storage = storage
        self._copy_target_paths = normalize_copy_target_paths(copy_target_paths)
        self._delete_source_after_copy = bool(delete_source_after_copy)
        self._sleep = sleep
        self._poll_interval_seconds = max(0.0, float(poll_interval_seconds))
        self._verify_retries = max(1, int(verify_retries))

    @property
    def enabled(self) -> bool:
        return bool(self._copy_target_paths)

    @property
    def target_paths(self) -> tuple[str, ...]:
        return self._copy_target_paths

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
            for target_state in item_state["targets"]:
                await self._process_target(
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
                self._log_copy_progress(undone_by_id[tid], dst_dir, logged_milestones)
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
    def _log_copy_progress(
        task: OpenlistTask, dst_dir: str, logged_milestones: dict[str, int]
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
                f"OpenList copy in progress ({progress:.0f}%): "
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


def _copy_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": state.get("version", 1),
        "items": {
            key: {
                **item,
                "filenames": list(item.get("filenames", [])),
                "expected_sizes": dict(item.get("expected_sizes", {})),
                "targets": [
                    {
                        **target,
                        "task_ids": list(target.get("task_ids", [])),
                    }
                    for target in item.get("targets", [])
                ],
            }
            for key, item in state.get("items", {}).items()
        },
    }
