"""Tests for OpenList post-organization copy and source cleanup."""

from __future__ import annotations

import posixpath
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openlist_ani.adapters.download_backends.openlist import (
    OpenListClient,
    OpenListOrganizerAdapter,
    OpenlistTask,
    OpenlistTaskState,
)
from openlist_ani.adapters.download_backends.openlist.post_copy import (
    OpenListPostCopyExecutor,
)
from openlist_ani.adapters.download_backends.openlist.storage import (
    OpenListStorageOperations,
)
from openlist_ani.application.organization import OrganizationError
from openlist_ani.application.ports import (
    DownloadedFile,
    DownloadManifest,
    OrganizationRequest,
    OrganizationResult,
    OrganizationSidecar,
)
from openlist_ani.domain import (
    DownloadJob,
    MetadataDocument,
    ReleaseCandidate,
    ReleaseMetadata,
)


class _InMemoryCopyClient:
    """In-memory OpenList filesystem + async/sync copy task simulator."""

    def __init__(
        self,
        *,
        async_copy: bool = True,
        return_tasks_on_copy: bool = True,
        polls_before_done: int = 1,
        fail_targets: set[str] | None = None,
        corrupt_size_targets: set[str] | None = None,
    ) -> None:
        self.async_copy = async_copy
        self.return_tasks_on_copy = return_tasks_on_copy
        self.polls_before_done = polls_before_done
        self.fail_targets = set(fail_targets or ())
        self.corrupt_size_targets = set(corrupt_size_targets or ())
        self.dirs: dict[str, dict[str, SimpleNamespace]] = {"/": {}}
        self.copy_calls: list[dict[str, object]] = []
        self.remove_calls: list[tuple[str, list[str]]] = []
        self._task_seq = 0
        self._pending_tasks: dict[str, dict[str, object]] = {}
        self._done_tasks: list[OpenlistTask] = []

    def put_file(self, directory: str, name: str, size: int = 1000) -> None:
        entries = self._ensure_dir(directory)
        entries[name] = SimpleNamespace(name=name, is_dir=False, size=size)

    def _ensure_dir(self, path: str) -> dict[str, SimpleNamespace]:
        normalized = posixpath.normpath(path)
        parts = [part for part in normalized.split("/") if part]
        current = "/"
        self.dirs.setdefault("/", {})
        for part in parts:
            parent = current
            current = posixpath.join(current, part)
            self.dirs[parent].setdefault(
                part, SimpleNamespace(name=part, is_dir=True, size=0)
            )
            self.dirs.setdefault(current, {})
        return self.dirs[current]

    async def list_files(self, path: str) -> list[SimpleNamespace] | None:
        normalized = posixpath.normpath(path)
        entries = self.dirs.get(normalized)
        if entries is None:
            return None
        return list(entries.values())

    async def mkdir(self, path: str) -> bool:
        self._ensure_dir(path)
        return True

    async def rename_file(self, full_path: str, new_name: str) -> bool:
        normalized = posixpath.normpath(full_path)
        parent, old_name = posixpath.split(normalized)
        entries = self.dirs.get(parent or "/")
        if entries is None or old_name not in entries:
            return False
        entry = entries.pop(old_name)
        entries[new_name] = SimpleNamespace(
            name=new_name, is_dir=entry.is_dir, size=entry.size
        )
        return True

    async def move_file(self, src_dir: str, dst_dir: str, filenames: list[str]) -> bool:
        src = self.dirs.get(posixpath.normpath(src_dir), {})
        dst = self._ensure_dir(dst_dir)
        for name in filenames:
            if name not in src:
                return False
            dst[name] = src.pop(name)
        return True

    async def copy_file(
        self,
        src_dir: str,
        dst_dir: str,
        filenames: list[str],
        *,
        overwrite: bool = False,
    ) -> tuple[bool, list[OpenlistTask]]:
        norm_src = posixpath.normpath(src_dir)
        norm_dst = posixpath.normpath(dst_dir)
        self.copy_calls.append(
            {
                "src_dir": norm_src,
                "dst_dir": norm_dst,
                "filenames": list(filenames),
                "overwrite": overwrite,
            }
        )
        src = self.dirs.get(norm_src, {})
        dst = self.dirs.get(norm_dst)
        if dst is None:
            return False, []
        for name in filenames:
            if name not in src:
                return False, []
            if name in dst and not overwrite:
                return False, []

        if not self.async_copy:
            self._materialize_copy(norm_src, norm_dst, filenames)
            return True, []

        tasks: list[OpenlistTask] = []
        for name in filenames:
            self._task_seq += 1
            tid = f"copy-{self._task_seq}"
            task_name = f"copy [{norm_src}/{name}] to [{norm_dst}]"
            self._pending_tasks[tid] = {
                "task": OpenlistTask(
                    id=tid,
                    name=task_name,
                    state=OpenlistTaskState.RUNNING,
                    progress=50,
                ),
                "src_dir": norm_src,
                "dst_dir": norm_dst,
                "name": name,
                "remaining_polls": self.polls_before_done,
            }
            tasks.append(
                OpenlistTask(
                    id=tid,
                    name=task_name,
                    state=OpenlistTaskState.PENDING,
                    progress=0,
                )
            )
        return True, (tasks if self.return_tasks_on_copy else [])

    def _materialize_copy(
        self, norm_src: str, norm_dst: str, filenames: list[str]
    ) -> None:
        src = self.dirs.get(norm_src, {})
        dst = self._ensure_dir(norm_dst)
        for name in filenames:
            entry = src[name]
            size = (
                entry.size - 1
                if any(
                    norm_dst.startswith(prefix) for prefix in self.corrupt_size_targets
                )
                else entry.size
            )
            dst[name] = SimpleNamespace(name=name, is_dir=False, size=size)

    async def get_copy_tasks_undone(self) -> list[OpenlistTask]:
        still_running: list[OpenlistTask] = []
        for tid in list(self._pending_tasks):
            record = self._pending_tasks[tid]
            remaining = int(record["remaining_polls"])
            if remaining > 0:
                record["remaining_polls"] = remaining - 1
                still_running.append(record["task"])
                continue
            self._pending_tasks.pop(tid)
            norm_src = str(record["src_dir"])
            norm_dst = str(record["dst_dir"])
            name = str(record["name"])
            should_fail = any(
                norm_dst.startswith(prefix) for prefix in self.fail_targets
            )
            if should_fail:
                self._done_tasks.append(
                    OpenlistTask(
                        id=tid,
                        name=record["task"].name,
                        state=OpenlistTaskState.FAILED,
                        error="remote quota exceeded",
                    )
                )
            else:
                self._materialize_copy(norm_src, norm_dst, [name])
                self._done_tasks.append(
                    OpenlistTask(
                        id=tid,
                        name=record["task"].name,
                        state=OpenlistTaskState.SUCCEEDED,
                        progress=100,
                    )
                )
        return still_running

    async def get_copy_tasks_done(self) -> list[OpenlistTask]:
        return list(self._done_tasks)

    async def remove_path(self, directory: str, names: list[str]) -> bool:
        norm_dir = posixpath.normpath(directory)
        self.remove_calls.append((norm_dir, list(names)))
        parent = self.dirs.get(norm_dir, {})
        for name in names:
            parent.pop(name, None)
            prefix = posixpath.join(norm_dir, name)
            for key in list(self.dirs):
                if key == prefix or key.startswith(f"{prefix}/"):
                    self.dirs.pop(key, None)
        return True


def _make_job(base_path: str = "/PikPak/Anime") -> DownloadJob:
    return DownloadJob(
        id="job-copy-1",
        candidate=ReleaseCandidate.create(
            source_name="manual",
            source_url="api",
            title="[Lilith-Raws] Frieren - 01 [1080p][CHT]",
            download_url="magnet:?xt=urn:btih:0123456789abcdef",
        ),
        metadata=MetadataDocument(
            values=ReleaseMetadata(anime_name="Frieren", season=1, episode=1)
        ),
        artifact={"base_path": base_path},
    )


def _make_result() -> OrganizationResult:
    return OrganizationResult(
        item_key="ep-1",
        state="completed",
        final_path="/PikPak/Anime/Frieren/Season 1/Frieren S01E01.mkv",
        sidecar_paths=("/PikPak/Anime/Frieren/Season 1/Frieren S01E01.zh.ass",),
    )


def _make_executor(
    client: _InMemoryCopyClient,
    *,
    targets: list[str],
    delete_source: bool = True,
) -> OpenListPostCopyExecutor:
    sleep = AsyncMock()
    storage = OpenListStorageOperations(client, sleep, cache_refresh_seconds=0)
    return OpenListPostCopyExecutor(
        client,
        storage,
        copy_target_paths=targets,
        delete_source_after_copy=delete_source,
        sleep=sleep,
        poll_interval_seconds=0,
        verify_retries=2,
    )


@pytest.mark.asyncio
async def test_client_copy_file_and_task_endpoints():
    client = OpenListClient("http://openlist.test", token="secret")
    client._post = AsyncMock(
        return_value={
            "code": 200,
            "data": {
                "message": "Successfully created 1 copy task(s)",
                "tasks": [{"id": "tid-1", "name": "copy video", "state": 0}],
            },
        }
    )
    ok, tasks = await client.copy_file(
        "/PikPak/Anime/Frieren/Season 1",
        "/115/Anime/Frieren/Season 1",
        ["Frieren S01E01.mkv"],
    )
    assert ok is True
    assert len(tasks) == 1
    assert tasks[0].id == "tid-1"
    client._post.assert_awaited_once_with(
        "http://openlist.test/api/fs/copy",
        {
            "src_dir": "/PikPak/Anime/Frieren/Season 1",
            "dst_dir": "/115/Anime/Frieren/Season 1",
            "names": ["Frieren S01E01.mkv"],
        },
    )

    client._get = AsyncMock(
        side_effect=[
            {"code": 200, "data": [{"id": "tid-1", "name": "copy video", "state": 1}]},
            {"code": 200, "data": [{"id": "tid-1", "name": "copy video", "state": 2}]},
        ]
    )
    undone = await client.get_copy_tasks_undone()
    done = await client.get_copy_tasks_done()
    assert undone is not None and undone[0].state == OpenlistTaskState.RUNNING
    assert done is not None and done[0].state == OpenlistTaskState.SUCCEEDED


@pytest.mark.asyncio
async def test_async_copy_waits_for_completion_and_deletes_pikpak_source():
    client = _InMemoryCopyClient(async_copy=True, polls_before_done=2)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115")

    executor = _make_executor(client, targets=["/115/Anime"], delete_source=True)
    saved: list[dict] = []

    async def save_state(state: dict) -> None:
        saved.append(state)

    results = await executor.execute(
        _make_job(),
        (_make_result(),),
        save_state=save_state,
    )

    assert len(results) == 1
    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert results[0].sidecar_paths == (
        "/115/Anime/Frieren/Season 1/Frieren S01E01.zh.ass",
    )
    assert "Frieren S01E01.mkv" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert "Frieren S01E01.zh.ass" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}
    assert saved[-1]["items"]["ep-1"]["source_deleted"] is True
    assert saved[-1]["items"]["ep-1"]["state"] == "completed"


@pytest.mark.asyncio
async def test_sync_and_legacy_async_copy_both_complete_and_clean_source():
    for async_copy, return_tasks in ((False, False), (True, False)):
        client = _InMemoryCopyClient(
            async_copy=async_copy,
            return_tasks_on_copy=return_tasks,
            polls_before_done=1,
        )
        client.put_file(
            "/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048
        )
        client.put_file(
            "/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128
        )
        client._ensure_dir("/Local/Anime")

        executor = _make_executor(client, targets=["/Local/Anime"], delete_source=True)
        results = await executor.execute(_make_job(), (_make_result(),))

        assert (
            results[0].final_path == "/Local/Anime/Frieren/Season 1/Frieren S01E01.mkv"
        )
        assert "Frieren S01E01.mkv" in client.dirs["/Local/Anime/Frieren/Season 1"]
        assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_multiple_targets_delete_source_only_after_all_targets_succeed():
    client = _InMemoryCopyClient(
        async_copy=True,
        polls_before_done=0,
        fail_targets={"/Local/Anime"},
    )
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime")
    client._ensure_dir("/Local/Anime")

    executor = _make_executor(
        client, targets=["/115/Anime", "/Local/Anime"], delete_source=True
    )
    checkpoint: dict = {}

    async def save_state(state: dict) -> None:
        checkpoint.clear()
        checkpoint.update(state)

    with pytest.raises(OrganizationError, match="remote quota exceeded"):
        await executor.execute(_make_job(), (_make_result(),), save_state=save_state)

    # First target succeeded, second failed -> source MUST remain intact.
    assert "Frieren S01E01.mkv" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert "Frieren S01E01.mkv" in client.dirs["/PikPak/Anime/Frieren/Season 1"]
    assert client.remove_calls == []
    assert checkpoint["items"]["ep-1"]["targets"][0]["state"] == "completed"
    assert checkpoint["items"]["ep-1"]["targets"][1]["state"] == "pending"

    # Second attempt recovers: skips completed target 1, copies target 2, deletes source.
    client.fail_targets.clear()
    copy_calls_before = len(client.copy_calls)
    results = await executor.execute(
        _make_job(),
        (_make_result(),),
        saved_state=checkpoint,
        save_state=save_state,
    )

    assert len(client.copy_calls) == copy_calls_before + 1
    assert client.copy_calls[-1]["dst_dir"] == "/Local/Anime/Frieren/Season 1"
    assert "Frieren S01E01.mkv" in client.dirs["/Local/Anime/Frieren/Season 1"]
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}
    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"


@pytest.mark.asyncio
async def test_delete_source_false_preserves_source_and_original_final_path():
    client = _InMemoryCopyClient(async_copy=True, polls_before_done=0)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime")

    executor = _make_executor(client, targets=["/115/Anime"], delete_source=False)
    results = await executor.execute(_make_job(), (_make_result(),))

    assert results[0].final_path == "/PikPak/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert "Frieren S01E01.mkv" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert "Frieren S01E01.mkv" in client.dirs["/PikPak/Anime/Frieren/Season 1"]
    assert client.remove_calls == []


@pytest.mark.asyncio
async def test_crash_recovery_resumes_in_flight_copy_without_resubmitting():
    client = _InMemoryCopyClient(async_copy=True, polls_before_done=1)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime/Frieren/Season 1")

    # Simulate already-submitted OpenList tasks captured in checkpoint before crash.
    ok, tasks = await client.copy_file(
        "/PikPak/Anime/Frieren/Season 1",
        "/115/Anime/Frieren/Season 1",
        ["Frieren S01E01.mkv", "Frieren S01E01.zh.ass"],
    )
    assert ok is True
    saved_state = {
        "version": 1,
        "items": {
            "ep-1": {
                "item_key": "ep-1",
                "src_dir": "/PikPak/Anime/Frieren/Season 1",
                "filenames": ["Frieren S01E01.mkv", "Frieren S01E01.zh.ass"],
                "expected_sizes": {
                    "Frieren S01E01.mkv": 2048,
                    "Frieren S01E01.zh.ass": 128,
                },
                "targets": [
                    {
                        "target_root": "/115/Anime",
                        "dst_dir": "/115/Anime/Frieren/Season 1",
                        "state": "submitted",
                        "task_ids": [task.id for task in tasks],
                    }
                ],
                "source_deleted": False,
                "state": "pending",
            }
        },
    }
    calls_before = len(client.copy_calls)

    executor = _make_executor(client, targets=["/115/Anime"], delete_source=True)
    results = await executor.execute(
        _make_job(), (_make_result(),), saved_state=saved_state
    )

    assert len(client.copy_calls) == calls_before
    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_size_mismatch_refuses_to_delete_source_and_retries_with_overwrite():
    client = _InMemoryCopyClient(
        async_copy=False,
        corrupt_size_targets={"/115/Anime"},
    )
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime")

    executor = _make_executor(client, targets=["/115/Anime"], delete_source=True)
    checkpoint: dict = {}

    async def save_state(state: dict) -> None:
        checkpoint.clear()
        checkpoint.update(state)

    with pytest.raises(OrganizationError, match="size mismatch"):
        await executor.execute(_make_job(), (_make_result(),), save_state=save_state)

    assert "Frieren S01E01.mkv" in client.dirs["/PikPak/Anime/Frieren/Season 1"]
    assert client.remove_calls == []

    # Next retry sees mismatched file in target and passes overwrite=True.
    client.corrupt_size_targets.clear()
    results = await executor.execute(
        _make_job(),
        (_make_result(),),
        saved_state=checkpoint,
        save_state=save_state,
    )

    assert client.copy_calls[-1]["overwrite"] is True
    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_organizer_adapter_runs_organize_then_post_copy_and_survives_replay():
    client = _InMemoryCopyClient(async_copy=True, polls_before_done=1)
    staging_root = "/PikPak/Anime/.oani-download-tmp/job-copy-1"
    client.put_file(staging_root, "raw.mkv", size=4096)
    client.put_file(staging_root, "raw.zh.ass", size=256)
    client._ensure_dir("/115/Anime")

    sleep = AsyncMock()
    adapter = OpenListOrganizerAdapter(
        client,
        sleep=sleep,
        copy_target_paths=["/115/Anime"],
        delete_source_after_copy=True,
    )
    adapter._storage._cache_refresh_seconds = 0
    adapter._post_copy._poll_interval_seconds = 0

    job = _make_job()
    manifest = DownloadManifest(
        root_path=staging_root,
        files=(
            DownloadedFile("raw.mkv", size=4096),
            DownloadedFile("raw.zh.ass", size=256),
        ),
        cleanup_root=staging_root,
    )
    request = OrganizationRequest(
        item_key="ep-1",
        video_relative_path="raw.mkv",
        sidecars=(OrganizationSidecar("raw.zh.ass", ".zh"),),
        target_directory_path="/PikPak/Anime/Frieren/Season 1",
        target_filename="Frieren S01E01.mkv",
        metadata=job.metadata,
    )

    async def checkpoint(payload: dict) -> None:
        job.artifact["organization_checkpoint"] = dict(payload)

    results = await adapter.organize(job, manifest, (request,), checkpoint)
    assert results[0].state == "completed"
    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert results[0].sidecar_paths == (
        "/115/Anime/Frieren/Season 1/Frieren S01E01.zh.ass",
    )
    assert "Frieren S01E01.mkv" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}
    assert staging_root not in client.dirs

    # Replay from persisted checkpoint is idempotent and does not re-copy.
    copy_count = len(client.copy_calls)
    replayed = await adapter.organize(job, manifest, (request,), checkpoint)
    assert replayed == results
    assert len(client.copy_calls) == copy_count


@pytest.mark.asyncio
async def test_done_task_poll_failures_terminate_instead_of_looping_forever():
    client = _InMemoryCopyClient(async_copy=True, polls_before_done=0)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime/Frieren/Season 1")
    client.get_copy_tasks_undone = AsyncMock(return_value=[])
    client.get_copy_tasks_done = AsyncMock(return_value=None)

    saved_state = {
        "version": 1,
        "items": {
            "ep-1": {
                "item_key": "ep-1",
                "src_dir": "/PikPak/Anime/Frieren/Season 1",
                "filenames": ["Frieren S01E01.mkv", "Frieren S01E01.zh.ass"],
                "expected_sizes": {
                    "Frieren S01E01.mkv": 2048,
                    "Frieren S01E01.zh.ass": 128,
                },
                "targets": [
                    {
                        "target_root": "/115/Anime",
                        "dst_dir": "/115/Anime/Frieren/Season 1",
                        "state": "submitted",
                        "task_ids": ["copy-1"],
                    }
                ],
                "source_deleted": False,
                "state": "pending",
            }
        },
    }

    executor = _make_executor(client, targets=["/115/Anime"], delete_source=True)
    with pytest.raises(OrganizationError, match="done copy tasks"):
        await executor.execute(_make_job(), (_make_result(),), saved_state=saved_state)

    assert client.get_copy_tasks_done.await_count == executor._MAX_POLL_ERRORS
    assert "Frieren S01E01.mkv" in client.dirs["/PikPak/Anime/Frieren/Season 1"]
    assert client.remove_calls == []


@pytest.mark.asyncio
async def test_pending_retry_attaches_to_in_flight_copy_without_duplicating():
    client = _InMemoryCopyClient(async_copy=True, polls_before_done=1)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime/Frieren/Season 1")

    # Simulate POST /api/fs/copy succeeding right before a crash prior to checkpoint save.
    ok, _tasks = await client.copy_file(
        "/PikPak/Anime/Frieren/Season 1",
        "/115/Anime/Frieren/Season 1",
        ["Frieren S01E01.mkv", "Frieren S01E01.zh.ass"],
    )
    assert ok is True
    calls_before = len(client.copy_calls)

    executor = _make_executor(client, targets=["/115/Anime"], delete_source=True)
    results = await executor.execute(_make_job(), (_make_result(),))

    assert len(client.copy_calls) == calls_before
    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_nested_missing_target_root_creates_only_missing_subdirectories():
    client = _InMemoryCopyClient(async_copy=False)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115")
    mkdir_calls: list[str] = []
    original_mkdir = client.mkdir

    async def tracking_mkdir(path: str) -> bool:
        mkdir_calls.append(path)
        return await original_mkdir(path)

    client.mkdir = tracking_mkdir

    executor = _make_executor(client, targets=["/115/Anime/2026"], delete_source=True)
    results = await executor.execute(_make_job(), (_make_result(),))

    assert "/115" not in mkdir_calls
    assert mkdir_calls == [
        "/115/Anime",
        "/115/Anime/2026",
        "/115/Anime/2026/Frieren",
        "/115/Anime/2026/Frieren/Season 1",
    ]
    assert (
        results[0].final_path == "/115/Anime/2026/Frieren/Season 1/Frieren S01E01.mkv"
    )


@pytest.mark.asyncio
async def test_conflicting_directory_in_target_raises_and_keeps_source():
    client = _InMemoryCopyClient(async_copy=False)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime/Frieren/Season 1/Frieren S01E01.mkv")

    executor = _make_executor(client, targets=["/115/Anime"], delete_source=True)
    with pytest.raises(OrganizationError, match="directory with the same name"):
        await executor.execute(_make_job(), (_make_result(),))

    assert "Frieren S01E01.mkv" in client.dirs["/PikPak/Anime/Frieren/Season 1"]
    assert client.remove_calls == []


class _InMemoryAria2CopyClient(_InMemoryCopyClient):
    """In-memory OpenList simulator supporting /api/fs/get, aria2 offline download, and transfer."""

    def __init__(
        self,
        *,
        admin_settings: dict[str, str] | None = None,
        rpc_succeeds: bool = True,
        download_polls: int = 1,
        transfer_polls: int = 1,
        mangled_download_names: dict[str, str] | None = None,
        fail_download_targets: set[str] | None = None,
        fail_transfer_targets: set[str] | None = None,
        corrupt_size_targets: set[str] | None = None,
    ) -> None:
        super().__init__(corrupt_size_targets=corrupt_size_targets)
        self.base_url = "http://openlist.test"
        self.admin_settings = (
            dict(admin_settings)
            if admin_settings is not None
            else {
                "aria2_uri": "http://aria2:6800/jsonrpc",
                "aria2_secret": "openlist-aria2-secret",
            }
        )
        self.rpc_succeeds = rpc_succeeds
        self.download_polls = download_polls
        self.transfer_polls = transfer_polls
        self.mangled_download_names = dict(mangled_download_names or {})
        self.fail_download_targets = set(fail_download_targets or ())
        self.fail_transfer_targets = set(fail_transfer_targets or ())
        self.admin_settings_calls: list[list[str]] = []
        self.aria2_rpc_calls: list[dict[str, object]] = []
        self.file_info_calls: list[str] = []
        self.offline_download_calls: list[dict[str, object]] = []
        self._url_to_source: dict[str, tuple[str, str]] = {}
        self._dl_seq = 0
        self._tx_seq = 0
        self._dl_undone: dict[str, dict[str, object]] = {}
        self._dl_done: list[OpenlistTask] = []
        self._tx_undone: dict[str, dict[str, object]] = {}
        self._tx_done: list[OpenlistTask] = []

    async def get_admin_settings(self, keys: list[str]) -> dict[str, str] | None:
        self.admin_settings_calls.append(list(keys))
        return {k: self.admin_settings[k] for k in keys if k in self.admin_settings}

    async def configure_aria2_options(
        self,
        rpc_url: str,
        options: dict[str, str],
        *,
        secret: str = "",
    ) -> bool:
        self.aria2_rpc_calls.append(
            {"rpc_url": rpc_url, "options": dict(options), "secret": secret}
        )
        return self.rpc_succeeds

    async def get_file_info(self, path: str) -> dict[str, object] | None:
        norm = posixpath.normpath(path)
        self.file_info_calls.append(norm)
        parent, name = posixpath.split(norm)
        entry = self.dirs.get(parent, {}).get(name)
        if entry is None or entry.is_dir:
            return None
        url = f"https://pikpak.cdn.test/file/{len(self._url_to_source) + 1}"
        self._url_to_source[url] = (parent, name)
        return {"name": name, "size": entry.size, "is_dir": False, "raw_url": url}

    async def add_offline_download(
        self,
        urls: list[str],
        path: str,
        tool: str,
        delete_policy: str = "delete_always",
    ) -> list[OpenlistTask] | None:
        self.offline_download_calls.append(
            {
                "urls": list(urls),
                "path": path,
                "tool": str(tool),
                "delete_policy": delete_policy,
            }
        )
        tasks: list[OpenlistTask] = []
        for url in urls:
            self._dl_seq += 1
            tid = f"aria2-dl-{self._dl_seq}"
            task_name = f"download {url} to ({path})"
            self._dl_undone[tid] = {
                "task": OpenlistTask(
                    id=tid,
                    name=task_name,
                    state=OpenlistTaskState.RUNNING,
                    progress=50,
                ),
                "url": url,
                "staging_dir": posixpath.normpath(path),
                "remaining_polls": self.download_polls,
            }
            tasks.append(
                OpenlistTask(
                    id=tid,
                    name=task_name,
                    state=OpenlistTaskState.PENDING,
                    progress=0,
                )
            )
        return tasks

    async def get_offline_download_undone(self) -> list[OpenlistTask]:
        running: list[OpenlistTask] = []
        for tid in list(self._dl_undone):
            rec = self._dl_undone[tid]
            rem = int(rec["remaining_polls"])
            if rem > 0:
                rec["remaining_polls"] = rem - 1
                running.append(rec["task"])
                continue
            self._dl_undone.pop(tid)
            staging_dir = str(rec["staging_dir"])
            if any(staging_dir.startswith(p) for p in self.fail_download_targets):
                self._dl_done.append(
                    OpenlistTask(
                        id=tid,
                        name=rec["task"].name,
                        state=OpenlistTaskState.FAILED,
                        error="aria2 HTTP 503",
                    )
                )
                continue
            self._dl_done.append(
                OpenlistTask(
                    id=tid,
                    name=rec["task"].name,
                    state=OpenlistTaskState.SUCCEEDED,
                    progress=100,
                )
            )
            self._tx_seq += 1
            tx_id = f"aria2-tx-{self._tx_seq}"
            tx_name = f"transfer [](/tmp/aria2/{tx_id}/file) to [{staging_dir}]"
            self._tx_undone[tx_id] = {
                "task": OpenlistTask(
                    id=tx_id,
                    name=tx_name,
                    state=OpenlistTaskState.RUNNING,
                    progress=50,
                ),
                "url": rec["url"],
                "staging_dir": staging_dir,
                "remaining_polls": self.transfer_polls,
            }
        return running

    async def get_offline_download_done(self) -> list[OpenlistTask]:
        return list(self._dl_done)

    async def get_offline_download_transfer_undone(self) -> list[OpenlistTask]:
        running: list[OpenlistTask] = []
        for tx_id in list(self._tx_undone):
            rec = self._tx_undone[tx_id]
            rem = int(rec["remaining_polls"])
            if rem > 0:
                rec["remaining_polls"] = rem - 1
                running.append(rec["task"])
                continue
            self._tx_undone.pop(tx_id)
            staging_dir = str(rec["staging_dir"])
            if any(staging_dir.startswith(p) for p in self.fail_transfer_targets):
                self._tx_done.append(
                    OpenlistTask(
                        id=tx_id,
                        name=rec["task"].name,
                        state=OpenlistTaskState.FAILED,
                        error="target upload timeout",
                    )
                )
                continue
            src_dir, src_name = self._url_to_source[str(rec["url"])]
            src_entry = self.dirs[src_dir][src_name]
            staged_name = self.mangled_download_names.get(src_name, src_name)
            size = (
                src_entry.size - 1
                if any(staging_dir.startswith(p) for p in self.corrupt_size_targets)
                else src_entry.size
            )
            self._ensure_dir(staging_dir)[staged_name] = SimpleNamespace(
                name=staged_name, is_dir=False, size=size
            )
            self._tx_done.append(
                OpenlistTask(
                    id=tx_id,
                    name=rec["task"].name,
                    state=OpenlistTaskState.SUCCEEDED,
                    progress=100,
                )
            )
        return running

    async def get_offline_download_transfer_done(self) -> list[OpenlistTask]:
        return list(self._tx_done)


def _make_aria2_executor(
    client: _InMemoryAria2CopyClient,
    *,
    targets: list[str],
    copy_threads: int = 8,
    aria2_rpc_url: str = "",
    aria2_rpc_secret: str = "",
    delete_source: bool = True,
) -> OpenListPostCopyExecutor:
    sleep = AsyncMock()
    storage = OpenListStorageOperations(client, sleep, cache_refresh_seconds=0)
    return OpenListPostCopyExecutor(
        client,
        storage,
        copy_target_paths=targets,
        copy_mode="aria2",
        copy_threads=copy_threads,
        aria2_rpc_url=aria2_rpc_url,
        aria2_rpc_secret=aria2_rpc_secret,
        delete_source_after_copy=delete_source,
        sleep=sleep,
        poll_interval_seconds=0,
        verify_retries=2,
    )


@pytest.mark.asyncio
async def test_client_get_file_info_admin_settings_and_aria2_rpc(monkeypatch):
    client = OpenListClient("http://openlist.test", token="secret")
    client._post = AsyncMock(
        return_value={
            "code": 200,
            "data": {
                "name": "ep1.mkv",
                "size": 1024,
                "raw_url": "https://cdn.test/ep1.mkv",
            },
        }
    )
    info = await client.get_file_info("/PikPak/Anime/ep1.mkv")
    assert info is not None and info["raw_url"] == "https://cdn.test/ep1.mkv"
    client._post.assert_awaited_once_with(
        "http://openlist.test/api/fs/get",
        {"path": "/PikPak/Anime/ep1.mkv", "password": ""},
    )

    client._get = AsyncMock(
        return_value={
            "code": 200,
            "data": [
                {"key": "aria2_uri", "value": "http://aria2:6800/jsonrpc"},
                {"key": "aria2_secret", "value": "s3cr3t"},
            ],
        }
    )
    settings = await client.get_admin_settings(["aria2_uri", "aria2_secret"])
    assert settings == {
        "aria2_uri": "http://aria2:6800/jsonrpc",
        "aria2_secret": "s3cr3t",
    }

    captured: list[tuple[str, dict]] = []

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        async def json(self, content_type=None) -> dict:
            return {"id": "openlist-ani-copy", "jsonrpc": "2.0", "result": "OK"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

    class _FakeSession:
        def __init__(self, **_kwargs):
            return None

        def post(self, url: str, *, json: dict):
            captured.append((url, json))
            return _FakeResponse()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

    monkeypatch.setattr(
        "openlist_ani.adapters.download_backends.openlist.client.aiohttp.ClientSession",
        _FakeSession,
    )
    ok = await client.configure_aria2_options(
        "ws://aria2:6800/jsonrpc",
        {"split": "8", "max-connection-per-server": "8", "min-split-size": "1M"},
        secret="s3cr3t",
    )
    assert ok is True
    assert captured == [
        (
            "http://aria2:6800/jsonrpc",
            {
                "jsonrpc": "2.0",
                "id": "openlist-ani-copy",
                "method": "aria2.changeGlobalOption",
                "params": [
                    "token:s3cr3t",
                    {
                        "split": "8",
                        "max-connection-per-server": "8",
                        "min-split-size": "1M",
                    },
                ],
            },
        )
    ]


@pytest.mark.asyncio
async def test_aria2_copy_mode_configures_8_threads_renames_mangled_file_and_cleans_source():
    client = _InMemoryAria2CopyClient(
        mangled_download_names={"Frieren S01E01.mkv": "download_raw_blob.bin"}
    )
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=4096)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=256)
    client._ensure_dir("/115")

    executor = _make_aria2_executor(client, targets=["/115/Anime"], delete_source=True)
    results = await executor.execute(_make_job(), (_make_result(),))

    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert results[0].sidecar_paths == (
        "/115/Anime/Frieren/Season 1/Frieren S01E01.zh.ass",
    )
    assert client.aria2_rpc_calls == [
        {
            "rpc_url": "http://aria2:6800/jsonrpc",
            "options": {
                "split": "8",
                "max-connection-per-server": "8",
                "min-split-size": "1M",
            },
            "secret": "openlist-aria2-secret",
        }
    ]
    assert len(client.offline_download_calls) == 2
    assert all(call["tool"] == "aria2" for call in client.offline_download_calls)
    assert "Frieren S01E01.mkv" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert "Frieren S01E01.zh.ass" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert client.dirs["/115/Anime/.oani-copy-tmp"] == {}
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_aria2_copy_mode_proceeds_when_rpc_unreachable_and_honors_custom_threads():
    client = _InMemoryAria2CopyClient(rpc_succeeds=False)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime")

    executor = _make_aria2_executor(
        client,
        targets=["/115/Anime"],
        copy_threads=12,
        aria2_rpc_url="http://custom-aria2:6800/jsonrpc",
        aria2_rpc_secret="custom-token",
        delete_source=False,
    )
    results = await executor.execute(_make_job(), (_make_result(),))

    assert client.admin_settings_calls == []
    assert client.aria2_rpc_calls == [
        {
            "rpc_url": "http://custom-aria2:6800/jsonrpc",
            "options": {
                "split": "12",
                "max-connection-per-server": "12",
                "min-split-size": "1M",
            },
            "secret": "custom-token",
        }
    ]
    assert results[0].final_path == "/PikPak/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert "Frieren S01E01.mkv" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert "Frieren S01E01.mkv" in client.dirs["/PikPak/Anime/Frieren/Season 1"]


@pytest.mark.asyncio
async def test_aria2_copy_failure_cleans_staging_preserves_source_and_recovers_on_retry():
    client = _InMemoryAria2CopyClient(fail_transfer_targets={"/115/Anime"})
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime")

    executor = _make_aria2_executor(client, targets=["/115/Anime"], delete_source=True)
    checkpoint: dict = {}

    async def save_state(state: dict) -> None:
        checkpoint.clear()
        checkpoint.update(state)

    with pytest.raises(OrganizationError, match="target upload timeout"):
        await executor.execute(_make_job(), (_make_result(),), save_state=save_state)

    assert "Frieren S01E01.mkv" in client.dirs["/PikPak/Anime/Frieren/Season 1"]
    assert client.dirs["/115/Anime/.oani-copy-tmp"] == {}

    client.fail_transfer_targets.clear()
    results = await executor.execute(
        _make_job(),
        (_make_result(),),
        saved_state=checkpoint,
        save_state=save_state,
    )

    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert "Frieren S01E01.mkv" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_aria2_copy_size_mismatch_rejects_and_replaces_stale_target_on_retry():
    client = _InMemoryAria2CopyClient(corrupt_size_targets={"/115/Anime"})
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    # Pre-existing stale/truncated file in destination directory.
    client.put_file("/115/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=100)

    executor = _make_aria2_executor(client, targets=["/115/Anime"], delete_source=True)
    checkpoint: dict = {}

    async def save_state(state: dict) -> None:
        checkpoint.clear()
        checkpoint.update(state)

    with pytest.raises(OrganizationError, match="size mismatch"):
        await executor.execute(_make_job(), (_make_result(),), save_state=save_state)

    assert "Frieren S01E01.mkv" in client.dirs["/PikPak/Anime/Frieren/Season 1"]
    assert client.dirs["/115/Anime/.oani-copy-tmp"] == {}

    client.corrupt_size_targets.clear()
    results = await executor.execute(
        _make_job(),
        (_make_result(),),
        saved_state=checkpoint,
        save_state=save_state,
    )

    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert client.dirs["/115/Anime/Frieren/Season 1"]["Frieren S01E01.mkv"].size == 2048
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_aria2_copy_crash_recovery_resumes_uncertain_submission_without_duplicating():
    client = _InMemoryAria2CopyClient(download_polls=1, transfer_polls=1)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    staging_token = "job-copy-1-t0-f0-deadbeef"
    staging_dir = f"/115/Anime/.oani-copy-tmp/{staging_token}"
    client._ensure_dir(staging_dir)
    client._ensure_dir("/115/Anime/Frieren/Season 1")

    # Simulate add_offline_download succeeding right before a crash prior to checkpointing "downloading".
    info = await client.get_file_info(
        "/PikPak/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    )
    assert info is not None
    await client.add_offline_download(
        urls=[str(info["raw_url"])],
        path=staging_dir,
        tool="aria2",
        delete_policy="delete_always",
    )
    saved_state = {
        "version": 1,
        "items": {
            "ep-1": {
                "item_key": "ep-1",
                "src_dir": "/PikPak/Anime/Frieren/Season 1",
                "filenames": ["Frieren S01E01.mkv", "Frieren S01E01.zh.ass"],
                "expected_sizes": {
                    "Frieren S01E01.mkv": 2048,
                    "Frieren S01E01.zh.ass": 128,
                },
                "targets": [
                    {
                        "target_root": "/115/Anime",
                        "dst_dir": "/115/Anime/Frieren/Season 1",
                        "state": "pending",
                        "task_ids": [],
                        "files": {
                            "Frieren S01E01.mkv": {
                                "state": "submitting",
                                "staging_token": staging_token,
                                "staging_dir": staging_dir,
                                "task_id": "",
                            },
                            "Frieren S01E01.zh.ass": {"state": "pending"},
                        },
                    }
                ],
                "source_deleted": False,
                "state": "pending",
            }
        },
    }

    executor = _make_aria2_executor(client, targets=["/115/Anime"], delete_source=True)
    results = await executor.execute(
        _make_job(), (_make_result(),), saved_state=saved_state
    )

    # Video was recovered (not re-submitted), only the subtitle triggered a new add_offline_download.
    assert len(client.offline_download_calls) == 2
    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert "Frieren S01E01.mkv" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert "Frieren S01E01.zh.ass" in client.dirs["/115/Anime/Frieren/Season 1"]
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_aria2_copy_finalizing_recovery_reuses_already_moved_file_and_cleans_staging():
    client = _InMemoryAria2CopyClient(download_polls=0, transfer_polls=0)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    # Simulate crash after moving video to dst_dir but before cleaning staging and saving state.
    staging_token = "job-copy-1-t0-f0-cafebabe"
    staging_dir = f"/115/Anime/.oani-copy-tmp/{staging_token}"
    client._ensure_dir(staging_dir)
    client.put_file("/115/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/115/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)

    saved_state = {
        "version": 1,
        "items": {
            "ep-1": {
                "item_key": "ep-1",
                "src_dir": "/PikPak/Anime/Frieren/Season 1",
                "filenames": ["Frieren S01E01.mkv", "Frieren S01E01.zh.ass"],
                "expected_sizes": {
                    "Frieren S01E01.mkv": 2048,
                    "Frieren S01E01.zh.ass": 128,
                },
                "targets": [
                    {
                        "target_root": "/115/Anime",
                        "dst_dir": "/115/Anime/Frieren/Season 1",
                        "state": "pending",
                        "task_ids": [],
                        "files": {
                            "Frieren S01E01.mkv": {
                                "state": "finalizing",
                                "staging_token": staging_token,
                                "staging_dir": staging_dir,
                                "task_id": "",
                            },
                            "Frieren S01E01.zh.ass": {"state": "completed"},
                        },
                    }
                ],
                "source_deleted": False,
                "state": "pending",
            }
        },
    }

    executor = _make_aria2_executor(client, targets=["/115/Anime"], delete_source=True)
    results = await executor.execute(
        _make_job(), (_make_result(),), saved_state=saved_state
    )

    assert client.offline_download_calls == []
    assert client.dirs["/115/Anime/.oani-copy-tmp"] == {}
    assert results[0].final_path == "/115/Anime/Frieren/Season 1/Frieren S01E01.mkv"
    assert client.dirs["/PikPak/Anime/Frieren/Season 1"] == {}


@pytest.mark.asyncio
async def test_aria2_explicit_rpc_url_falls_back_to_openlist_aria2_secret_on_auth_error():
    client = _InMemoryAria2CopyClient(download_polls=0, transfer_polls=0)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.mkv", size=2048)
    client.put_file("/PikPak/Anime/Frieren/Season 1", "Frieren S01E01.zh.ass", size=128)
    client._ensure_dir("/115/Anime")

    async def auth_checked_rpc(
        rpc_url: str, options: dict[str, str], *, secret: str = ""
    ) -> bool:
        client.aria2_rpc_calls.append(
            {"rpc_url": rpc_url, "options": dict(options), "secret": secret}
        )
        return secret == "openlist-aria2-secret"

    client.configure_aria2_options = auth_checked_rpc

    executor = _make_aria2_executor(
        client,
        targets=["/115/Anime"],
        aria2_rpc_url="http://192.168.1.10:6800/jsonrpc",
        aria2_rpc_secret="",
        delete_source=True,
    )
    await executor.execute(_make_job(), (_make_result(),))

    assert [call["secret"] for call in client.aria2_rpc_calls] == [
        "",
        "openlist-aria2-secret",
    ]
    assert client.admin_settings_calls == [["aria2_secret"]]
