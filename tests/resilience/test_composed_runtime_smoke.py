import asyncio
import posixpath
import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from openlist_ani.adapters.configuration import (
    ConfigManager,
    compile_core_settings,
)
from openlist_ani.adapters.configuration import loader as config_loader
from openlist_ani.adapters.download_backends.openlist import (
    OpenlistTask,
    OpenlistTaskState,
)
from openlist_ani.adapters.http.app import create_app
from openlist_ani.adapters.http.service import BackendApiService
from openlist_ani.adapters.persistence import LegacyMigrationRunner
from openlist_ani.bootstrap import backend as backend_module
from openlist_ani.bootstrap.backend import _compose_runtime, _load_runtime_config


@pytest.mark.asyncio
async def test_composed_runtime_starts_ready_and_releases_database(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[rss]
urls = []
torrent_to_magnet = true

[metadata_parser]
provider = "regex"

[metadata_validator]
provider = "none"

[openlist]
url = "http://127.0.0.1:1"
token = ""
download_path = "/anime"
offline_download_tool = "qBittorrent"
rename_format = "{anime_name} S{season:02d}E{episode:02d}"
""".strip(),
        encoding="utf-8",
    )
    config = ConfigManager(str(config_path))
    settings = compile_core_settings(config.data)
    LegacyMigrationRunner().run()

    assembly = await _compose_runtime(config, settings)
    assert [
        transformer.name
        for transformer in assembly.runtime.metadata_worker._candidate_transformers
    ] == ["torrent_to_magnet"]
    await assembly.runtime.start()
    try:
        await asyncio.sleep(0)

        health = assembly.runtime.health()
        assert health["status"] == "ready"
        assert health["ready"] is True
        assert health["workers"] == {"running": 6, "expected": 6}
    finally:
        await assembly.runtime.stop()

    with closing(sqlite3.connect(tmp_path / "data" / "data.db")) as connection:
        connection.execute("BEGIN EXCLUSIVE")
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        connection.rollback()


class _FakePikPakOpenListClient:
    """Minimal in-memory OpenList client covering PikPak offline download & organize."""

    def __init__(self, downloaded_filename: str) -> None:
        self._downloaded_filename = downloaded_filename
        self.base_url = "http://127.0.0.1:5244"
        self.offline_calls: list[dict[str, object]] = []
        self.copy_calls: list[dict[str, object]] = []
        self.aria2_rpc_calls: list[dict[str, object]] = []
        self._copy_tasks_undone: list[OpenlistTask] = []
        self._copy_tasks_done: list[OpenlistTask] = []
        self._copy_payloads: dict[str, tuple[str, str, str]] = {}
        self._offline_tasks_done: list[OpenlistTask] = []
        self._transfer_tasks_done: list[OpenlistTask] = []
        self._url_sources: dict[str, tuple[str, str]] = {}
        self._dirs: dict[str, dict[str, SimpleNamespace]] = {
            "/": {},
            "/PikPak": {},
            "/PikPak/Anime": {},
            "/115": {},
            "/115/Anime": {},
        }

    def _ensure_dir(self, path: str) -> dict[str, SimpleNamespace]:
        normalized = posixpath.normpath(path)
        parts = [part for part in normalized.split("/") if part]
        current = "/"
        self._dirs.setdefault("/", {})
        for part in parts:
            parent = current
            current = posixpath.join(current, part)
            self._dirs[parent].setdefault(
                part, SimpleNamespace(name=part, is_dir=True, size=0)
            )
            self._dirs.setdefault(current, {})
        return self._dirs[current]

    async def is_healthy(self) -> bool:
        return True

    async def get_offline_download_tools(self) -> list[str]:
        return ["PikPak", "aria2"]

    async def get_admin_settings(self, keys: list[str]) -> dict[str, str]:
        defaults = {
            "aria2_uri": "http://aria2:6800/jsonrpc",
            "aria2_secret": "smoke-secret",
        }
        return {k: defaults[k] for k in keys if k in defaults}

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
        return True

    async def get_file_info(self, path: str) -> dict[str, object] | None:
        normalized = posixpath.normpath(path)
        parent, name = posixpath.split(normalized)
        entry = self._dirs.get(parent, {}).get(name)
        if entry is None:
            return None
        url = f"https://pikpak.cdn.test/{len(self._url_sources) + 1}/{name}"
        self._url_sources[url] = (parent, name)
        return {"name": name, "size": entry.size, "is_dir": False, "raw_url": url}

    async def add_offline_download(
        self,
        *,
        urls: list[str],
        path: str,
        tool: str,
        **_kwargs,
    ) -> list[OpenlistTask]:
        self.offline_calls.append({"urls": list(urls), "path": path, "tool": tool})
        entries = self._ensure_dir(path)
        seq = len(self.offline_calls)
        if str(tool).lower() == "aria2":
            src_dir, src_name = self._url_sources[urls[0]]
            src_entry = self._dirs[src_dir][src_name]
            entries["aria2_raw_download.mkv"] = SimpleNamespace(
                name="aria2_raw_download.mkv",
                is_dir=False,
                size=src_entry.size,
            )
            tid = f"aria2-task-{seq}"
            task = OpenlistTask(
                id=tid,
                name=f"download {urls[0]} to ({path})",
                state=OpenlistTaskState.SUCCEEDED,
                progress=100,
            )
            self._offline_tasks_done.append(task)
            self._transfer_tasks_done.append(
                OpenlistTask(
                    id=f"aria2-tx-{seq}",
                    name=f"transfer [](/tmp/aria2/file) to [{path}]",
                    state=OpenlistTaskState.SUCCEEDED,
                    progress=100,
                )
            )
            return [task]

        entries[self._downloaded_filename] = SimpleNamespace(
            name=self._downloaded_filename,
            is_dir=False,
            size=1024,
        )
        tid = f"pikpak-task-{seq}"
        task = OpenlistTask(
            id=tid,
            name="pikpak-download",
            state=OpenlistTaskState.SUCCEEDED,
        )
        self._offline_tasks_done.append(task)
        return [task]

    async def get_offline_download_undone(self) -> list[OpenlistTask]:
        return []

    async def get_offline_download_done(self) -> list[OpenlistTask]:
        return list(self._offline_tasks_done)

    async def get_offline_download_transfer_undone(self) -> list[OpenlistTask]:
        return []

    async def get_offline_download_transfer_done(self) -> list[OpenlistTask]:
        return list(self._transfer_tasks_done)

    async def list_files(self, path: str) -> list[SimpleNamespace]:
        normalized = posixpath.normpath(path)
        entries = self._dirs.get(normalized)
        if entries is None:
            return []
        return list(entries.values())

    async def mkdir(self, path: str) -> bool:
        self._ensure_dir(path)
        return True

    async def rename_file(self, full_path: str, new_name: str) -> bool:
        normalized = posixpath.normpath(full_path)
        parent, old_name = posixpath.split(normalized)
        entries = self._dirs.get(parent or "/")
        if entries is None or old_name not in entries:
            return False
        entry = entries.pop(old_name)
        entries[new_name] = SimpleNamespace(
            name=new_name, is_dir=entry.is_dir, size=entry.size
        )
        return True

    async def move_file(
        self,
        source_directory: str,
        target_directory: str,
        filenames: list[str],
    ) -> bool:
        src = self._dirs.get(posixpath.normpath(source_directory), {})
        dst = self._ensure_dir(target_directory)
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
        tasks: list[OpenlistTask] = []
        for index, name in enumerate(filenames, start=1):
            tid = f"copy-task-{len(self.copy_calls)}-{index}"
            task = OpenlistTask(
                id=tid,
                name=f"copy [{norm_src}/{name}] to [{norm_dst}]",
                state=OpenlistTaskState.RUNNING,
                progress=50,
            )
            self._copy_tasks_undone.append(task)
            self._copy_payloads[tid] = (norm_src, norm_dst, name)
            tasks.append(task)
        return True, tasks

    async def get_copy_tasks_undone(self) -> list[OpenlistTask]:
        if not self._copy_tasks_undone:
            return []
        running = list(self._copy_tasks_undone)
        self._copy_tasks_undone.clear()
        for task in running:
            norm_src, norm_dst, name = self._copy_payloads[task.id]
            entry = self._dirs[norm_src][name]
            self._ensure_dir(norm_dst)[name] = SimpleNamespace(
                name=name, is_dir=False, size=entry.size
            )
            self._copy_tasks_done.append(
                OpenlistTask(
                    id=task.id,
                    name=task.name,
                    state=OpenlistTaskState.SUCCEEDED,
                    progress=100,
                )
            )
        return []

    async def get_copy_tasks_done(self) -> list[OpenlistTask]:
        return list(self._copy_tasks_done)

    async def remove_path(self, directory: str, filenames: list[str]) -> bool:
        parent = self._dirs.get(posixpath.normpath(directory), {})
        for name in filenames:
            parent.pop(name, None)
            prefix = posixpath.join(posixpath.normpath(directory), name)
            for key in list(self._dirs):
                if key == prefix or key.startswith(f"{prefix}/"):
                    self._dirs.pop(key, None)
        return True

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_empty_rss_startup_and_manual_pikpak_magnet_download_end_to_end(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
config_version = 2

[rss]
urls = []

[downloader]
download_path = "/PikPak/Anime"
rename_format = "{anime_name} S{season:02d}E{episode:02d} {fansub} {quality} {languages}"

[downloader.openlist]
url = "http://127.0.0.1:5244"
token = "test-token"
offline_download_tool = "PikPak"

[metadata]
pipeline = ["regex"]
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    monkeypatch.setattr(config_loader, "_config_instance", None)
    monkeypatch.setattr(BackendApiService, "_instance", None)

    raw_filename = "[Lilith-Raws] Frieren - 01 [1080p][CHT].mkv"
    fake_client = _FakePikPakOpenListClient(raw_filename)
    original_bundle_factory = backend_module._create_openlist_backend_bundle

    def _patched_bundle(cfg, *, client=None):
        bundle = original_bundle_factory(cfg, client=client or fake_client)
        bundle.downloader._sleep = AsyncMock()
        bundle.organizer._storage._sleep = AsyncMock()
        bundle.organizer._storage._cache_refresh_seconds = 0
        bundle.organizer._post_copy._sleep = AsyncMock()
        bundle.organizer._post_copy._poll_interval_seconds = 0
        return bundle

    monkeypatch.setattr(
        backend_module, "_create_openlist_backend_bundle", _patched_bundle
    )

    config, settings = _load_runtime_config()
    assert config.rss.urls == []
    LegacyMigrationRunner().run()
    assembly = await _compose_runtime(config, settings)

    feed_fetch_spy = AsyncMock()
    for adapter in assembly.application._registry.feeds:
        monkeypatch.setattr(adapter, "fetch", feed_fetch_spy)

    await backend_module._check_download_backend_health(assembly)
    await assembly.runtime.start()
    BackendApiService.init(assembly.application)
    app = create_app()

    magnet_url = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
    release_title = "[Lilith-Raws] Frieren - 01 [1080p][CHT]"

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as http_client:
            ready_resp = await http_client.get("/health/ready")
            assert ready_resp.status_code == 200
            assert ready_resp.json()["ready"] is True

            create_resp = await http_client.post(
                "/api/downloads",
                json={"download_url": magnet_url, "title": release_title},
            )
            assert create_resp.status_code == 200
            created = create_resp.json()
            assert created["success"] is True
            task_id = created["task"]["id"]

            final_task = None
            for _ in range(100):
                detail_resp = await http_client.get(f"/api/downloads/{task_id}")
                assert detail_resp.status_code == 200
                detail = detail_resp.json()
                if detail["state"] in {"completed", "failed", "cancelled"}:
                    final_task = detail
                    break
                await asyncio.sleep(0.05)

            assert final_task is not None
            assert final_task["state"] == "completed", final_task
            assert final_task["anime_name"] == "Frieren"
            assert final_task["season"] == 1
            assert final_task["episode"] == 1
            assert final_task["fansub"] == "Lilith-Raws"
            assert final_task["quality"] == "1080p"
            assert final_task["final_path"] == (
                "/PikPak/Anime/Frieren/Season 1/"
                "Frieren S01E01 Lilith-Raws 1080p 繁.mkv"
            )
            assert len(fake_client.offline_calls) == 1
            assert fake_client.offline_calls[0]["tool"] == "PikPak"
            assert fake_client.offline_calls[0]["urls"] == [magnet_url]
            assert (
                "Frieren S01E01 Lilith-Raws 1080p 繁.mkv"
                in fake_client._dirs["/PikPak/Anime/Frieren/Season 1"]
            )
            feed_fetch_spy.assert_not_awaited()
    finally:
        await assembly.runtime.stop()


@pytest.mark.asyncio
async def test_pikpak_download_auto_copies_to_target_and_deletes_source_end_to_end(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
config_version = 2

[rss]
urls = []

[downloader]
download_path = "/PikPak/Anime"
rename_format = "{anime_name} S{season:02d}E{episode:02d} {fansub} {quality} {languages}"

[downloader.openlist]
url = "http://127.0.0.1:5244"
token = "test-token"
offline_download_tool = "PikPak"
copy_target_paths = ["/115/Anime"]
delete_source_after_copy = true

[metadata]
pipeline = ["regex"]
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    monkeypatch.setattr(config_loader, "_config_instance", None)
    monkeypatch.setattr(BackendApiService, "_instance", None)

    raw_filename = "[Lilith-Raws] Frieren - 01 [1080p][CHT].mkv"
    fake_client = _FakePikPakOpenListClient(raw_filename)
    original_bundle_factory = backend_module._create_openlist_backend_bundle

    def _patched_bundle(cfg, *, client=None):
        bundle = original_bundle_factory(cfg, client=client or fake_client)
        bundle.downloader._sleep = AsyncMock()
        bundle.organizer._storage._sleep = AsyncMock()
        bundle.organizer._storage._cache_refresh_seconds = 0
        bundle.organizer._post_copy._sleep = AsyncMock()
        bundle.organizer._post_copy._poll_interval_seconds = 0
        return bundle

    monkeypatch.setattr(
        backend_module, "_create_openlist_backend_bundle", _patched_bundle
    )

    config, settings = _load_runtime_config()
    LegacyMigrationRunner().run()
    assembly = await _compose_runtime(config, settings)

    await backend_module._check_download_backend_health(assembly)
    await assembly.runtime.start()
    BackendApiService.init(assembly.application)
    app = create_app()

    magnet_url = "magnet:?xt=urn:btih:fedcba9876543210fedcba9876543210fedcba98"
    release_title = "[Lilith-Raws] Frieren - 01 [1080p][CHT]"
    expected_name = "Frieren S01E01 Lilith-Raws 1080p 繁.mkv"

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as http_client:
            create_resp = await http_client.post(
                "/api/downloads",
                json={"download_url": magnet_url, "title": release_title},
            )
            assert create_resp.status_code == 200
            task_id = create_resp.json()["task"]["id"]

            final_task = None
            for _ in range(100):
                detail_resp = await http_client.get(f"/api/downloads/{task_id}")
                assert detail_resp.status_code == 200
                detail = detail_resp.json()
                if detail["state"] in {"completed", "failed", "cancelled"}:
                    final_task = detail
                    break
                await asyncio.sleep(0.05)

            assert final_task is not None
            assert final_task["state"] == "completed", final_task
            assert (
                final_task["final_path"]
                == f"/115/Anime/Frieren/Season 1/{expected_name}"
            )
            assert len(fake_client.copy_calls) == 1
            assert fake_client.copy_calls[0] == {
                "src_dir": "/PikPak/Anime/Frieren/Season 1",
                "dst_dir": "/115/Anime/Frieren/Season 1",
                "filenames": [expected_name],
                "overwrite": False,
            }
            assert expected_name in fake_client._dirs["/115/Anime/Frieren/Season 1"]
            assert (
                expected_name not in fake_client._dirs["/PikPak/Anime/Frieren/Season 1"]
            )
    finally:
        await assembly.runtime.stop()


@pytest.mark.asyncio
async def test_pikpak_download_aria2_copy_defaults_to_8_threads_end_to_end(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
config_version = 2

[rss]
urls = []

[downloader]
download_path = "/PikPak/Anime"
rename_format = "{anime_name} S{season:02d}E{episode:02d} {fansub} {quality} {languages}"

[downloader.openlist]
url = "http://127.0.0.1:5244"
token = "test-token"
offline_download_tool = "PikPak"
copy_target_paths = ["/115/Anime"]
copy_mode = "aria2"
delete_source_after_copy = true

[metadata]
pipeline = ["regex"]
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("CONFIG_PATH", str(config_path))
    monkeypatch.setattr(config_loader, "_config_instance", None)
    monkeypatch.setattr(BackendApiService, "_instance", None)

    raw_filename = "[Lilith-Raws] Frieren - 01 [1080p][CHT].mkv"
    fake_client = _FakePikPakOpenListClient(raw_filename)
    original_bundle_factory = backend_module._create_openlist_backend_bundle

    def _patched_bundle(cfg, *, client=None):
        bundle = original_bundle_factory(cfg, client=client or fake_client)
        bundle.downloader._sleep = AsyncMock()
        bundle.organizer._storage._sleep = AsyncMock()
        bundle.organizer._storage._cache_refresh_seconds = 0
        bundle.organizer._post_copy._sleep = AsyncMock()
        bundle.organizer._post_copy._poll_interval_seconds = 0
        return bundle

    monkeypatch.setattr(
        backend_module, "_create_openlist_backend_bundle", _patched_bundle
    )

    config, settings = _load_runtime_config()
    LegacyMigrationRunner().run()
    assembly = await _compose_runtime(config, settings)

    await backend_module._check_download_backend_health(assembly)
    await assembly.runtime.start()
    BackendApiService.init(assembly.application)
    app = create_app()

    magnet_url = "magnet:?xt=urn:btih:00112233445566778899aabbccddeeff00112233"
    release_title = "[Lilith-Raws] Frieren - 01 [1080p][CHT]"
    expected_name = "Frieren S01E01 Lilith-Raws 1080p 繁.mkv"

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as http_client:
            create_resp = await http_client.post(
                "/api/downloads",
                json={"download_url": magnet_url, "title": release_title},
            )
            assert create_resp.status_code == 200
            task_id = create_resp.json()["task"]["id"]

            final_task = None
            for _ in range(100):
                detail_resp = await http_client.get(f"/api/downloads/{task_id}")
                assert detail_resp.status_code == 200
                detail = detail_resp.json()
                if detail["state"] in {"completed", "failed", "cancelled"}:
                    final_task = detail
                    break
                await asyncio.sleep(0.05)

            assert final_task is not None
            assert final_task["state"] == "completed", final_task
            assert (
                final_task["final_path"]
                == f"/115/Anime/Frieren/Season 1/{expected_name}"
            )
            assert fake_client.copy_calls == []
            assert [call["tool"] for call in fake_client.offline_calls] == [
                "PikPak",
                "aria2",
            ]
            assert fake_client.aria2_rpc_calls == [
                {
                    "rpc_url": "http://aria2:6800/jsonrpc",
                    "options": {
                        "split": "8",
                        "max-connection-per-server": "8",
                        "min-split-size": "1M",
                    },
                    "secret": "smoke-secret",
                }
            ]
            assert expected_name in fake_client._dirs["/115/Anime/Frieren/Season 1"]
            assert fake_client._dirs["/115/Anime/.oani-copy-tmp"] == {}
            assert (
                expected_name not in fake_client._dirs["/PikPak/Anime/Frieren/Season 1"]
            )
    finally:
        await assembly.runtime.stop()
