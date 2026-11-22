"""OpenList composition root for the shared durable organizer."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from openlist_ani.application.organization import DurableOrganizationExecutor
from openlist_ani.application.ports import (
    CheckpointCallback,
    DownloadedAsset,
    DownloadManifest,
    DownloadedFile,
    OrganizedAsset,
    OrganizationRequest,
    OrganizationResult,
    OrganizationSidecar,
)
from openlist_ani.domain import DownloadJob

from .client import OpenListClient
from .post_copy import OpenListPostCopyExecutor
from .storage import OpenListStorageOperations


class OpenListOrganizerAdapter:
    backend_name = "openlist"

    def __init__(
        self,
        client: OpenListClient,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        *,
        copy_target_paths: Iterable[str] = (),
        copy_mode: str = "copy",
        copy_threads: int = 8,
        aria2_rpc_url: str = "",
        aria2_rpc_secret: str = "",
        delete_source_after_copy: bool = True,
    ) -> None:
        self._client = client
        self._storage = OpenListStorageOperations(client, sleep)
        self._executor = DurableOrganizationExecutor(self._storage)
        self._post_copy = OpenListPostCopyExecutor(
            client,
            self._storage,
            copy_target_paths=copy_target_paths,
            copy_mode=copy_mode,
            copy_threads=copy_threads,
            aria2_rpc_url=aria2_rpc_url,
            aria2_rpc_secret=aria2_rpc_secret,
            delete_source_after_copy=delete_source_after_copy,
            sleep=sleep,
        )

    async def organize(
        self,
        job: DownloadJob,
        manifest: DownloadManifest | DownloadedAsset,
        requests: tuple[OrganizationRequest, ...] | str,
        checkpoint_callback: CheckpointCallback | None = None,
    ):
        """Organize all requests using the OpenList storage driver.

        The ``DownloadedAsset``/string branch only exists so persisted v1 jobs
        and integrations can finish after an upgrade.  New jobs always use a
        manifest plus a tuple of requests.
        """

        if isinstance(manifest, DownloadedAsset):
            if not isinstance(requests, str):
                raise TypeError("legacy organizer target filename must be a string")
            legacy_asset = manifest
            converted_manifest = DownloadManifest(
                root_path=legacy_asset.directory_path,
                files=(
                    DownloadedFile(legacy_asset.filename),
                    *(
                        DownloadedFile(sidecar.filename)
                        for sidecar in legacy_asset.sidecars
                    ),
                ),
                checkpoint=dict(legacy_asset.checkpoint),
                legacy_materialized=True,
            )
            converted_request = OrganizationRequest(
                item_key="legacy",
                video_relative_path=legacy_asset.filename,
                sidecars=tuple(
                    OrganizationSidecar(sidecar.filename, sidecar.suffix)
                    for sidecar in legacy_asset.sidecars
                ),
                target_directory_path=legacy_asset.directory_path,
                target_filename=requests,
                metadata=job.metadata,
            )
            results = await self._organize_with_post_copy(
                job,
                converted_manifest,
                (converted_request,),
                checkpoint_callback,
            )
            result = results[0]
            if result.state != "completed" or not result.final_path:
                raise RuntimeError(result.error or "Legacy organization failed")
            directory_path, filename = result.final_path.rsplit("/", 1)
            return OrganizedAsset(
                directory_path,
                filename,
                tuple(path.rsplit("/", 1)[-1] for path in result.sidecar_paths),
            )

        if isinstance(requests, str):
            raise TypeError("organization requests must be a tuple")
        return await self._organize_with_post_copy(
            job,
            manifest,
            requests,
            checkpoint_callback,
        )

    async def _organize_with_post_copy(
        self,
        job: DownloadJob,
        manifest: DownloadManifest,
        requests: tuple[OrganizationRequest, ...],
        checkpoint_callback: CheckpointCallback | None,
    ) -> tuple[OrganizationResult, ...]:
        latest_checkpoint: dict[str, Any] = dict(
            job.artifact.get("organization_checkpoint") or {}
        )

        async def track_checkpoint(payload: dict[str, Any]) -> None:
            latest_checkpoint.clear()
            latest_checkpoint.update(payload)
            if checkpoint_callback is not None:
                await checkpoint_callback(dict(latest_checkpoint))

        results = await self._executor.organize(
            job,
            manifest,
            requests,
            track_checkpoint,
        )
        if not self._post_copy.enabled:
            return results

        async def save_post_copy(post_copy_state: dict[str, Any]) -> None:
            latest_checkpoint["post_copy"] = post_copy_state
            if checkpoint_callback is not None:
                await checkpoint_callback(dict(latest_checkpoint))

        return await self._post_copy.execute(
            job,
            results,
            saved_state=latest_checkpoint.get("post_copy"),
            save_state=save_post_copy,
        )
