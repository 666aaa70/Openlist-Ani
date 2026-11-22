"""Gating tests for OpenList startup validation."""

from unittest.mock import AsyncMock

import pytest

from openlist_ani.adapters.download_backends.openlist import OpenListHealthCheck


async def _validate(client: AsyncMock, tool: str = "qBittorrent") -> bool:
    return await OpenListHealthCheck(
        client=client,
        base_url="http://localhost:5244",
        offline_download_tool=tool,
    ).validate()


@pytest.mark.asyncio
async def test_unhealthy_openlist_blocks_startup():
    client = AsyncMock()
    client.is_healthy.return_value = False

    assert await _validate(client) is False


@pytest.mark.asyncio
async def test_missing_download_tool_blocks_startup():
    client = AsyncMock()
    client.is_healthy.return_value = True
    client.get_offline_download_tools.return_value = ["aria2"]

    assert await _validate(client) is False


@pytest.mark.asyncio
async def test_available_download_tool_allows_startup():
    client = AsyncMock()
    client.is_healthy.return_value = True
    client.get_offline_download_tools.return_value = [
        {"name": "qBittorrent"},
        {"name": "aria2"},
    ]

    assert await _validate(client, "QBittorrent") is True


@pytest.mark.asyncio
async def test_extra_required_aria2_tool_is_checked_when_configured():
    client = AsyncMock()
    client.is_healthy.return_value = True
    client.get_offline_download_tools.return_value = ["PikPak"]

    missing_aria2 = await OpenListHealthCheck(
        client=client,
        base_url="http://localhost:5244",
        offline_download_tool="PikPak",
        extra_required_tools=("aria2",),
    ).validate()
    assert missing_aria2 is False

    client.get_offline_download_tools.return_value = ["PikPak", "aria2"]
    with_aria2 = await OpenListHealthCheck(
        client=client,
        base_url="http://localhost:5244",
        offline_download_tool="PikPak",
        extra_required_tools=("aria2",),
    ).validate()
    assert with_aria2 is True
