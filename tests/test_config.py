"""Gating tests for user configuration behavior and failure handling."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

import pytest
from pydantic import ValidationError

from openlist_ani.adapters.configuration import (
    ConfigManager,
    ConfigValidator,
    compile_core_settings,
    validate_core_settings,
)
from openlist_ani.adapters.configuration.models import (
    BotConfig,
    DownloaderConfig,
    MetadataFilterConfig,
    NotificationConfig,
    OpenListConfig,
    PriorityConfig,
    RSSConfig,
    UserConfig,
)


def _valid_config(**overrides) -> UserConfig:
    payload = {
        "rss": {"urls": ["https://example.test/feed.xml"]},
        "downloader": {
            "openlist": {
                "url": "http://localhost:5244",
                "token": "token",
            }
        },
    }
    payload.update(overrides)
    return UserConfig.model_validate(payload)


def test_minimal_user_config_is_ready_to_run():
    config = _valid_config()

    assert config.metadata_provider_names() == ("regex", "tmdb")
    assert config.rss.torrent_to_magnet is False
    assert ConfigValidator(config).validate() is True


def test_core_settings_select_one_bound_download_backend():
    settings = compile_core_settings(_valid_config())

    assert settings.download_backend == "openlist"
    assert not hasattr(settings, "downloader")
    assert not hasattr(settings, "organizer")
    invalid_settings = replace(settings, download_backend=" ")

    with pytest.raises(ValueError, match="A download backend is required"):
        validate_core_settings(invalid_settings)


def test_torrent_to_magnet_can_be_enabled_in_rss_config():
    config = _valid_config(
        rss={
            "urls": ["https://example.test/feed.xml"],
            "torrent_to_magnet": True,
        }
    )

    assert config.rss.torrent_to_magnet is True


def test_metadata_pipeline_rejects_unrunnable_configurations():
    for pipeline in (["tmdb"], ["tmdb", "ai"], ["regex", "unknown"]):
        with pytest.raises(ValidationError):
            UserConfig.model_validate({"metadata": {"pipeline": pipeline}})


def test_config_file_round_trip_and_rss_update(tmp_path):
    config_path = tmp_path / "config.toml"
    manager = ConfigManager(config_path)

    manager.add_rss_url("https://example.test/feed.xml")
    manager.add_rss_url("https://example.test/feed.xml")

    reloaded = ConfigManager(config_path)
    assert reloaded.rss.urls == ["https://example.test/feed.xml"]


def test_corrupt_config_fails_closed(tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text("not valid [[[ toml", encoding="utf-8")

    manager = ConfigManager(config_path)

    assert manager.load_failed is True
    assert ConfigValidator(manager.data, manager.load_failed).validate() is False


@pytest.mark.parametrize(
    "payload",
    [
        {
            "rss": {"urls": []},
            "downloader": {
                "download_path": "/PikPak/Anime",
                "openlist": {
                    "url": "http://localhost:5244",
                    "token": "token",
                    "offline_download_tool": "PikPak",
                },
            },
        },
        {
            "downloader": {
                "openlist": {
                    "url": "http://localhost:5244",
                    "token": "token",
                }
            }
        },
    ],
    ids=["explicit-empty-rss-urls", "omitted-rss-section"],
)
def test_empty_rss_urls_config_is_ready_to_run(payload):
    config = UserConfig.model_validate(payload)

    assert config.rss.urls == []
    assert ConfigValidator(config).validate() is True
    settings = compile_core_settings(config)
    validate_core_settings(settings)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"rss": {"urls": []}},
        {"rss": {"urls": ["https://example.test/feed.xml"]}},
        {
            "rss": {"urls": ["https://example.test/feed.xml"]},
            "downloader": {"openlist": {"url": "", "token": "token"}},
        },
        {
            "rss": {"urls": [""]},
            "downloader": {
                "openlist": {"url": "http://localhost:5244", "token": "token"}
            },
        },
        {
            "rss": {"urls": ["https://example.test/feed.xml", "   "]},
            "downloader": {
                "openlist": {"url": "http://localhost:5244", "token": "token"}
            },
        },
        {
            "downloader": {
                "download_path": "/PikPak/Anime",
                "openlist": {
                    "url": "http://localhost:5244",
                    "token": "token",
                    "copy_target_paths": [""],
                },
            }
        },
        {
            "downloader": {
                "download_path": "/PikPak/Anime",
                "openlist": {
                    "url": "http://localhost:5244",
                    "token": "token",
                    "copy_target_paths": ["115/Anime"],
                },
            }
        },
        {
            "downloader": {
                "download_path": "/PikPak/Anime",
                "openlist": {
                    "url": "http://localhost:5244",
                    "token": "token",
                    "copy_target_paths": ["/115/../Anime"],
                },
            }
        },
        {
            "downloader": {
                "download_path": "/PikPak/Anime",
                "openlist": {
                    "url": "http://localhost:5244",
                    "token": "token",
                    "copy_target_paths": ["/PikPak/Anime/"],
                },
            }
        },
        {
            "downloader": {
                "download_path": "/PikPak/Anime",
                "openlist": {
                    "url": "http://localhost:5244",
                    "token": "token",
                    "copy_target_paths": ["//PikPak/Anime"],
                },
            }
        },
    ],
    ids=[
        "empty-config-missing-token",
        "empty-rss-missing-token",
        "missing-token",
        "missing-url",
        "blank-rss-url",
        "mixed-blank-rss-url",
        "blank-copy-target-path",
        "relative-copy-target-path",
        "traversal-copy-target-path",
        "copy-target-equals-download-path",
        "copy-target-double-slash-equals-download-path",
    ],
)
def test_missing_required_runtime_settings_are_rejected(payload):
    assert ConfigValidator(UserConfig.model_validate(payload)).validate() is False


def test_openlist_copy_target_paths_config_is_validated():
    config = UserConfig.model_validate(
        {
            "downloader": {
                "download_path": "/PikPak/Anime",
                "openlist": {
                    "url": "http://localhost:5244",
                    "token": "token",
                    "offline_download_tool": "PikPak",
                    "copy_target_paths": ["/115/Anime", "/Local/Anime"],
                },
            }
        }
    )

    assert config.downloader.openlist.copy_target_paths == [
        "/115/Anime",
        "/Local/Anime",
    ]
    assert config.downloader.openlist.delete_source_after_copy is True
    assert ConfigValidator(config).validate() is True


@pytest.mark.parametrize(
    "overrides",
    [
        {"notification": {"enabled": True, "bots": []}},
        {
            "notification": {
                "enabled": True,
                "bots": [{"type": "telegram", "enabled": False}],
            }
        },
        {"assistant": {"enabled": True}},
    ],
    ids=[
        "notification-without-channel",
        "notification-with-disabled-channel",
        "assistant-without-frontend",
    ],
)
def test_enabled_optional_features_require_a_usable_configuration(overrides):
    assert ConfigValidator(_valid_config(**overrides)).validate() is False


@pytest.mark.parametrize(
    "build",
    [
        lambda: OpenListConfig(offline_download_tool=""),
        lambda: OpenListConfig.model_validate({"copy_target_paths": "/115/Anime"}),
        lambda: DownloaderConfig(rename_format="{unsupported}"),
        lambda: MetadataFilterConfig(exclude_patterns=["[invalid"]),
        lambda: NotificationConfig(batch_interval=-1),
        lambda: UserConfig.model_validate({"metdata": {"pipeline": ["regex"]}}),
        lambda: BotConfig(type="telegram", config={"bot_tokn": "typo"}),
        lambda: RSSConfig(interval_time=0),
        lambda: RSSConfig(interval_time=-10),
        lambda: RSSConfig.model_validate({"urls": "https://example.test/feed.xml"}),
        lambda: RSSConfig.model_validate({"unknown_key": True}),
        lambda: PriorityConfig(field_order=["fansub", "fansub"]),
    ],
    ids=[
        "empty-tool",
        "non-list-copy-target-paths",
        "bad-rename-field",
        "bad-filter-regex",
        "negative-batch",
        "unknown-config-key",
        "unknown-notification-key",
        "zero-rss-interval",
        "negative-rss-interval",
        "non-list-rss-urls",
        "unknown-rss-key",
        "duplicate-rss-priority-field",
    ],
)
def test_invalid_user_input_is_rejected_by_the_config_model(
    build: Callable[[], object],
):
    with pytest.raises(ValidationError):
        build()
