import asyncio

import pytest

from openlist_ani.application.feed_scheduler import FeedScheduler
from openlist_ani.application.ports import FeedFetchResult
from openlist_ani.domain import ReleaseCandidate


class _FeedState:
    def __init__(self):
        self.successes = []
        self.failures = []

    async def cache_headers(self, _url):
        return '"old"', "yesterday"

    async def mark_feed_success(self, url, interval, etag, modified):
        self.successes.append((url, interval, etag, modified))

    async def mark_feed_failure(self, url, error):
        self.failures.append((url, error))


class _Jobs:
    def __init__(self):
        self.items = []

    async def add_candidate(self, candidate):
        self.items.append(candidate)
        return object()


class _Adapter:
    name = "test"

    def __init__(self, fail=False):
        self.fail = fail
        self.headers = None

    async def fetch(self, url, *, etag=None, last_modified=None):
        self.headers = (etag, last_modified)
        if self.fail:
            raise TimeoutError("isolated timeout")
        return FeedFetchResult(
            [
                ReleaseCandidate.create(
                    source_name=self.name,
                    source_url=url,
                    title="Example 01",
                    download_url="magnet:example",
                )
            ],
            etag='"new"',
            last_modified="today",
        )


class _Registry:
    def __init__(self, adapters):
        self.adapters = adapters

    def feed_for(self, url):
        return self.adapters[url]


@pytest.mark.asyncio
async def test_feed_cache_headers_and_failures_are_isolated_per_subscription():
    ok_url = "https://ok.test/rss"
    bad_url = "https://bad.test/rss"
    ok = _Adapter()
    bad = _Adapter(fail=True)
    jobs = _Jobs()
    state = _FeedState()
    scheduler = FeedScheduler(
        registry=_Registry({ok_url: ok, bad_url: bad}),
        jobs=jobs,
        feed_state=state,
        get_urls=lambda: [ok_url, bad_url],
        interval_seconds=300,
        jobs_available=asyncio.Event(),
    )

    await asyncio.gather(scheduler._fetch_one(ok_url), scheduler._fetch_one(bad_url))

    assert ok.headers == ('"old"', "yesterday")
    assert len(jobs.items) == 1
    assert state.successes == [(ok_url, 300, '"new"', "today")]
    assert state.failures == [(bad_url, "isolated timeout")]


@pytest.mark.asyncio
async def test_empty_rss_urls_skips_feed_fetching_until_url_is_added():
    ok_url = "https://ok.test/rss"
    adapter = _Adapter()
    jobs = _Jobs()
    configured_urls: list[str] = []

    class _TrackingFeedState(_FeedState):
        def __init__(self):
            super().__init__()
            self.synced: list[list[str]] = []
            self.list_due_calls = 0
            self.synced_event = asyncio.Event()
            self.fetched_event = asyncio.Event()

        async def sync_urls(self, urls: list[str]) -> None:
            self.synced.append(list(urls))
            self.synced_event.set()

        async def list_due(self, limit: int) -> list[str]:
            self.list_due_calls += 1
            return list(configured_urls)[:limit]

        async def mark_feed_success(self, url, interval, etag, modified):
            await super().mark_feed_success(url, interval, etag, modified)
            self.fetched_event.set()

    class _TrackingRegistry(_Registry):
        def __init__(self, adapters):
            super().__init__(adapters)
            self.feed_for_calls: list[str] = []

        def feed_for(self, url):
            self.feed_for_calls.append(url)
            return super().feed_for(url)

    state = _TrackingFeedState()
    registry = _TrackingRegistry({ok_url: adapter})
    scheduler = FeedScheduler(
        registry=registry,
        jobs=jobs,
        feed_state=state,
        get_urls=lambda: list(configured_urls),
        interval_seconds=300,
        jobs_available=asyncio.Event(),
    )

    task = asyncio.create_task(scheduler.run())
    try:
        await asyncio.wait_for(state.synced_event.wait(), timeout=2.0)
        scheduler.wake()
        await asyncio.sleep(0.05)

        assert state.synced == [[]]
        assert state.list_due_calls == 0
        assert registry.feed_for_calls == []
        assert jobs.items == []

        configured_urls.append(ok_url)
        scheduler.wake()
        await asyncio.wait_for(state.fetched_event.wait(), timeout=2.0)

        assert state.synced == [[], [ok_url]]
        assert state.list_due_calls >= 1
        assert registry.feed_for_calls == [ok_url]
        assert len(jobs.items) == 1
        assert state.successes == [(ok_url, 300, '"new"', "today")]
    finally:
        await scheduler.stop()
        await asyncio.wait_for(task, timeout=2.0)
