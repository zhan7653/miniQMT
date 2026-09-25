from datetime import date
from types import SimpleNamespace

from fundlab.agent.opinion import OpinionService
from fundlab.agent.opinion.collectors import OpinionCollector, _extract_items
from fundlab.agent.opinion.service import render_markdown
from fundlab.agent.research_tools import build_research_tools
from fundlab.settings import AgentOpinionSettings


class FakeCollector(OpinionCollector):
    source = "fake"

    def collect(self, query, *, as_of, limit):
        return [
            {
                "source": "fake",
                "source_item_id": "1",
                "url": "https://example.test/1",
                "title": "600000.SH 分红讨论",
                "excerpt": "有人看多但也有风险，风险需要核实。",
                "published_at": "2026-07-15T08:00:00+00:00",
                "author_ref": "author-a",
                "query": query,
            },
            {
                "source": "fake",
                "source_item_id": "future",
                "url": "https://example.test/future",
                "title": "未来内容",
                "excerpt": "上涨",
                "published_at": "2026-07-17T08:00:00+00:00",
            },
        ]


def _settings(tmp_path):
    return SimpleNamespace(agent=SimpleNamespace(opinion=AgentOpinionSettings(
        root=tmp_path / "opinion", providers=("fake",), max_age_days=30,
    )))


def test_opinion_snapshot_is_summary_first_and_filters_future(tmp_path):
    service = OpinionService(_settings(tmp_path), collectors={"fake": FakeCollector()})
    result = service.collect(
        as_of=date(2026, 7, 16), queries=("600000.SH",), providers=("fake",),
    )
    snapshot = result["snapshot"]
    assert snapshot["schema_version"] == "opinion_snapshot.v1"
    assert len(snapshot["items"]) == 1
    item = snapshot["items"][0]
    assert item["detail_available"] is True
    assert "text" not in item
    assert item["detail_ref"]
    assert snapshot["summaries"]["600000.SH"]["mention_count"] == 1
    again = service.collect(
        as_of=date(2026, 7, 16), queries=("600000.SH",), providers=("fake",),
    )
    assert again["snapshot"]["snapshot_id"] == snapshot["snapshot_id"]

    detail = service.detail(as_of=date(2026, 7, 16), detail_ref=item["detail_ref"])
    assert detail["status"] == "ok"
    assert "风险" in detail["detail"]["text"]


def test_opinion_repository_read_tools_are_progressive(tmp_path):
    service = OpinionService(_settings(tmp_path), collectors={"fake": FakeCollector()})
    result = service.collect(
        as_of=date(2026, 7, 16), queries=("600000.SH",), providers=("fake",),
    )
    item = result["snapshot"]["items"][0]
    tools = build_research_tools(
        market=SimpleNamespace(), as_of=date(2026, 7, 16),
        opinion_service=service,
    )
    summary = tools["read_opinion_snapshot"]["callable"](
        instrument_id="600000.SH", snapshot_id=None, limit=20,
    )
    assert summary["detail_level"] == "summary"
    assert "text" not in summary["items"][0]
    expanded = tools["read_opinion_detail"]["callable"](
        detail_ref=item["detail_ref"], snapshot_id=None,
    )
    assert expanded["detail_level"] == "detail"


def test_markdown_is_rendered_from_snapshot_without_new_facts():
    markdown = render_markdown({
        "as_of": "2026-07-16",
        "snapshot_id": "opinion-1",
        "quality": "ready",
        "providers": ["fake"],
        "summaries": {"600000.SH": {"mention_count": 1, "independent_authors": 1, "disagreement": 1.0, "topics": ["分红"]}},
        "items": [{"title": "分红讨论", "url": "https://example.test/1", "excerpt": "摘要", "detail_ref": "a"}],
    })
    assert "分红讨论" in markdown
    assert "未来内容" not in markdown


def test_zhihu_envelope_pascal_case_is_normalized():
    rows = _extract_items({"Code": 0, "Data": {"Items": [{
        "Id": 7,
        "Title": "沪深300 分红讨论",
        "Url": "https://www.zhihu.com/question/7",
        "ContentText": "内容",
        "EditTime": 1784102400,
        "VoteUpCount": 9,
        "CommentCount": 2,
        "Author": {"UrlToken": "author-7"},
    }]}}, source="zhihu", query="沪深300")
    assert rows == [{
        "source": "zhihu",
        "source_item_id": "7",
        "url": "https://www.zhihu.com/question/7",
        "title": "沪深300 分红讨论",
        "excerpt": "内容",
        "published_at": 1784102400,
        "author_ref": "author-7",
        "engagement": {"likes": 9, "comments": 2},
    }]


def test_opinion_settings_have_broad_default_topics():
    settings = AgentOpinionSettings()
    assert "A股 今日 市场 观点" in settings.broad_queries
    assert "基金 今日 市场 观点" in settings.broad_queries


def test_implicit_snapshot_read_prefers_ready_version(tmp_path):
    service = OpinionService(_settings(tmp_path), collectors={"fake": FakeCollector()})
    ready = service.collect(
        as_of=date(2026, 7, 16), queries=("600000.SH",), providers=("fake",),
    )["snapshot"]
    service.collect(
        as_of=date(2026, 7, 16), queries=("600000.SH",), providers=("missing",),
        refresh=True,
    )
    loaded = service.repository.load(as_of="2026-07-16")
    assert loaded["snapshot_id"] == ready["snapshot_id"]
