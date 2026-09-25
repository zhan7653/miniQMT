"""Collection, normalization, aggregation, and rendering service."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from fundlab.agent.opinion.collectors import OpinionCollector, OpinionCollectorError, ResponsesWebSearchCollector, RssCollector, ZhihuSearchCollector
from fundlab.agent.opinion.models import OpinionItem, OpinionSnapshot
from fundlab.agent.opinion.repository import OpinionRepository
from fundlab.common.atomic_files import publish_immutable_bytes
from fundlab.common.canonical import canonical_json, stable_digest


class OpinionService:
    def __init__(self, settings, *, market=None, collectors: Mapping[str, OpinionCollector] | None = None):
        self.settings = settings
        self.market = market
        opinion = settings.agent.opinion
        self.repository = OpinionRepository(opinion.root)
        paths = getattr(settings, "paths", None)
        report_base = getattr(paths, "report_root", opinion.root)
        self.report_root = Path(report_base).resolve() / "opinion"
        configured = collectors or self._configured_collectors(opinion)
        self.collectors = dict(configured)
        self.max_items = opinion.max_items
        self.max_excerpt_chars = opinion.max_excerpt_chars
        self.max_detail_chars = opinion.max_detail_chars

    def _configured_collectors(self, opinion):
        result: dict[str, OpinionCollector] = {}
        enabled = set(opinion.providers)
        if "zhihu" in enabled:
            result["zhihu"] = ZhihuSearchCollector(base_url=opinion.zhihu_base_url, token_env=opinion.zhihu_token_env)
        if "rss" in enabled and opinion.rss_feeds:
            result["rss"] = RssCollector(tuple(opinion.rss_feeds))
        if "web" in enabled and self.settings.agent.llm.base_url:
            result["web"] = ResponsesWebSearchCollector(
                base_url=self.settings.agent.llm.base_url,
                model=self.settings.agent.research_profiles.get('opinion-research', {}).get('model', self.settings.agent.llm.model),
                api_key_env=self.settings.agent.llm.api_key_env,
                tool_type=self.settings.agent.llm.web_tool,
            )
        return result

    def collect(self, *, as_of: date, queries: Sequence[str], providers: Sequence[str] | None = None, dry_run: bool = False, limit: int | None = None, refresh: bool = False) -> dict[str, Any]:
        if not queries:
            raise ValueError("at least one opinion query is required")
        query_list = tuple(dict.fromkeys(str(item).strip() for item in queries if str(item).strip()))
        if not query_list:
            raise ValueError("opinion queries cannot be empty")
        selected = tuple(providers or self.collectors)
        if not refresh and not dry_run:
            existing = self.repository.load(as_of=as_of.isoformat())
            if existing is not None and existing.get('quality') == 'ready' and tuple(existing.get("queries", ())) == query_list and tuple(existing.get("providers", ())) == selected:
                path = self.repository.snapshot_path(as_of.isoformat(), str(existing["snapshot_id"]))
                return {"status": "ok", "reused": True, "snapshot": existing, "snapshot_path": str(path), "markdown_path": str(path.with_suffix(".md"))}
        errors: list[dict[str, Any]] = []
        raw_items: list[Mapping[str, Any]] = []
        for provider in selected:
            collector = self.collectors.get(provider)
            if collector is None:
                errors.append({"provider": provider, "status": "unavailable", "error": "provider is not configured"})
                continue
            for query in query_list:
                try:
                    collected = collector.collect(
                        query, as_of=as_of,
                        limit=min(limit or self.max_items, self.max_items),
                    )
                    raw_items.extend(
                        {**dict(item), "query": str(item.get("query") or query)}
                        for item in collected
                    )
                except OpinionCollectorError as exc:
                    errors.append({"provider": provider, "query": query, "status": "error", "error_type": type(exc).__name__, "error": str(exc)})
        items, details = self._normalize(raw_items, as_of=as_of)
        summaries = self._aggregate(items, as_of=as_of)
        quality = "ready" if items and not errors else "degraded" if items or errors else "no_data"
        snapshot_seed = {"as_of": as_of.isoformat(), "queries": query_list, "providers": selected, "items": [item.summary(max_excerpt_chars=self.max_excerpt_chars) for item in items], "summaries": summaries, "errors": errors}
        snapshot_id = "opinion-" + stable_digest(snapshot_seed)[:24]
        snapshot = OpinionSnapshot(as_of, snapshot_id, datetime.now(timezone.utc), tuple(items), summaries, tuple(selected), query_list, quality, tuple(errors))
        path = None if dry_run else self.repository.save(snapshot, details, raw_items=list(raw_items))
        markdown = render_markdown(snapshot.to_dict())
        markdown_path = None
        if path is not None:
            markdown_path = path.with_suffix(".md")
            publish_immutable_bytes(markdown_path, markdown.encode("utf-8"))
            canonical_dir = self.report_root / as_of.isoformat()
            canonical_dir.mkdir(parents=True, exist_ok=True)
            report_json = canonical_dir / path.name
            report_md = canonical_dir / markdown_path.name
            report_payload = snapshot.to_dict()
            report_bytes = (canonical_json(report_payload) + "\n").encode("utf-8")
            if report_json.exists():
                existing_report = json.loads(report_json.read_text(encoding="utf-8"))
                existing_report.pop("created_at", None)
                report_payload.pop("created_at", None)
                if existing_report != report_payload:
                    raise ValueError(f"Immutable opinion report collision: {report_json}")
            else:
                publish_immutable_bytes(report_json, report_bytes)
            publish_immutable_bytes(report_md, markdown.encode("utf-8"))
        return {"status": "degraded" if errors else "ok", "snapshot": snapshot.to_dict(), "snapshot_path": None if path is None else str(path), "markdown_path": None if markdown_path is None else str(markdown_path)}

    def _normalize(self, raw_items: Sequence[Mapping[str, Any]], *, as_of: date) -> tuple[list[OpinionItem], dict[str, Mapping[str, Any]]]:
        now = datetime.now(timezone.utc)
        found: dict[str, OpinionItem] = {}
        by_url: dict[tuple[str, str], str] = {}
        details: dict[str, Mapping[str, Any]] = {}
        for raw in raw_items:
            source = str(raw.get("source") or "unknown")
            url = str(raw.get("url") or "").strip()
            title = str(raw.get("title") or "").strip()
            if not source or not url or not title:
                continue
            published = _parse_datetime(raw.get("published_at"))
            if published is not None and (
                published.date() > as_of
                or published.date() < as_of - timedelta(days=self.settings.agent.opinion.max_age_days)
            ):
                continue
            excerpt = str(raw.get("excerpt") or raw.get("text") or "").strip()
            instrument_ids = self._match_instruments(
                f"{raw.get('query') or ''} {title} {excerpt}"
            )
            dedupe = hashlib.sha256(f"{title}\n{excerpt}".encode("utf-8")).hexdigest()
            url_key = (source, url.split("?", 1)[0].rstrip("/"))
            dedupe = by_url.get(url_key, dedupe)
            detail_ref = dedupe[:32]
            item = OpinionItem(source, str(raw.get("source_item_id") or url), url, title, excerpt[:self.max_detail_chars], published, now, str(raw.get("query") or ""), str(raw.get("author_ref")) if raw.get("author_ref") is not None else None, raw.get("engagement") if isinstance(raw.get("engagement"), Mapping) else {}, instrument_ids, _stance(title + " " + excerpt), _topics(title + " " + excerpt), "official" if source in {"exchange", "cninfo"} else "public", dedupe, detail_ref, bool(excerpt), len(excerpt) > self.max_detail_chars, (source,))
            item = OpinionItem(**{**item.__dict__, 'stance': 'unknown',
                                  'analysis': dict(raw.get('analysis') or {})})
            existing = found.get(dedupe)
            if existing is None:
                found[dedupe] = item
                by_url[url_key] = dedupe
                details[detail_ref] = {"schema_version": "opinion_detail.v1", "source": source, "source_item_id": item.source_item_id, "url": url, "title": title, "text": excerpt[: self.max_detail_chars], "published_at": None if published is None else published.isoformat(), "retrieved_at": now.isoformat(), 'analysis': dict(item.analysis)}
            elif existing.source == source and len(item.excerpt) > len(existing.excerpt):
                found[dedupe] = OpinionItem(**{
                    **existing.__dict__,
                    "excerpt": item.excerpt,
                    "detail_available": item.detail_available,
                    "truncated": item.truncated,
                })
            elif existing.source != source:
                found[dedupe] = OpinionItem(**{
                    **existing.__dict__,
                    "topics": tuple(sorted(set(existing.topics) | set(item.topics))),
                    "source_set": tuple(sorted(set(existing.source_set or (existing.source,)) | {source})),
                })
        return list(found.values())[: self.max_items], details

    def _match_instruments(self, text: str) -> tuple[str, ...]:
        if self.market is None:
            return tuple(sorted(set(re.findall(r"\b\d{6}\.(?:SH|SZ)\b", text.upper()))))
        matches = []
        folded = text.casefold()
        for instrument in self.market.instruments():
            if instrument.instrument_id.casefold() in folded or instrument.local_code.casefold() in folded or instrument.name.casefold() in folded:
                matches.append(instrument.instrument_id)
        return tuple(sorted(set(matches)))

    @staticmethod
    def _aggregate(items: Sequence[OpinionItem], *, as_of: date) -> dict[str, Mapping[str, Any]]:
        groups: dict[str, list[OpinionItem]] = defaultdict(list)
        for item in items:
            for instrument_id in item.instrument_ids:
                groups[instrument_id].append(item)
        result = {}
        for instrument_id, rows in groups.items():
            stances = Counter(item.stance for item in rows)
            classified = stances.get('bullish', 0) + stances.get('bearish', 0)
            authors = {item.author_ref for item in rows if item.author_ref}
            result[instrument_id] = {"as_of": as_of.isoformat(), "mention_count": len(rows), "independent_authors": len(authors) if authors else None, "stance_counts": dict(stances), "bullish_ratio": round(stances.get("bullish", 0) / classified, 4) if classified else None, "bearish_ratio": round(stances.get("bearish", 0) / classified, 4) if classified else None, "disagreement": round(1 - abs(stances.get("bullish", 0) - stances.get("bearish", 0)) / classified, 4) if classified else None, "topics": sorted({topic for item in rows for topic in item.topics})[:12], "quality": "mixed" if len({item.source for item in rows}) > 1 else "single_source"}
        return result

    def summary(self, *, as_of: date, instrument_id: str | None = None, snapshot_id: str | None = None, limit: int = 20) -> dict[str, Any]:
        value = self.repository.load(as_of=as_of.isoformat(), snapshot_id=snapshot_id)
        if value is None:
            return {"status": "no_snapshot", "as_of": as_of.isoformat(), "items": [], "summaries": {}}
        if instrument_id:
            value = {**value, "items": [item for item in value.get("items", []) if instrument_id in item.get("instrument_ids", [])], "summaries": {instrument_id: value.get("summaries", {}).get(instrument_id, {})}}
        value["items"] = value.get("items", [])[: max(1, min(limit, 100))]
        value.pop("details", None)
        return {"status": "ok", "detail_level": "summary", **value}

    def detail(self, *, as_of: date, detail_ref: str, snapshot_id: str | None = None) -> dict[str, Any]:
        value = self.repository.detail(as_of=as_of.isoformat(), detail_ref=detail_ref, snapshot_id=snapshot_id)
        return {"status": "ok", "detail_level": "detail", "as_of": as_of.isoformat(), "detail": value} if value else {"status": "not_found", "as_of": as_of.isoformat(), "detail_ref": detail_ref}

    def search(self, *, query: str, as_of: date, source: str | None = None, limit: int = 20) -> dict[str, Any]:
        query = str(query).strip().casefold()
        if not query:
            raise ValueError("opinion search query cannot be empty")
        found: list[dict[str, Any]] = []
        for info in self.repository.list_snapshots(limit=500):
            snapshot_as_of = info.get("as_of")
            if not isinstance(snapshot_as_of, str) or snapshot_as_of > as_of.isoformat():
                continue
            snapshot = self.repository.load(as_of=snapshot_as_of, snapshot_id=info.get("snapshot_id"))
            if not snapshot:
                continue
            for item in snapshot.get("items", []):
                if source and item.get("source") != source:
                    continue
                haystack = f"{item.get('title', '')} {item.get('excerpt', '')}".casefold()
                if query in haystack:
                    found.append({"as_of": snapshot_as_of, "snapshot_id": snapshot.get("snapshot_id"), **item})
                    if len(found) >= max(1, min(limit, 100)):
                        return {"status": "ok", "detail_level": "summary", "query": query, "as_of": as_of.isoformat(), "items": found}
        return {"status": "ok", "detail_level": "summary", "query": query, "as_of": as_of.isoformat(), "items": found}

    def compare(self, *, instrument_id: str, as_of: date, window: int = 30) -> dict[str, Any]:
        days = []
        for item in self.repository.list_snapshots(limit=500):
            if item.get("as_of") and item["as_of"] <= as_of.isoformat() and item["as_of"] >= (as_of.fromordinal(as_of.toordinal() - max(1, window))).isoformat():
                snapshot = self.repository.load(as_of=item["as_of"], snapshot_id=item.get("snapshot_id"))
                if snapshot:
                    days.append(snapshot.get("summaries", {}).get(instrument_id, {"as_of": item["as_of"], "mention_count": 0}))
        return {"status": "ok", "instrument_id": instrument_id, "as_of": as_of.isoformat(), "window_days": window, "points": sorted(days, key=lambda row: row.get("as_of", ""))}


def render_markdown(snapshot: Mapping[str, Any]) -> str:
    lines = [f"# 舆论研究快照 {snapshot.get('as_of')}", "", f"- snapshot_id: `{snapshot.get('snapshot_id')}`", f"- quality: **{snapshot.get('quality')}**", f"- providers: {', '.join(snapshot.get('providers', [])) or 'none'}", "", "## 标的摘要", ""]
    summaries = snapshot.get("summaries", {})
    if not summaries:
        lines.append("暂无可关联标的的舆论摘要。")
    for instrument_id, summary in summaries.items():
        lines.extend([f"### {instrument_id}", f"- 讨论量：{summary.get('mention_count', 0)}", f"- 独立作者：{summary.get('independent_authors', 0)}", f"- 多空分歧：{summary.get('disagreement', 0)}", f"- 主题：{', '.join(summary.get('topics', [])) or '无'}", ""])
    lines.extend(["## 内容摘要", ""])
    for item in snapshot.get("items", []):
        lines.append(f"- [{item.get('title')}]({item.get('url')}) — {item.get('excerpt', '')} (`{item.get('detail_ref')}`)")
    lines.append("")
    return "\n".join(lines)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(str(value))
        except (TypeError, ValueError, IndexError):
            return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _stance(text: str) -> str:
    if any(word in text for word in ("上涨", "看多", "增持", "利好", "突破", "景气")):
        return "bullish"
    if any(word in text for word in ("下跌", "看空", "减持", "利空", "暴雷", "风险")):
        return "bearish"
    if text.strip():
        return "neutral"
    return "unknown"


def _topics(text: str) -> tuple[str, ...]:
    vocabulary = ("分红", "股息", "业绩", "估值", "政策", "回撤", "流动性", "风险", "盈利", "行业")
    return tuple(item for item in vocabulary if item in text)
