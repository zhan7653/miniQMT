"""Read-only research tools for agents."""

from datetime import date, timedelta
from typing import Any, Mapping, Sequence
import math
from fundlab.marketdata.contracts import PriceMode
from fundlab.agent.features import build_crisis_features


def _clean(x):
    if hasattr(x, "item"):
        x = x.item()
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    if isinstance(x, (date,)):
        return x.isoformat()
    if isinstance(x, dict):
        return {str(k): _clean(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_clean(v) for v in x]
    return x


def build_research_tools(
    market,
    as_of: date,
    *,
    portfolio: Mapping = {},
    library=None,
    memory: Sequence[Mapping] = (),
    extra_tools: Mapping[str, Mapping[str, Any]] | None = None,
    opinion_service=None,
):
    def validate(ids):
        if isinstance(ids, str):
            ids = [ids]
        if (
            not isinstance(ids, (list, tuple))
            or not ids
            or len(ids) > 10
            or any(not isinstance(i, str) for i in ids)
        ):
            raise ValueError("instrument_ids must be 1..10 strings")
        return list(dict.fromkeys(ids))

    def search_instruments(query="", asset_type=None, limit=20):
        if not isinstance(query, str) or not 1 <= limit <= 100:
            raise ValueError("query string and limit 1..100 required")
        aliases = {"equity": "stock", "fund": "etf", "stock": "stock", "etf": "etf"}
        if asset_type is not None:
            asset_type = aliases.get(str(asset_type).lower())
            if asset_type is None:
                raise ValueError("asset_type must be stock, equity, etf, or fund")
        q = query.lower()
        out = []
        for i in market.instruments(
            as_of=as_of, asset_types=[asset_type] if asset_type else None
        ):
            if not q or q in (i.instrument_id + i.local_code + i.name).lower():
                out.append(
                    {
                        "instrument_id": i.instrument_id,
                        "local_code": i.local_code,
                        "name": i.name,
                        "asset_type": str(i.asset_type),
                        "exchange": i.exchange,
                    }
                )
        return {
            "snapshot_id": market.snapshot_id,
            "as_of": as_of.isoformat(),
            "instruments": out[:limit],
        }

    def price_history(instrument_ids, start_date, end_date, price_mode="raw"):
        s, e = date.fromisoformat(start_date), date.fromisoformat(end_date)
        if e > as_of or e < s or (e - s).days > 400:
            raise ValueError("invalid date range: end <= as_of, span <=400 days")
        df = market.bars(
            validate(instrument_ids),
            s,
            e,
            price_mode=PriceMode(price_mode),
            as_of=as_of,
        )
        columns = [
            "instrument_id", "session_date", "open", "high", "low", "close",
            "volume", "amount", "suspended", "is_st", "price_limit_state",
            "source_provider", "source_observation_id",
        ]
        df = df[[column for column in columns if column in df.columns]]
        return {
            "snapshot_id": market.snapshot_id,
            "as_of": as_of.isoformat(),
            "rows": _clean(df.to_dict("records")),
        }

    def dividend_history(instrument_ids, years=12):
        if not 1 <= years <= 12:
            raise ValueError("years must be 1..12")
        df = market.corporate_actions(
            validate(instrument_ids),
            as_of - timedelta(days=366 * years),
            as_of,
            as_of=as_of,
        )
        columns = [
            "action_id", "instrument_id", "action_type", "known_date", "record_date",
            "ex_date", "pay_date", "listing_date", "cash_per_share", "share_ratio",
            "rights_price", "source_provider", "source_observation_id", "source_payload",
        ]
        df = df[[column for column in columns if column in df.columns]]
        return {
            "snapshot_id": market.snapshot_id,
            "as_of": as_of.isoformat(),
            "warning": "Historical dividends are not sustainable forecasts; TTM unknown without evidence.",
            "rows": _clean(df.to_dict("records")),
        }

    def crisis_features(instrument_ids, drawdown_days=252, event_lookback_days=60, confirmation_days=20, volatility_days=60):
        ids = validate(instrument_ids)
        features = build_crisis_features(
            market, ids, as_of=as_of, drawdown_days=drawdown_days,
            event_lookback_days=event_lookback_days,
            confirmation_days=confirmation_days, volatility_days=volatility_days,
        )
        return {
            "snapshot_id": market.snapshot_id, "as_of": as_of.isoformat(),
            "features": _clean({key: value.__dict__ for key, value in features.items()}),
        }

    def empty():
        return {"as_of": as_of.isoformat()}

    tools = {
        "search_instruments": {
            "description": "Search canonical instruments",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "asset_type": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
                "required": ["query", "asset_type", "limit"],
                "additionalProperties": False,
            },
            "callable": search_instruments,
            "write": False,
        },
        "price_history": {
            "description": "Read prices",
            "parameters": {
                "type": "object",
                "properties": {
                    "instrument_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": 10,
                    },
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "price_mode": {"type": "string", "enum": ["raw", "adjusted"]},
                },
                "required": ["instrument_ids", "start_date", "end_date", "price_mode"],
                "additionalProperties": False,
            },
            "callable": price_history,
            "write": False,
        },
        "dividend_history": {
            "description": "Read dividends",
            "parameters": {
                "type": "object",
                "properties": {
                    "instrument_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                        "maxItems": 10,
                    },
                    "years": {"type": "integer", "minimum": 1, "maximum": 12},
                },
                "required": ["instrument_ids", "years"],
                "additionalProperties": False,
            },
            "callable": dividend_history,
            "write": False,
        },
        "crisis_features": {
            "description": "Compute point-in-time drawdown, rebound, confirmation and volatility features. Use this first for every explicitly named ETF or index instrument; it is deterministic canonical evidence, not a forecast.",
            "parameters": {"type": "object", "properties": {"instrument_ids": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 10}, "drawdown_days": {"type": "integer", "minimum": 20, "maximum": 1000}, "event_lookback_days": {"type": "integer", "minimum": 5, "maximum": 400}, "confirmation_days": {"type": "integer", "minimum": 3, "maximum": 120}, "volatility_days": {"type": "integer", "minimum": 5, "maximum": 250}}, "required": ["instrument_ids", "drawdown_days", "event_lookback_days", "confirmation_days", "volatility_days"], "additionalProperties": False},
            "callable": crisis_features,
            "write": False,
        },
        "portfolio_state": {
            "description": "Portfolio",
            "parameters": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            "callable": lambda: _clean(
                {"as_of": as_of.isoformat(), "portfolio": portfolio}
            ),
            "write": False,
        },
        "read_research_memory": {
            "description": "Memory",
            "parameters": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            "callable": lambda: _clean({"as_of": as_of.isoformat(), "memory": memory}),
            "write": False,
        },
        "read_library": {
            "description": "Library",
            "parameters": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            "callable": lambda: _clean(
                {
                    "as_of": as_of.isoformat(),
                    "documents": [
                        d.evidence() for d in (library.context() if library else ())
                    ],
                }
            ),
            "write": False,
        },
    }
    if opinion_service is not None:
        def opinion_summary(instrument_id=None, snapshot_id=None, limit=20):
            return opinion_service.summary(
                as_of=as_of, instrument_id=instrument_id,
                snapshot_id=snapshot_id, limit=limit,
            )

        def opinion_detail(detail_ref, snapshot_id=None):
            return opinion_service.detail(
                as_of=as_of, detail_ref=detail_ref, snapshot_id=snapshot_id,
            )

        def opinion_trend(instrument_id, window=30):
            return opinion_service.compare(
                instrument_id=instrument_id, as_of=as_of, window=window,
            )

        def opinion_search(query, source=None, limit=20):
            return opinion_service.search(
                query=query, as_of=as_of, source=source, limit=limit,
            )

        tools.update({
            "read_opinion_snapshot": {
                "description": "Read compact, point-in-time public-opinion summaries. Detail text is omitted; use read_opinion_detail only when needed.",
                "parameters": {"type": "object", "properties": {
                    "instrument_id": {"type": ["string", "null"]},
                    "snapshot_id": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                }, "required": ["instrument_id", "snapshot_id", "limit"], "additionalProperties": False},
                "callable": opinion_summary, "write": False,
            },
            "read_opinion_detail": {
                "description": "Expand one opinion item after reviewing its summary. Returns only stored provider-permitted detail.",
                "parameters": {"type": "object", "properties": {
                    "detail_ref": {"type": "string", "minLength": 1},
                    "snapshot_id": {"type": ["string", "null"]},
                }, "required": ["detail_ref", "snapshot_id"], "additionalProperties": False},
                "callable": opinion_detail, "write": False,
            },
            "compare_opinion_trend": {
                "description": "Compare daily public-opinion aggregates over a bounded historical window.",
                "parameters": {"type": "object", "properties": {
                    "instrument_id": {"type": "string", "minLength": 1},
                    "window": {"type": "integer", "minimum": 1, "maximum": 180},
                }, "required": ["instrument_id", "window"], "additionalProperties": False},
                "callable": opinion_trend, "write": False,
            },
            "search_opinion": {
                "description": "Search previously published opinion summaries up to the current as_of; returns compact items only.",
                "parameters": {"type": "object", "properties": {
                    "query": {"type": "string", "minLength": 1},
                    "source": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                }, "required": ["query", "source", "limit"], "additionalProperties": False},
                "callable": opinion_search, "write": False,
            },
        })

    for name, spec in (extra_tools or {}).items():
        if not isinstance(spec, Mapping) or spec.get("write") is not False or not callable(spec.get("callable")):
            raise ValueError(f"extra research tool {name!r} must be an explicit read-only mapping")
        tools[str(name)] = dict(spec)
    return tools
