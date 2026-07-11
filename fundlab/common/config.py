from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = Path("config/base.yaml")
CONFIG_ENV_VAR = "FUNDLAB_CONFIG_PATH"


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    configured = path if path is not None else os.environ.get(CONFIG_ENV_VAR, os.environ.get("FUNDLAB_CONFIG", DEFAULT_CONFIG_PATH))
    config_path = Path(configured).expanduser().resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Config must be a mapping: {config_path}")

    data["_config_path"] = config_path
    data["_config_dir"] = config_path.parent
    return data


def get_path(config: dict[str, Any], key: str) -> Path:
    try:
        value = config["paths"][key]
    except KeyError as exc:
        raise KeyError(f"Missing config path: paths.{key}") from exc

    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    config_dir = Path(config.get("_config_dir", Path.cwd()))
    return (config_dir / candidate).resolve()


def resolve_config_path(config: dict[str, Any], value: str | Path) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (Path(config.get("_config_dir", Path.cwd())) / candidate).resolve()


def validate_platform_config(config: dict[str, Any]) -> None:
    timezone = config.get("platform", {}).get("timezone")
    if timezone != "Asia/Hong_Kong":
        raise ValueError("platform.timezone must be Asia/Hong_Kong")
    providers = config.get("providers", {})
    enabled = providers.get("enabled", [])
    if enabled != ["xtquant"]:
        raise ValueError("V1 requires exactly one explicitly enabled provider: xtquant")
    if providers.get("fallback") is not None:
        raise ValueError("Automatic provider fallback is forbidden")
    required_paths = ("v2_catalog", "v2_raw_root", "v2_staging_root", "v2_published_root", "v2_report_root")
    for key in required_paths:
        get_path(config, key)
    universe = config.get("reviewed_universe", {})
    if not universe.get("config_path"):
        raise ValueError("reviewed_universe.config_path is required")
    if not universe.get("benchmark_symbols"):
        raise ValueError("reviewed_universe.benchmark_symbols cannot be empty")
    if int(universe.get("feature_lookback_days", 0)) < 120:
        raise ValueError("reviewed_universe.feature_lookback_days must be at least 120")
    from fundlab.data.platform.universe import load_universe_snapshot

    snapshot = load_universe_snapshot(resolve_config_path(config, universe["config_path"]))
    if tuple(sorted(universe["benchmark_symbols"])) != snapshot.benchmarks:
        raise ValueError("Configured benchmarks must match the reviewed universe snapshot")
