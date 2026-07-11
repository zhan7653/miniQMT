from __future__ import annotations

from typing import Any


class LegacyDataPortal:
    """Explicit read-only facade for a v1 portal during parallel migration."""

    def __init__(self, portal: Any) -> None:
        object.__setattr__(self, "_portal", portal)

    def __getattr__(self, name: str) -> Any:
        if name.startswith(("write", "update", "insert", "delete", "create", "save")):
            raise AttributeError(f"Legacy data portal is read-only: {name}")
        return getattr(self._portal, name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("Legacy data portal is read-only")
