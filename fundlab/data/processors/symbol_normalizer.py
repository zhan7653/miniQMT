from __future__ import annotations

import re


SYMBOL_PATTERN = re.compile(r"^(\d{6})(?:\.(SH|SZ|BJ))?$")


def normalize_symbol(value: str) -> str:
    text = str(value).strip().upper()
    match = SYMBOL_PATTERN.match(text)
    if not match:
        raise ValueError(f"Invalid Chinese security symbol: {value}")

    code, exchange = match.groups()
    if exchange:
        return f"{code}.{exchange}"
    if code.startswith(("5", "6")):
        return f"{code}.SH"
    if code.startswith(("0", "1", "2", "3")):
        return f"{code}.SZ"
    if code.startswith(("4", "8")):
        return f"{code}.BJ"
    raise ValueError(f"Cannot infer exchange for symbol: {value}")

