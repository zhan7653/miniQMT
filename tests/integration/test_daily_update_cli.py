from datetime import date

from fundlab.data.pipeline import UpdateResult
from scripts.update_data_v2 import build_parser, write_requested_outputs


def test_cli_requires_target_and_accepts_backfill():
    args = build_parser().parse_args(["--target-date", "2026-05-07", "--start-date", "2026-05-01",
        "--backfill-missing", "--json", "--json-output", "out/result.json",
        "--markdown-output", "out/result.md"])
    assert args.target_date == date(2026, 5, 7)
    assert args.start_date == date(2026, 5, 1)
    assert args.json
    assert args.backfill_missing
    assert str(args.json_output).replace("\\", "/") == "out/result.json"
    assert str(args.markdown_output).replace("\\", "/") == "out/result.md"


def test_cli_writes_exact_json_and_markdown_outputs(tmp_path):
    result = UpdateResult("complete", "2026-05-07", "xtquant", batch_id="batch-1", version_id="version-1")
    json_path, markdown_path = tmp_path / "nested" / "result.json", tmp_path / "nested" / "result.md"
    write_requested_outputs(result, json_output=json_path, markdown_output=markdown_path)
    assert '"status": "complete"' in json_path.read_text(encoding="utf-8")
    assert "# Data Platform v2 daily update" in markdown_path.read_text(encoding="utf-8")
