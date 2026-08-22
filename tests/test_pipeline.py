from __future__ import annotations

import pandas as pd
import pytest

from src.pipeline import ETLPipeline


def write_input(tmp_path, rows: list[dict[str, object]]):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    pd.DataFrame(rows).to_csv(input_dir / "transactions.csv", index=False)
    return input_dir, output_dir


def test_transform_rejects_invalid_rows_without_fabricating_usd(tmp_path) -> None:
    input_dir, output_dir = write_input(tmp_path, [
        {"user_id": 1, "transaction_amount": 100, "status": " pending "},
        {"user_id": 2, "transaction_amount": -1, "status": "completed"},
        {"user_id": None, "transaction_amount": 300, "status": "failed"},
    ])
    result = ETLPipeline(input_dir, output_dir).run("transactions.csv", "clean.parquet")
    output = pd.read_parquet(result.output_path)
    assert result.rows_read == 3
    assert result.rows_written == 1
    assert result.rows_rejected == 2
    assert output.loc[0, "status"] == "PENDING"
    assert "amount_usd" not in output.columns


def test_authoritative_rate_is_explicit(tmp_path) -> None:
    input_dir, output_dir = write_input(tmp_path, [{"user_id": 1, "transaction_amount": 100, "status": "paid"}])
    result = ETLPipeline(input_dir, output_dir, usd_rate=1.25).run("transactions.csv", "rated.parquet")
    assert pd.read_parquet(result.output_path).loc[0, "amount_usd"] == 125


def test_missing_columns_fail_fast(tmp_path) -> None:
    input_dir, output_dir = write_input(tmp_path, [{"user_id": 1, "status": "paid"}])
    with pytest.raises(ValueError, match="missing required columns"):
        ETLPipeline(input_dir, output_dir).extract("transactions.csv")


def test_paths_cannot_escape_configured_directories(tmp_path) -> None:
    input_dir, output_dir = write_input(tmp_path, [{"user_id": 1, "transaction_amount": 1, "status": "paid"}])
    pipeline = ETLPipeline(input_dir, output_dir)
    with pytest.raises(ValueError, match="inside input_dir"):
        pipeline.extract("../transactions.csv")
    with pytest.raises(ValueError, match="inside output_dir"):
        pipeline.load(pd.DataFrame(), "../escape.parquet")


def test_invalid_rate_is_rejected(tmp_path) -> None:
    input_dir, output_dir = write_input(tmp_path, [{"user_id": 1, "transaction_amount": 1, "status": "paid"}])
    with pytest.raises(ValueError, match="positive"):
        ETLPipeline(input_dir, output_dir, usd_rate=0)
