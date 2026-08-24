"""Validated batch ETL primitives for CSV input and Parquet output."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pandas as pd

LOGGER = logging.getLogger(__name__)
REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset({"user_id", "transaction_amount", "status"})


@dataclass(frozen=True)
class PipelineResult:
    input_path: Path
    output_path: Path
    rows_read: int
    rows_written: int
    rows_rejected: int


class ETLPipeline:
    """Run deterministic CSV-to-Parquet ETL with explicit data-quality rules.

    A USD conversion is optional and must be supplied by the caller. The pipeline
    never fabricates a market or exchange rate.
    """

    def __init__(self, input_dir: str | Path, output_dir: str | Path, usd_rate: float | None = None) -> None:
        self.input_dir = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        if usd_rate is not None and usd_rate <= 0:
            raise ValueError("usd_rate must be positive when provided")
        self.usd_rate = usd_rate
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _input_path(self, filename: str) -> Path:
        path = (self.input_dir / filename).resolve()
        if self.input_dir not in path.parents:
            raise ValueError("input filename must stay within input directory")
        if path.suffix.lower() != ".csv":
            raise ValueError("input file must be a .csv file")
        return path

    def _output_path(self, filename: str) -> Path:
        path = (self.output_dir / filename).resolve()
        if self.output_dir not in path.parents:
            raise ValueError("output filename must stay within output directory")
        if path.suffix.lower() != ".parquet":
            raise ValueError("output file must be a .parquet file")
        return path

    def run(self, input_filename: str, output_filename: str) -> PipelineResult:
        input_path = self._input_path(input_filename)
        output_path = self._output_path(output_filename)
        if not input_path.is_file():
            raise FileNotFoundError(f"input file not found: {input_path}")

        frame = pd.read_csv(input_path)
        missing = REQUIRED_COLUMNS - set(frame.columns)
        if missing:
            raise ValueError(f"missing required columns: {', '.join(sorted(missing))}")

        rows_read = len(frame)
        frame["transaction_amount"] = pd.to_numeric(frame["transaction_amount"], errors="coerce")
        valid = frame["user_id"].notna() & frame["transaction_amount"].notna() & frame["status"].notna()
        valid &= frame["transaction_amount"] >= 0
        cleaned = frame.loc[valid].copy()
        cleaned["status"] = cleaned["status"].astype(str).str.strip().str.lower()
        cleaned["user_id"] = cleaned["user_id"].astype(str).str.strip()

        if self.usd_rate is not None:
            cleaned["transaction_amount_usd"] = cleaned["transaction_amount"] * self.usd_rate

        output_path.parent.mkdir(parents=True, exist_ok=True)
        cleaned.to_parquet(output_path, index=False)
        result = PipelineResult(
            input_path=input_path,
            output_path=output_path,
            rows_read=rows_read,
            rows_written=len(cleaned),
            rows_rejected=rows_read - len(cleaned),
        )
        LOGGER.info(
            "etl_run input=%s output=%s rows_read=%d rows_written=%d rows_rejected=%d",
            input_path,
            output_path,
            result.rows_read,
            result.rows_written,
            result.rows_rejected,
        )
        return result
