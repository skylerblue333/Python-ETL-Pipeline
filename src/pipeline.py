"""Validated batch ETL primitives for CSV input and Parquet output."""
from __future__ import annotations

from dataclasses import dataclass
import logging
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
            raise ValueError("filename must remain inside input_dir")
        return path

    def _output_path(self, target_name: str) -> Path:
        path = (self.output_dir / target_name).resolve()
        if self.output_dir not in path.parents:
            raise ValueError("target_name must remain inside output_dir")
        if path.suffix.lower() != ".parquet":
            raise ValueError("target_name must use the .parquet extension")
        return path

    def extract(self, filename: str) -> pd.DataFrame:
        path = self._input_path(filename)
        if not path.is_file():
            raise FileNotFoundError(path)
        frame = pd.read_csv(path)
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"missing required columns: {sorted(missing)}")
        return frame

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        result["user_id"] = pd.to_numeric(result["user_id"], errors="coerce")
        result["transaction_amount"] = pd.to_numeric(result["transaction_amount"], errors="coerce")
        result["status"] = result["status"].astype("string").str.strip().str.upper()
        valid = (
            result["user_id"].notna()
            & result["transaction_amount"].notna()
            & (result["transaction_amount"] >= 0)
            & result["status"].notna()
            & result["status"].ne("")
        )
        result = result.loc[valid].copy()
        result["user_id"] = result["user_id"].astype("int64")
        if self.usd_rate is not None:
            result["amount_usd"] = result["transaction_amount"] * self.usd_rate
        return result.reset_index(drop=True)

    def load(self, frame: pd.DataFrame, target_name: str) -> Path:
        target = self._output_path(target_name)
        temporary = target.with_suffix(target.suffix + ".tmp")
        try:
            frame.to_parquet(temporary, index=False)
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
        return target

    def run(self, filename: str, target_name: str) -> PipelineResult:
        input_path = self._input_path(filename)
        extracted = self.extract(filename)
        transformed = self.transform(extracted)
        output_path = self.load(transformed, target_name)
        return PipelineResult(input_path, output_path, len(extracted), len(transformed), len(extracted) - len(transformed))
