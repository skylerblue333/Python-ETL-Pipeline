from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.pipeline import ETLPipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a validated CSV-to-Parquet ETL job.")
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("filename")
    parser.add_argument("target_name")
    parser.add_argument("--usd-rate", type=float, default=None, help="Optional authoritative USD conversion rate.")
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args()
    result = ETLPipeline(args.input_dir, args.output_dir, args.usd_rate).run(args.filename, args.target_name)
    print(f"rows_read={result.rows_read} rows_written={result.rows_written} rows_rejected={result.rows_rejected} output={result.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
