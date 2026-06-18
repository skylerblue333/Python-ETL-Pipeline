import pandas as pd
import logging
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def extract(self, filename: str) -> Optional[pd.DataFrame]:
        filepath = self.input_dir / filename
        if not filepath.exists():
            logger.error(f"File not found: {filepath}")
            return None
        logger.info(f"Extracting data from {filepath}")
        return pd.read_csv(filepath)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Transforming data...")
        # Drop nulls
        df = df.dropna(subset=['user_id', 'transaction_amount'])
        # Normalize text
        if 'status' in df.columns:
            df['status'] = df['status'].str.upper()
        # Calculate aggregates
        df['amount_usd'] = df['transaction_amount'] * 1.1  # Mock conversion
        return df

    def load(self, df: pd.DataFrame, target_name: str) -> None:
        target_path = self.output_dir / target_name
        logger.info(f"Loading data to {target_path}")
        df.to_parquet(target_path, index=False)

    def run(self, filename: str, target_name: str):
        df = self.extract(filename)
        if df is not None:
            clean_df = self.transform(df)
            self.load(clean_df, target_name)
            logger.info("Pipeline complete.")
