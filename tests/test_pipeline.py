import pytest
import pandas as pd
from pathlib import Path
from src.pipeline import ETLPipeline

@pytest.fixture
def setup_dirs(tmp_path):
    in_dir = tmp_path / "input"
    out_dir = tmp_path / "output"
    in_dir.mkdir()
    out_dir.mkdir()
    
    # Create mock data
    df = pd.DataFrame({
        'user_id': [1, 2, None, 4],
        'transaction_amount': [100, 200, 300, None],
        'status': ['pending', 'completed', 'failed', 'pending']
    })
    df.to_csv(in_dir / "test.csv", index=False)
    return in_dir, out_dir

def test_transform(setup_dirs):
    in_dir, out_dir = setup_dirs
    etl = ETLPipeline(in_dir, out_dir)
    df = etl.extract("test.csv")
    clean_df = etl.transform(df)
    
    assert len(clean_df) == 2  # Nulls dropped
    assert 'amount_usd' in clean_df.columns
    assert clean_df['status'].iloc[0] == 'PENDING'
