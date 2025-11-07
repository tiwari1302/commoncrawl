import os
from pathlib import Path
import pandas as pd
import s3fs
import logging

logger = logging.getLogger(__name__)


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def write_df_local_parquet(df: pd.DataFrame, path: str) -> None:
    ensure_dir(os.path.dirname(path))
    df.to_parquet(path, index=False)
    logger.info("Wrote local parquet %s (%d rows)", path, len(df))


def write_df_s3_parquet(df: pd.DataFrame, s3_path: str, anon: bool = False) -> None:
    if not s3_path.startswith("s3://"):
        raise ValueError("s3_path must start with s3://")
    fs = s3fs.S3FileSystem(anon=anon)
    with fs.open(s3_path, "wb") as f:
        df.to_parquet(f, index=False)
    logger.info("Wrote parquet to s3: %s (%d rows)", s3_path, len(df))