import os
from pathlib import Path
import pandas as pd
import s3fs
import logging
import pyarrow.parquet as pq
import pyarrow as pa


logger = logging.getLogger(__name__)


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)



def write_df_local_parquet(df: pd.DataFrame, path: str, append=False) -> None:
    """Write DataFrame to Parquet. If append=True, append to existing Parquet file."""
    ensure_dir(os.path.dirname(path))  # Ensure the directory exists
    
    try:
        table = pa.Table.from_pandas(df)

        if append and os.path.exists(path):  # If appending and file exists
            # Read the existing Parquet file
            existing_table = pq.read_table(path)
            # Concatenate the existing data with the new data
            combined_table = pa.concat_tables([existing_table, table])
            # Write the combined data back to the file
            pq.write_table(combined_table, path)
        else:
            # Write the new data to the file
            pq.write_table(table, path)
        
        logger.info("Wrote local parquet %s (%d rows)", path, len(df))

    except Exception as e:
        logger.exception(f"Error writing to local Parquet file {path}: {e}")

        
import s3fs
import pyarrow.parquet as pq
import pyarrow as pa

def write_df_s3_parquet(df: pd.DataFrame, s3_path: str, anon: bool = False, append=False) -> None:
    """Write DataFrame to Parquet on S3. If append=True, append to existing file."""
    if not s3_path.startswith("s3://"):
        raise ValueError("s3_path must start with s3://")
    
    fs = s3fs.S3FileSystem(anon=anon)
    
    try:
        # Convert the DataFrame to a PyArrow Table
        table = pa.Table.from_pandas(df)

        if append:
            # Check if the file already exists on S3
            if fs.exists(s3_path):
                # Read the existing file from S3
                with fs.open(s3_path, 'rb') as f:
                    existing_table = pq.read_table(f)
                # Concatenate the existing data with the new data
                combined_table = pa.concat_tables([existing_table, table])
                # Write the combined data back to S3
                with fs.open(s3_path, 'wb') as f:
                    pq.write_table(combined_table, f)
            else:
                # If the file doesn't exist, create it
                with fs.open(s3_path, 'wb') as f:
                    pq.write_table(table, f)
        else:
            # If not appending, simply write the new data
            with fs.open(s3_path, 'wb') as f:
                pq.write_table(table, f)
        
        logger.info("Wrote parquet to s3: %s (%d rows)", s3_path, len(df))

    except Exception as e:
        logger.exception(f"Error writing to S3 file {s3_path}: {e}")