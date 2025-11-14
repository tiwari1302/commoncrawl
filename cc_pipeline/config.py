from dataclasses import dataclass
from typing import Optional

@dataclass
class Config:
    aws_region: str = "us-east-1"
    athena_database: str = "ccindex"
    athena_workgroup: str = "primary"
    athena_custom_workgroup_name: str = "cc-custom-wg"
    athena_output_s3: Optional[str] = None # e.g., "s3://my-athena-results/"
    athena_poll_interval: int = 3

    local_output_dir: str = "./outputs"
    s3_output_dir: Optional[str] = None # e.g., "s3://my-cc-extracted/"

    max_workers: int = 8
    chunk_size: int = 2000

    s3_read_retries: int = 2
    s3_read_retry_backoff: float = 2.0
    
    # Range-read fallback flag: if True, attempt S3 Range GET; if gzip boundaries prevent useful reads,
    # set to False to always stream and skip to offsets sequentially.
    use_range_reads: bool = True