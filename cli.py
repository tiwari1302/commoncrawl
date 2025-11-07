# python cli.py --query-file query.sql --local-out ./outputs --s3-out s3://my-bucket/outputs --athena-out s3://my-athena-out/

import argparse
import logging
from cc_pipeline.config import Config
from cc_pipeline.pipeline import Pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cc_cli")


def main():
    p = argparse.ArgumentParser(description="Run Common Crawl Athena->WAT extraction pipeline")
    p.add_argument("--query-file", help="Path to SQL file to run (required)", required=True)
    p.add_argument("--local-out", help="Local output dir", default="./outputs")
    p.add_argument("--s3-out", help="S3 output dir for extracted parquet (optional)")
    p.add_argument("--athena-out", help="Athena output s3 location (optional)")
    p.add_argument("--max-workers", type=int, default=8)
    args = p.parse_args()

    with open(args.query_file, "r") as fh:
        query = fh.read()

    cfg = Config(
        local_output_dir=args.local_out,
        s3_output_dir=args.s3_out,
        athena_output_s3=args.athena_out,
        max_workers=args.max_workers,
    )

    pipeline = Pipeline(cfg)
    pipeline.run(query)


if __name__ == "__main__":
    main()