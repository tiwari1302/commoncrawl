# python cli.py --query-file query.sql --local-out ./outputs --s3-out s3://my-bucket/outputs --athena-out s3://my-athena-out/

import argparse
import logging
from cc_pipeline.config import Config
from cc_pipeline.pipeline import Pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cc_cli")


def main():
    p = argparse.ArgumentParser(description="Run offset-guided Common Crawl extraction pipeline")
    p.add_argument("--query-file", required=True, help="SQL file that returns url, warc_filename, offset, length, wat_s3_url ordered by warc_filename,offset")
    p.add_argument("--local-out", default="./outputs")
    p.add_argument("--s3-out", default=None)
    p.add_argument("--athena-out", default=None)
    p.add_argument("--max-workers", type=int, default=8)
    p.add_argument("--no-range-reads", action="store_true", help="Disable S3 Range GETs and always stream WAT files")
    args = p.parse_args()

    with open(args.query_file, "r") as fh:
        query = fh.read()

    cfg = Config(
        local_output_dir=args.local_out,
        s3_output_dir=args.s3_out,
        athena_output_s3=args.athena_out,
        max_workers=args.max_workers,
        use_range_reads=(not args.no_range_reads),
    )

    pipeline = Pipeline(cfg)
    pipeline.run(query)


if __name__ == "__main__":
    main()