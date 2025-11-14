# commoncrawl
This package implements an efficient offset-guided Common Crawl extractor.

Usage:
0. Rename 
1. Install dependencies: `pip install -r requirements.txt`
2. Prepare an Athena SQL file that returns columns: `url, warc_filename, offset, length, wat_s3_url` ordered by `warc_filename, offset`.
   Example Athena SELECT should construct `wat_s3_url` as in earlier messages.
3. Run:

```bash
python cli.py --query-file query.sql --local-out ./outputs --s3-out s3://my-bucket/cc-extracted/ --athena-out s3://my-athena-out/ --max-workers 6
```

Notes:
- The extractor will attempt S3 range GETs when `use_range_reads` is enabled. If range reads fail to yield parsable JSON (common when gzip compressed blocks are not aligned), the extractor falls back to streaming the WAT and performing a single-pass extraction for that file.
- This design ensures **one pass per WAT file** and avoids scanning entire crawls multiple times.
- For very large workloads consider running on EC2 with an instance role that has appropriate S3 and Athena permissions.