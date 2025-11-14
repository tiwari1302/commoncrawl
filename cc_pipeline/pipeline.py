import os
import time
import logging
from typing import Optional, Set, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from .athena_runner import AthenaRunner
from .wat_extractor import WATExtractor
from .data_writer import write_df_local_parquet, write_df_s3_parquet, ensure_dir

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, cfg):
        self.cfg = cfg
        ensure_dir(cfg.local_output_dir)
        self.athena = AthenaRunner(cfg)
        self.extractor = WATExtractor(cfg)

    def run(self, query: str, urls_of_interest: Optional[Set[str]] = None, persist_athena_to_s3: bool = False):
        logger.info("Running Athena query")
        exec_info = self.athena.run_query(query, use_custom_output=bool(self.cfg.athena_output_s3))
        qid = exec_info["QueryExecution"]["QueryExecutionId"]

        athena_df = self.athena.fetch_query_results(qid)
        timestamp = int(time.time())
        athena_local = os.path.join(self.cfg.local_output_dir, f"athena_results_{timestamp}.parquet")
        write_df_local_parquet(athena_df, athena_local)
        if persist_athena_to_s3 and self.cfg.s3_output_dir:
            s3_path = os.path.join(self.cfg.s3_output_dir.rstrip('/'), f"athena_results_{timestamp}.parquet")
            write_df_s3_parquet(athena_df, s3_path)

        # Expect athena_df to have columns: url, warc_filename, offset, length, wat_s3_url
        required = {"url", "warc_filename", "offset", "length", "wat_s3_url"}
        if not required.issubset(set(athena_df.columns)):
            logger.error("Athena result missing required columns. Got: %s", athena_df.columns.tolist())
            raise ValueError("Athena result must include url, warc_filename, offset, length, wat_s3_url")

        # Convert offsets to int and sort by warc_filename and offset
        athena_df["offset"] = athena_df["offset"].astype(int)
        athena_df["length"] = athena_df["length"].astype(int)
        athena_df = athena_df.sort_values(["warc_filename", "offset"])  # ensure ordering

        # Group by wat_s3_url
        groups = athena_df.groupby("wat_s3_url")
        wat_urls = list(groups.groups.keys())
        logger.info("Will process %d WAT files (one pass each)", len(wat_urls))

        # Parallel: each worker handles one WAT file and its offsets list
        aggregated = []
        total_extracted = 0
        timestamp = int(time.time())
        with ThreadPoolExecutor(max_workers=self.cfg.max_workers) as ex:
            futures = {}
            for wat_url, subset_idx in groups.groups.items():
                subset = groups.get_group(wat_url)
                # build list of tuples (offset, length, url)
                offset_rows: List[Tuple[int,int,str]] = list(subset[["offset","length","url"]].itertuples(index=False, name=None))
                # submit worker
                futures[ex.submit(self.extractor.extract_by_offsets, wat_url, offset_rows, urls_of_interest)] = wat_url

            for fut in as_completed(futures):
                wat = futures[fut]
                try:
                    rows = fut.result()
                    if rows:
                        aggregated.extend(rows)
                except Exception as e:
                    logger.exception("Worker for %s failed: %s", wat, e)

                # flush in chunks
                if len(aggregated) >= self.cfg.chunk_size:
                    df_chunk = pd.DataFrame(aggregated)
                    total_extracted += len(df_chunk)
                    out_local = os.path.join(self.cfg.local_output_dir, f"extracted_{timestamp}_{total_extracted}.parquet")
                    write_df_local_parquet(df_chunk, out_local)
                    if self.cfg.s3_output_dir:
                        out_s3 = os.path.join(self.cfg.s3_output_dir.rstrip('/'), f"extracted_{timestamp}_{total_extracted}.parquet")
                        write_df_s3_parquet(df_chunk, out_s3)
                    aggregated = []

        # final flush
        if aggregated:
            df_chunk = pd.DataFrame(aggregated)
            total_extracted += len(df_chunk)
            out_local = os.path.join(self.cfg.local_output_dir, f"extracted_{timestamp}_{total_extracted}.parquet")
            write_df_local_parquet(df_chunk, out_local)
            if self.cfg.s3_output_dir:
                out_s3 = os.path.join(self.cfg.s3_output_dir.rstrip('/'), f"extracted_{timestamp}_{total_extracted}.parquet")
                write_df_s3_parquet(df_chunk, out_s3)

        logger.info("Completed. total extracted records: %d", total_extracted)