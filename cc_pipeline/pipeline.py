import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Set
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

    @staticmethod
    def _normalize_wat_url(candidate: str) -> str:
        if candidate is None:
            return candidate
        if ".warc.wat.gz" in candidate:
            return candidate
        out = candidate.replace("/warc/", "/wat/")
        out = out.replace(".warc.gz", ".warc.wat.gz")
        return out

    def run(self, query: str, urls_of_interest: Optional[Set[str]] = None, persist_athena_to_s3: bool = False):
        exec_info = self.athena.run_query(query, use_custom_output=bool(self.cfg.athena_output_s3))
        qid = exec_info["QueryExecution"]["QueryExecutionId"]

        athena_df = self.athena.fetch_query_results(qid)
        timestamp = int(time.time())
        athena_local = os.path.join(self.cfg.local_output_dir, f"athena_results_{timestamp}.parquet")
        write_df_local_parquet(athena_df, athena_local)
        if persist_athena_to_s3 and self.cfg.s3_output_dir:
            s3_path = os.path.join(self.cfg.s3_output_dir.rstrip('/'), f"athena_results_{timestamp}.parquet")
            write_df_s3_parquet(athena_df, s3_path)

        # Build wat list
        if "wat_s3_url" in athena_df.columns:
            candidates = athena_df["wat_s3_url"].dropna().unique().tolist()
        elif "warc_filename" in athena_df.columns:
            candidates = [self._normalize_wat_url(v) for v in athena_df["warc_filename"].dropna().unique().tolist()]
        else:
            candidates = []
            for col in athena_df.columns:
                if athena_df[col].dtype == object:
                    candidates.extend([v for v in athena_df[col].dropna().unique().tolist() if ".warc" in str(v)])
            candidates = list(set(candidates))

        wat_urls = [u for u in (self._normalize_wat_url(x) for x in candidates) if u and u.startswith("s3://")]
        logger.info("Found %d wat files", len(wat_urls))
        if not wat_urls:
            logger.warning("No wat files found")
            return

        aggregated = []
        total = 0
        futures = []
        with ThreadPoolExecutor(max_workers=self.cfg.max_workers) as ex:
            for u in wat_urls:
                futures.append(ex.submit(self.extractor.extract_from_wat, u, urls_of_interest))
            for fut in as_completed(futures):
                try:
                    rows = fut.result()
                    if rows:
                        aggregated.extend(rows)
                except Exception as e:
                    logger.exception("worker failed: %s", e)

                if len(aggregated) >= self.cfg.chunk_size:
                    df = pd.DataFrame(aggregated)
                    total += len(df)
                    fname = os.path.join(self.cfg.local_output_dir, f"extracted_{timestamp}_{total}.parquet")
                    write_df_local_parquet(df, fname)
                    if self.cfg.s3_output_dir:
                        s3path = os.path.join(self.cfg.s3_output_dir.rstrip('/'), f"extracted_{timestamp}_{total}.parquet")
                        write_df_s3_parquet(df, s3path)
                    aggregated = []
        if aggregated:
            df = pd.DataFrame(aggregated)
            total += len(df)
            fname = os.path.join(self.cfg.local_output_dir, f"extracted_{timestamp}_{total}.parquet")
            write_df_local_parquet(df, fname)
            if self.cfg.s3_output_dir:
                s3path = os.path.join(self.cfg.s3_output_dir.rstrip('/'), f"extracted_{timestamp}_{total}.parquet")
                write_df_s3_parquet(df, s3path)
        logger.info("Done. total records=%d", total)
