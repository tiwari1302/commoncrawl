import gzip
import json
import logging
from typing import List, Dict, Optional, Set, Tuple
from warcio.archiveiterator import ArchiveIterator
import s3fs
import time

logger = logging.getLogger(__name__)


class WATExtractor:
    def __init__(self, cfg):
        self.cfg = cfg
        # Use anonymous for public Common Crawl; if writing to s3 with creds, fs can be changed
        self.fs = s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": cfg.aws_region})

    def _open_s3_gzip_stream(self, s3_url: str) -> Tuple[gzip.GzipFile, object]:
        last_exc = None
        for attempt in range(self.cfg.s3_read_retries + 1):
            try:
                f = self.fs.open(s3_url, "rb")
                gz = gzip.GzipFile(fileobj=f)
                return gz, f
            except Exception as e:
                last_exc = e
                wait = self.cfg.s3_read_retry_backoff * (2 ** attempt)
                logger.warning("open failed %s attempt %d: %s - retrying %.1fs", s3_url, attempt + 1, e, wait)
                time.sleep(wait)
        logger.error("Failed to open %s: %s", s3_url, last_exc)
        raise last_exc

    def extract_from_wat(self, s3_url: str, urls_of_interest: Optional[Set[str]] = None) -> List[Dict]:
        gz_stream = None
        fs_handle = None
        results: List[Dict] = []
        try:
            gz_stream, fs_handle = self._open_s3_gzip_stream(s3_url)
            for record in ArchiveIterator(gz_stream):
                try:
                    if record.rec_type != "metadata":
                        continue
                    wat_json = json.loads(record.content_stream().read())
                    envelope = wat_json.get("Envelope", {})
                    payload_meta = envelope.get("Payload-Metadata", {})
                    content_type = payload_meta.get("Actual-Content-Type")
                    if content_type != "application/http; msgtype=response":
                        continue
                    target_uri = record.rec_headers.get_header("WARC-Target-URI")
                    if urls_of_interest and target_uri not in urls_of_interest:
                        continue
                    html_meta = payload_meta.get("HTTP-Response-Metadata", {}).get("HTML-Metadata", {})
                    head = html_meta.get("Head")
                    digest = payload_meta.get("Entity-Digest") or envelope.get("WARC-Header-Metadata", {}).get("WARC-Payload-Digest")
                    if head or digest:
                        results.append({
                            "URL": target_uri,
                            "Head": json.dumps(head, ensure_ascii=False) if head else None,
                            "Entity-Digest": digest,
                            "WatFile": s3_url,
                        })
                except Exception as e:
                    logger.debug("record parse error %s: %s", s3_url, e)
            logger.info("extracted %d records from %s", len(results), s3_url)
        except Exception as e:
            logger.exception("failed %s: %s", s3_url, e)
        finally:
            try:
                if gz_stream:
                    gz_stream.close()
                if fs_handle:
                    fs_handle.close()
            except Exception:
                pass
        return results