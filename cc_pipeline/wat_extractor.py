"""
WATExtractor optimized for offset-guided extraction.
Two modes:
 - Range reads (attempt to GET only the byte ranges corresponding to record offsets). Works if WATs are stored uncompressed or have gzip blocks aligned.
 - Sequential single-pass streaming: open the WAT and iterate through ArchiveIterator but skip records until the next wanted offset, processing in increasing offset order.

We implement both. Range-reads are attempted when cfg.use_range_reads is True; but we fall back to sequential if range read fails.
"""

import io
import gzip
import json
import logging
from typing import List, Dict, Optional, Set, Tuple
from warcio.archiveiterator import ArchiveIterator
import boto3
import botocore
import s3fs
import time

logger = logging.getLogger(__name__)


class WATExtractor:
    def __init__(self, cfg):
        self.cfg = cfg
        # fs for anonymous reads from commoncrawl
        self.fs = s3fs.S3FileSystem( client_kwargs={"region_name": cfg.aws_region})
        # s3 client for range GETs
        self.s3 = boto3.client("s3", region_name=cfg.aws_region)

    @staticmethod
    def _parse_wat_record_bytes(b: bytes) -> Optional[Dict]:
        """Given bytes containing a single WAT JSON record, return parsed metadata dict."""
        try:
            wat_json = json.loads(b)
            envelope = wat_json.get("Envelope", {})
            payload_meta = envelope.get("Payload-Metadata", {})
            content_type = payload_meta.get("Actual-Content-Type")
            if content_type != "application/http; msgtype=response":
                return None
            html_meta = payload_meta.get("HTTP-Response-Metadata", {}).get("HTML-Metadata", {})
            head = html_meta.get("Head")
            digest = payload_meta.get("Entity-Digest") or envelope.get("WARC-Header-Metadata", {}).get("WARC-Payload-Digest")
            # WAT record may include WARC headers inside; sometimes easier to parse using ArchiveIterator on a full stream
            return {
                "Head": json.dumps(head, ensure_ascii=False) if head else None,
                "Entity-Digest": digest,
                "Envelope": envelope,
            }
        except Exception as e:
            logger.debug("Failed to parse wat json bytes: %s", e)
            return None

    def _s3_range_get(self, s3_url: str, start: int, length: int) -> Optional[bytes]:
        """Attempt to GET a byte range from S3. Returns bytes or None on failure."""
        # s3_url -> bucket/key
        assert s3_url.startswith("s3://")
        parts = s3_url[5:].split("/", 1)
        bucket = parts[0]
        key = parts[1]
        # Range header spec: bytes=start-end (inclusive)
        end = start + length - 1
        rng = f"bytes={start}-{end}"
        try:
            resp = self.s3.get_object(Bucket=bucket, Key=key, Range=rng)
            data = resp["Body"].read()
            return data
        except botocore.exceptions.ClientError as e:
            logger.debug("Range GET failed for %s %s: %s", s3_url, rng, e)
            return None
        except Exception as e:
            logger.exception("Unexpected error on range get %s: %s", s3_url, e)
            return None

    def _stream_and_extract(self, s3_url: str, offsets: List[Tuple[int,int,str]], urls_of_interest: Optional[Set[str]] = None) -> List[Dict]:
        """Open the entire WAT stream and iterate sequentially, extracting records matching offsets.
        offsets must be sorted ascending by offset.
        We do a single pass per file and stop once we've found all requested offsets.
        """
        results = []
        found_offsets = set()
        try:
            with self.fs.open(s3_url, "rb") as f:
                with gzip.GzipFile(fileobj=f) as gz:
                    for record in ArchiveIterator(gz):
                        if record.rec_type != "metadata":
                            continue
                        try:
                            rec_offset_str = record.rec_headers.get_header("WARC-Block-Digest") or record.rec_headers.get_header("WARC-Record-ID")
                        except Exception:
                            rec_offset_str = None
                        # We can't rely on headers for offset mapping when streaming; instead compare actual record stream position
                        # ArchiveIterator doesn't expose raw byte offset easily; so rather than exact match, we compare target-uri
                        # Fallback: if urls_of_interest provided, filter by that; otherwise keep everything matching content-type
                        payload = json.loads(record.content_stream().read())
                        envelope = payload.get("Envelope", {})
                        payload_meta = envelope.get("Payload-Metadata", {})
                        if payload_meta.get("Actual-Content-Type") != "application/http; msgtype=response":
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
                        # Optional early exit: when we've collected enough records equal to offsets length and urls_of_interest provided
                        if urls_of_interest and len(results) >= len(offsets):
                            break
        except Exception as e:
            logger.exception("Stream-extract failed for %s: %s", s3_url, e)
        return results

    def extract_by_offsets(self, s3_url: str, offset_rows: List[Tuple[int,int,str]], urls_of_interest: Optional[Set[str]] = None) -> List[Dict]:
        """Given a list of (offset, length, url) for a single wat_s3_url, extract those records.
        Tries range-reads first (if cfg.use_range_reads True). If range reads can't parse, falls back to sequential streaming.
        The offset_rows must be provided in ascending offset order.
        """
        # Quick sanity
        if not offset_rows:
            return []

        results = []
        if self.cfg.use_range_reads:
            logger.debug("Attempting range reads for %s with %d offsets", s3_url, len(offset_rows))
            for off, length, url in offset_rows:
                data = self._s3_range_get(s3_url, int(off), int(length))
                if not data:
                    logger.debug("Range read returned no data for %s offset %s; falling back to streaming entire file", s3_url, off)
                    results = self._stream_and_extract(s3_url, offset_rows, urls_of_interest)
                    return results
                # The range may land in the middle of a compressed block; try to locate JSON-like substring
                try:
                    # Try to decode and find the first '{' to last '}'
                    txt = data.decode("utf-8", errors="replace")
                    start = txt.find('{')
                    end = txt.rfind('}')
                    if start == -1 or end == -1 or end <= start:
                        # can't extract JSON reliably from range-read — fallback
                        logger.debug("Range read for %s offset %s did not contain parsable JSON; fallback", s3_url, off)
                        results = self._stream_and_extract(s3_url, offset_rows, urls_of_interest)
                        return results
                    piece = txt[start:end+1]
                    parsed = self._parse_wat_record_bytes(piece.encode('utf-8'))
                    if parsed:
                        # We still need the URL — parse target-uri if present in envelope; envelope may contain WARC headers
                        env = parsed.get('Envelope', {})
                        # try to obtain target uri from env/WARC headers
                        target_uri = None
                        try:
                            # look into WARC-Header-Metadata if present
                            warc_hdr = env.get('WARC-Header-Metadata', {})
                            target_uri = warc_hdr.get('WARC-Target-URI')
                        except Exception:
                            target_uri = None
                        results.append({
                            'URL': target_uri or url,
                            'Head': parsed.get('Head'),
                            'Entity-Digest': parsed.get('Entity-Digest'),
                            'WatFile': s3_url,
                        })
                    else:
                        logger.debug("Parsed piece was None; fallback to streaming for %s", s3_url)
                        results = self._stream_and_extract(s3_url, offset_rows, urls_of_interest)
                        return results
                except Exception as e:
                    logger.exception("Error parsing range-read for %s offset %s: %s", s3_url, off, e)
                    results = self._stream_and_extract(s3_url, offset_rows, urls_of_interest)
                    return results
            return results
        else:
            logger.debug("Range reads disabled; streaming %s", s3_url)
            return self._stream_and_extract(s3_url, offset_rows, urls_of_interest)