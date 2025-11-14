SELECT
  url,
  warc_filename,
  warc_record_offset AS offset,
  warc_record_length AS length,
  CONCAT(
    's3://commoncrawl/crawl-data/', crawl, '/',
    regexp_extract(warc_filename, 'segments/([^/]+)'), '/wat/',
    regexp_replace(
      element_at(split(warc_filename, '/'), cardinality(split(warc_filename, '/'))),
      '.warc.gz', '.warc.wat.gz'
    )
  ) AS wat_s3_url
FROM ccindex
WHERE crawl = 'CC-MAIN-2025-43'
  AND subset = 'warc'
ORDER BY warc_filename, warc_record_offset
