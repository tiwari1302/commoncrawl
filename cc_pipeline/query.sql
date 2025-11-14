WITH
  src AS (
    SELECT
      row_number() OVER (
        PARTITION BY
          url_host_name
        ORDER BY
          url_protocol DESC, -- prefer https
          LENGTH (url_surtkey) -- shortest path/query
      ) AS rank,
      url,
      url_surtkey,
      url_host_name,
      warc_filename,
      warc_record_offset,
      warc_record_length,
      crawl,
      subset
    FROM
      ccindex 
    WHERE
      crawl = 'CC-MAIN-2025-43'
      AND subset = 'warc'
     

  )
SELECT
  url,
  url_surtkey,
  url_host_name,
  warc_filename,
  warc_record_offset AS offset,
  warc_record_length AS length,
  crawl,
  subset,
  CONCAT(
    's3://commoncrawl/crawl-data/', crawl, '/',
    regexp_extract(warc_filename, 'segments/([^/]+)'), '/wat/',
    regexp_replace(
      element_at(split(warc_filename, '/'), cardinality(split(warc_filename, '/'))),
      '.warc.gz',
      '.warc.wat.gz'
    )
  ) AS wat_s3_url
FROM src
WHERE rank = 1
limit 20
