WITH
    src AS (
        select
            row_number() over (
                partition by
                    url_host_name
                ORDER BY
                    url_protocol DESC, -- prefer https over http
                    length (url_surtkey) -- prefer shortest path/query
            ) "rank",
            *
        from
            ccindex
        where
            crawl = 'CC-MAIN-2025-38'
            AND subset = 'warc'
    )
SELECT
    url_surtkey,
    url,
    warc_filename,
    warc_record_offset,
    warc_record_length,
    crawl,
    subset
FROM
    src
WHERE
    rank = 1