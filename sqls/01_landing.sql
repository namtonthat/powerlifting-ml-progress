CREATE TABLE landing.openpowerlifting AS (
    SELECT
        *
    FROM read_parquet('{s3_file_path}')
);
