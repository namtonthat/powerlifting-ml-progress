DROP TABLE IF EXISTS raw.openpowerlifting;
CREATE TABLE raw.openpowerlifting AS (
    SELECT * FROM landing.openpowerlifting
    WHERE
        event = 'SBD'
        AND tested = 'Yes'
        AND equipment = 'Raw'
)
