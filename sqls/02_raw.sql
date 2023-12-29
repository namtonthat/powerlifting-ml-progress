CREATE TABLE raw.natural_powerlifting AS (
  SELECT * FROM landing.powerlifting
  WHERE
    [event] = 'SBD'
    AND tested = 'Yes'
    AND equipment = 'Raw'
);
