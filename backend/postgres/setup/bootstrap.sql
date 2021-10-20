CREATE TABLE IF NOT EXISTS quake_sink (
    magnitude DECIMAL,
    longitude NUMERIC,
    latitude NUMERIC
);

ALTER TABLE quake_sink REPLICA IDENTITY FULL; -- temporary row key until update


-- -- schema
-- CREATE SCHEMA quake;
-- SET search_path TO quake;


-- tables