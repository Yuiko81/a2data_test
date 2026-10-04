CREATE TABLE logs (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    service TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,

    PRIMARY KEY (timestamp, id)
) PARTITION BY RANGE (timestamp);


DO $$
DECLARE
    day DATE;
BEGIN
    FOR day IN
        SELECT generate_series(
            CURRENT_DATE - 7,
            CURRENT_DATE + 2,
            INTERVAL '1 day'
        )::DATE
    LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF logs
             FOR VALUES FROM (%L) TO (%L)',
            'logs_' || to_char(day, 'YYYYMMDD'),
            day,
            day + 1
        );
    END LOOP;
END $$;


CREATE INDEX IF NOT EXISTS logs_service_level_timestamp_id_idx
ON logs (service, level, timestamp DESC, id DESC);
