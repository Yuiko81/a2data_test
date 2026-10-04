CREATE INDEX IF NOT EXISTS logs_service_level_timestamp_id_idx
ON logs (service, level, timestamp DESC, id DESC);
