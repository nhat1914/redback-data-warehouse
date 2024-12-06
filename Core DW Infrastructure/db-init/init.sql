-- db-init/init.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS provenance (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_source VARCHAR(255),
    event_type VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(255),
    source_ip VARCHAR(255),
    bucket_name VARCHAR(255),
    object_key VARCHAR(255),
    object_size BIGINT,
    object_etag VARCHAR(255),
    content_type VARCHAR(255),
    service_endpoint VARCHAR(255),
    custom_metadata JSONB
);