CREATE TABLE risk_detection_request_inbox (
    event_id UUID PRIMARY KEY,
    tenant_alias VARCHAR(35) NOT NULL,
    run_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    request_id VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(100) NOT NULL,
    snapshot_hash VARCHAR(100) NOT NULL,
    event_payload JSONB NOT NULL,
    status VARCHAR(30) NOT NULL,
    http_attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    last_error_code VARCHAR(60),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_risk_detection_inbox_status CHECK (
        status IN (
            'RECEIVED', 'PROCESSING', 'RETRY_PENDING', 'OUTCOME_PENDING',
            'OUTCOME_PUBLISHED', 'OUTCOME_DEAD'
        )
    ),
    CONSTRAINT ck_risk_detection_inbox_attempts CHECK (http_attempts >= 0),
    CONSTRAINT ck_risk_detection_inbox_tenant_alias CHECK (
        tenant_alias ~ '^tn_[0-9a-f]{32}$'
    )
);

CREATE INDEX idx_risk_detection_inbox_claim
    ON risk_detection_request_inbox (next_attempt_at, created_at)
    WHERE status IN ('RECEIVED', 'RETRY_PENDING', 'PROCESSING');

CREATE TABLE risk_detection_outbox (
    event_id UUID PRIMARY KEY,
    source_event_id UUID NOT NULL UNIQUE
        REFERENCES risk_detection_request_inbox(event_id),
    topic VARCHAR(200) NOT NULL,
    message_key VARCHAR(100) NOT NULL,
    event_payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    last_error_code VARCHAR(60),
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT ck_risk_detection_outbox_status CHECK (
        status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'DEAD')
    ),
    CONSTRAINT ck_risk_detection_outbox_attempts CHECK (publish_attempts >= 0),
    CONSTRAINT ck_risk_detection_outbox_message_key CHECK (
        message_key ~ '^tn_[0-9a-f]{32}$'
    )
);

CREATE INDEX idx_risk_detection_outbox_claim
    ON risk_detection_outbox (available_at, created_at)
    WHERE status IN ('PENDING', 'PUBLISHING');
