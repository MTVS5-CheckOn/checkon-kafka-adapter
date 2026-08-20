CREATE TABLE counsel_draft_request_inbox (
    event_id UUID PRIMARY KEY,
    tenant_alias VARCHAR(35) NOT NULL,
    run_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    request_id VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(200) NOT NULL,
    snapshot_hash VARCHAR(100) NOT NULL,
    event_payload JSONB NOT NULL,
    status VARCHAR(30) NOT NULL,
    http_attempts INTEGER NOT NULL DEFAULT 0,
    claim_version BIGINT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    last_error_code VARCHAR(60),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_counsel_draft_inbox_status CHECK (
        status IN (
            'RECEIVED', 'PROCESSING', 'RETRY_PENDING', 'OUTCOME_PENDING',
            'OUTCOME_PUBLISHED', 'OUTCOME_DEAD'
        )
    ),
    CONSTRAINT ck_counsel_draft_inbox_attempts CHECK (http_attempts >= 0),
    CONSTRAINT ck_counsel_draft_inbox_claim_version CHECK (claim_version >= 0),
    CONSTRAINT ck_counsel_draft_inbox_tenant_alias CHECK (
        tenant_alias ~ '^tn_[0-9a-f]{32}$'
    )
);

CREATE INDEX idx_counsel_draft_inbox_ready ON counsel_draft_request_inbox(next_attempt_at, created_at)
    WHERE status IN ('RECEIVED', 'RETRY_PENDING');
CREATE INDEX idx_counsel_draft_inbox_stale ON counsel_draft_request_inbox(locked_at, created_at)
    WHERE status = 'PROCESSING';

CREATE TABLE counsel_draft_outbox (
    event_id UUID PRIMARY KEY,
    source_event_id UUID NOT NULL UNIQUE
        REFERENCES counsel_draft_request_inbox(event_id),
    topic VARCHAR(200) NOT NULL,
    message_key VARCHAR(100) NOT NULL,
    event_payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    claim_version BIGINT NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    last_error_code VARCHAR(60),
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT ck_counsel_draft_outbox_status CHECK (
        status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'DEAD')
    ),
    CONSTRAINT ck_counsel_draft_outbox_attempts CHECK (publish_attempts >= 0),
    CONSTRAINT ck_counsel_draft_outbox_claim_version CHECK (claim_version >= 0),
    CONSTRAINT ck_counsel_draft_outbox_message_key CHECK (
        message_key ~ '^tn_[0-9a-f]{32}$'
    )
);

CREATE INDEX idx_counsel_draft_outbox_ready ON counsel_draft_outbox(available_at, created_at)
    WHERE status = 'PENDING';
CREATE INDEX idx_counsel_draft_outbox_stale ON counsel_draft_outbox(locked_at, created_at)
    WHERE status = 'PUBLISHING';

CREATE TABLE counsel_draft_attempt (
    attempt_id UUID PRIMARY KEY,
    source_event_id UUID NOT NULL REFERENCES counsel_draft_request_inbox(event_id),
    phase VARCHAR(20) NOT NULL,
    claim_version BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    result_status VARCHAR(30),
    error_code VARCHAR(80),
    stale_reclaim BOOLEAN NOT NULL DEFAULT FALSE,
    superseded BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT ck_counsel_draft_attempt_phase CHECK (phase IN ('AI_HTTP')),
    CONSTRAINT ck_counsel_draft_attempt_claim_version CHECK (claim_version > 0)
);
CREATE UNIQUE INDEX uq_counsel_draft_attempt_claim ON counsel_draft_attempt(source_event_id, claim_version);

-- outbox_publish_attempt (V4) is a shared table across features -- widen its
-- worker_kind allowlist instead of creating a parallel per-feature table.
ALTER TABLE outbox_publish_attempt DROP CONSTRAINT ck_outbox_attempt_worker_kind;
ALTER TABLE outbox_publish_attempt ADD CONSTRAINT ck_outbox_attempt_worker_kind
    CHECK (worker_kind IN ('risk_detection', 'problem_generation', 'counsel_draft'));
