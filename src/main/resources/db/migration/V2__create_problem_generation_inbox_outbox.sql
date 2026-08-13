CREATE TABLE problem_generation_request_inbox (
    event_id UUID PRIMARY KEY,
    adapter_execution_id UUID NOT NULL UNIQUE,
    tenant_alias VARCHAR(35) NOT NULL,
    problem_request_id UUID NOT NULL,
    problem_execution_id UUID NOT NULL,
    target_index INTEGER NOT NULL,
    request_id VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(100) NOT NULL,
    event_payload JSONB NOT NULL,
    phase VARCHAR(20) NOT NULL,
    status VARCHAR(30) NOT NULL,
    ai_job_id VARCHAR(100),
    http_attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    last_error_code VARCHAR(80),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_problem_generation_inbox_phase CHECK (phase IN ('SUBMIT', 'POLL')),
    CONSTRAINT ck_problem_generation_inbox_status CHECK (
        status IN ('RECEIVED', 'PROCESSING', 'WAITING', 'RETRY_PENDING',
                   'OUTCOME_PENDING', 'OUTCOME_PUBLISHED', 'OUTCOME_DEAD')
    ),
    CONSTRAINT ck_problem_generation_inbox_attempts CHECK (http_attempts >= 0),
    CONSTRAINT ck_problem_generation_inbox_target_index CHECK (target_index >= 0),
    CONSTRAINT ck_problem_generation_inbox_tenant_alias CHECK (
        tenant_alias ~ '^tn_[0-9a-f]{32}$'
    )
);

CREATE INDEX idx_problem_generation_inbox_claim
    ON problem_generation_request_inbox (next_attempt_at, created_at)
    WHERE status IN ('RECEIVED', 'WAITING', 'RETRY_PENDING', 'PROCESSING');

CREATE TABLE problem_generation_outbox (
    event_id UUID PRIMARY KEY,
    source_event_id UUID NOT NULL UNIQUE
        REFERENCES problem_generation_request_inbox(event_id),
    topic VARCHAR(200) NOT NULL,
    message_key VARCHAR(100) NOT NULL,
    event_payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    last_error_code VARCHAR(80),
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT ck_problem_generation_outbox_status CHECK (
        status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'DEAD')
    ),
    CONSTRAINT ck_problem_generation_outbox_attempts CHECK (publish_attempts >= 0),
    CONSTRAINT ck_problem_generation_outbox_message_key CHECK (
        message_key ~ '^tn_[0-9a-f]{32}$'
    )
);

CREATE INDEX idx_problem_generation_outbox_claim
    ON problem_generation_outbox (available_at, created_at)
    WHERE status IN ('PENDING', 'PUBLISHING');
