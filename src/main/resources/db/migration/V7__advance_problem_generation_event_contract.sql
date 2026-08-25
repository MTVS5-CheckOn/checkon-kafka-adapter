ALTER TABLE problem_generation_request_inbox
    ADD COLUMN ai_set_id VARCHAR(120),
    ADD COLUMN worker_phase VARCHAR(20) NOT NULL DEFAULT 'queued',
    ADD COLUMN domain_status VARCHAR(40);

ALTER TABLE problem_generation_outbox
    DROP CONSTRAINT problem_generation_outbox_source_event_id_key,
    ADD COLUMN event_kind VARCHAR(160) NOT NULL DEFAULT 'legacy-terminal',
    ADD COLUMN slot_index INTEGER,
    ADD COLUMN sequence_no INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN terminal_outcome BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN revision_source_event_id UUID;

ALTER TABLE problem_generation_outbox
    ADD CONSTRAINT ck_problem_generation_outbox_sequence CHECK (sequence_no >= 0);

CREATE UNIQUE INDEX uq_problem_generation_outbox_logical_event
    ON problem_generation_outbox (source_event_id, event_kind, slot_index) NULLS NOT DISTINCT;

CREATE TABLE problem_generation_revision_inbox (
    event_id UUID PRIMARY KEY,
    revision_request_id UUID NOT NULL UNIQUE,
    generation_source_event_id UUID NOT NULL REFERENCES problem_generation_request_inbox(event_id),
    tenant_alias VARCHAR(35) NOT NULL,
    problem_request_id UUID NOT NULL,
    problem_execution_id UUID NOT NULL,
    set_id VARCHAR(120) NOT NULL,
    slot_index INTEGER NOT NULL,
    request_id VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(200) NOT NULL,
    event_payload JSONB NOT NULL,
    request_payload JSONB NOT NULL,
    status VARCHAR(30) NOT NULL,
    http_attempts INTEGER NOT NULL DEFAULT 0,
    claim_version BIGINT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_at TIMESTAMPTZ,
    last_error_code VARCHAR(80),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_problem_generation_revision_status CHECK (
        status IN ('RECEIVED', 'PROCESSING', 'RETRY_PENDING', 'OUTCOME_PENDING',
                   'OUTCOME_PUBLISHED', 'OUTCOME_DEAD')
    ),
    CONSTRAINT ck_problem_generation_revision_slot CHECK (slot_index >= 0),
    CONSTRAINT ck_problem_generation_revision_attempts CHECK (http_attempts >= 0),
    CONSTRAINT ck_problem_generation_revision_claim CHECK (claim_version >= 0),
    CONSTRAINT ck_problem_generation_revision_tenant CHECK (tenant_alias ~ '^tn_[0-9a-f]{32}$')
);

ALTER TABLE problem_generation_outbox
    ADD CONSTRAINT fk_problem_generation_outbox_revision_source
    FOREIGN KEY (revision_source_event_id) REFERENCES problem_generation_revision_inbox(event_id);

CREATE INDEX idx_problem_generation_revision_ready
    ON problem_generation_revision_inbox (next_attempt_at, created_at)
    WHERE status IN ('RECEIVED', 'RETRY_PENDING');

CREATE INDEX idx_problem_generation_revision_stale
    ON problem_generation_revision_inbox (locked_at, created_at)
    WHERE status = 'PROCESSING';

CREATE TABLE problem_generation_revision_attempt (
    attempt_id UUID PRIMARY KEY,
    source_event_id UUID NOT NULL REFERENCES problem_generation_revision_inbox(event_id),
    claim_version BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    result_status VARCHAR(30),
    error_code VARCHAR(80),
    stale_reclaim BOOLEAN NOT NULL DEFAULT FALSE,
    superseded BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT uq_problem_generation_revision_attempt UNIQUE (source_event_id, claim_version)
);
