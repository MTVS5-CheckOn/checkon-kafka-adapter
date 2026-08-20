ALTER TABLE risk_detection_request_inbox ADD COLUMN claim_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE risk_detection_outbox ADD COLUMN claim_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE problem_generation_request_inbox ADD COLUMN claim_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE problem_generation_outbox ADD COLUMN claim_version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE risk_detection_request_inbox ADD CONSTRAINT ck_risk_detection_inbox_claim_version CHECK (claim_version >= 0);
ALTER TABLE risk_detection_outbox ADD CONSTRAINT ck_risk_detection_outbox_claim_version CHECK (claim_version >= 0);
ALTER TABLE problem_generation_request_inbox ADD CONSTRAINT ck_problem_generation_inbox_claim_version CHECK (claim_version >= 0);
ALTER TABLE problem_generation_outbox ADD CONSTRAINT ck_problem_generation_outbox_claim_version CHECK (claim_version >= 0);

CREATE TABLE risk_detection_attempt (
    attempt_id UUID PRIMARY KEY,
    source_event_id UUID NOT NULL REFERENCES risk_detection_request_inbox(event_id),
    phase VARCHAR(20) NOT NULL,
    claim_version BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    result_status VARCHAR(30),
    error_code VARCHAR(80),
    stale_reclaim BOOLEAN NOT NULL DEFAULT FALSE,
    superseded BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT ck_risk_detection_attempt_phase CHECK (phase IN ('AI_HTTP')),
    CONSTRAINT ck_risk_detection_attempt_claim_version CHECK (claim_version > 0)
);
CREATE UNIQUE INDEX uq_risk_detection_attempt_claim ON risk_detection_attempt(source_event_id, claim_version);

CREATE TABLE problem_generation_attempt (
    attempt_id UUID PRIMARY KEY,
    source_event_id UUID NOT NULL REFERENCES problem_generation_request_inbox(event_id),
    phase VARCHAR(20) NOT NULL,
    claim_version BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    result_status VARCHAR(30),
    error_code VARCHAR(80),
    stale_reclaim BOOLEAN NOT NULL DEFAULT FALSE,
    superseded BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT ck_problem_generation_attempt_phase CHECK (phase IN ('SUBMIT', 'POLL')),
    CONSTRAINT ck_problem_generation_attempt_claim_version CHECK (claim_version > 0)
);
CREATE UNIQUE INDEX uq_problem_generation_attempt_claim ON problem_generation_attempt(source_event_id, claim_version);

CREATE TABLE outbox_publish_attempt (
    attempt_id UUID PRIMARY KEY,
    worker_kind VARCHAR(40) NOT NULL,
    outbox_event_id UUID NOT NULL,
    source_event_id UUID NOT NULL,
    claim_version BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    result_status VARCHAR(30),
    error_code VARCHAR(80),
    stale_reclaim BOOLEAN NOT NULL DEFAULT FALSE,
    superseded BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT ck_outbox_attempt_worker_kind CHECK (worker_kind IN ('risk_detection', 'problem_generation')),
    CONSTRAINT ck_outbox_attempt_claim_version CHECK (claim_version > 0),
    CONSTRAINT uq_outbox_attempt_claim UNIQUE (worker_kind, outbox_event_id, claim_version)
);

DROP INDEX idx_risk_detection_inbox_claim;
CREATE INDEX idx_risk_detection_inbox_ready ON risk_detection_request_inbox(next_attempt_at, created_at)
    WHERE status IN ('RECEIVED', 'RETRY_PENDING');
CREATE INDEX idx_risk_detection_inbox_stale ON risk_detection_request_inbox(locked_at, created_at)
    WHERE status = 'PROCESSING';

DROP INDEX idx_problem_generation_inbox_claim;
CREATE INDEX idx_problem_generation_inbox_ready ON problem_generation_request_inbox(next_attempt_at, created_at)
    WHERE status IN ('RECEIVED', 'WAITING', 'RETRY_PENDING');
CREATE INDEX idx_problem_generation_inbox_stale ON problem_generation_request_inbox(locked_at, created_at)
    WHERE status = 'PROCESSING';

DROP INDEX idx_risk_detection_outbox_claim;
CREATE INDEX idx_risk_detection_outbox_ready ON risk_detection_outbox(available_at, created_at)
    WHERE status = 'PENDING';
CREATE INDEX idx_risk_detection_outbox_stale ON risk_detection_outbox(locked_at, created_at)
    WHERE status = 'PUBLISHING';

DROP INDEX idx_problem_generation_outbox_claim;
CREATE INDEX idx_problem_generation_outbox_ready ON problem_generation_outbox(available_at, created_at)
    WHERE status = 'PENDING';
CREATE INDEX idx_problem_generation_outbox_stale ON problem_generation_outbox(locked_at, created_at)
    WHERE status = 'PUBLISHING';
