-- AI-A 2026-08-20 POST-계약변경: POST는 이제 적재만 하고 즉시 반환한다(적재
-- 시점 status가 이미 종단(succeeded/failed/cancelled)이 아니면 status=queued
-- 로 돌아오고, 실제 결과는 GET 폴링으로 받아야 한다 -- problem-generation과
-- 같은 SUBMIT/POLL 2단계 모델이 필요해졌다.
ALTER TABLE counsel_draft_request_inbox
    ADD COLUMN phase VARCHAR(20) NOT NULL DEFAULT 'SUBMIT',
    ADD COLUMN ai_job_id VARCHAR(100),
    ADD COLUMN ai_execution_id VARCHAR(120);
ALTER TABLE counsel_draft_request_inbox ALTER COLUMN phase DROP DEFAULT;

ALTER TABLE counsel_draft_request_inbox DROP CONSTRAINT ck_counsel_draft_inbox_status;
ALTER TABLE counsel_draft_request_inbox ADD CONSTRAINT ck_counsel_draft_inbox_status CHECK (
    status IN (
        'RECEIVED', 'PROCESSING', 'WAITING', 'RETRY_PENDING', 'OUTCOME_PENDING',
        'OUTCOME_PUBLISHED', 'OUTCOME_DEAD'
    )
);
ALTER TABLE counsel_draft_request_inbox ADD CONSTRAINT ck_counsel_draft_inbox_phase CHECK (phase IN ('SUBMIT', 'POLL'));

ALTER TABLE counsel_draft_attempt DROP CONSTRAINT ck_counsel_draft_attempt_phase;
ALTER TABLE counsel_draft_attempt ADD CONSTRAINT ck_counsel_draft_attempt_phase CHECK (phase IN ('SUBMIT', 'POLL'));

DROP INDEX idx_counsel_draft_inbox_ready;
CREATE INDEX idx_counsel_draft_inbox_ready ON counsel_draft_request_inbox(next_attempt_at, created_at)
    WHERE status IN ('RECEIVED', 'WAITING', 'RETRY_PENDING');

COMMENT ON COLUMN counsel_draft_request_inbox.ai_job_id IS
    'The AI''s own job_id, known only once SUBMIT (POST) returns -- null while phase=SUBMIT is still pending.';
COMMENT ON COLUMN counsel_draft_request_inbox.ai_execution_id IS
    'Canonical execution_id from the SUBMIT (POST) response. Preserved as-is through POLL even if a later GET reports a different value -- POST''s value is authoritative (mirrors problem_generation_request_inbox.ai_execution_id).';
