# 위험 탐지 운영 가이드

## 상태 확인

Adapter DB의 두 테이블이 복구 기준입니다.

- `risk_detection_request_inbox`: 요청 등록, AI 처리 시도, 결과 발행 완료 상태
- `risk_detection_outbox`: completed/failed 이벤트와 Kafka 발행 시도 상태

정상 종착 상태는 Inbox `OUTCOME_PUBLISHED`, Outbox `PUBLISHED`입니다. 계약 또는 AI 오류는 failed 이벤트로 변환되므로, AI 처리 실패 자체가 곧 Adapter 장애를 의미하지는 않습니다.

## 자동 복구

- 처리 중 프로세스 종료: `AI_RISK_DETECTION_LOCK_TIMEOUT` 후 Inbox를 다시 claim합니다.
- Kafka 발행 중 프로세스 종료: `RISK_DETECTION_OUTBOX_LOCK_TIMEOUT` 후 같은 결과 `event_id`를 다시 발행합니다.
- AI 일시 장애: `AI_RISK_DETECTION_RETRY_INITIAL_DELAY`를 기준으로 지수 지연하고 최대 `AI_RISK_DETECTION_MAX_ATTEMPTS`까지 시도합니다.
- Kafka 발행 장애: `RISK_DETECTION_OUTBOX_RETRY_DELAY` 간격으로 최대 `RISK_DETECTION_OUTBOX_MAX_ATTEMPTS`까지 시도합니다.

HTTP client 내부 자동 재시도는 사용하지 않습니다. Kafka 요청 재시도와 AI HTTP 재시도를 중첩하지 않기 위한 결정입니다.

## 수동 확인이 필요한 상태

- 요청 DLT: 역직렬화/계약 오류, 같은 `event_id`의 payload 충돌 또는 Inbox 등록 자체의 반복 실패를 조사합니다.
- Outbox `DEAD`: 브로커/권한/토픽 오류를 먼저 해결한 뒤 Inbox와 Outbox를 하나의 DB 트랜잭션으로 함께 복구해야 합니다.
- Inbox `OUTCOME_DEAD`: 대응 Outbox를 단독으로 `PENDING`으로 바꾸지 않습니다. 두 상태가 어긋나면 발행 확인 트랜잭션이 실패합니다.

수동 SQL을 저장소에 고정하지 않은 이유는 운영 계정 권한과 감사 절차가 아직 확정되지 않았기 때문입니다. 운영 도구를 추가할 때는 대상 `event_id`, 실행자, 사유, 변경 전후 상태를 감사 기록으로 남겨야 합니다.

## 로그와 보안

- Kafka payload와 AI HTTP body 전체를 로그에 남기지 않습니다.
- 원본 강사·학생 UUID나 개인정보를 Adapter에서 새로 추가하지 않습니다.
- 장애 추적에는 opaque alias와 `event_id`, `request_id`, `correlation_id`, `causation_id`만 사용합니다.
- 실제 DB 비밀번호, AI 인증정보와 토큰은 환경변수 또는 비밀 저장소로 주입합니다.
# Problem Generation worker

The problem-generation path consumes one `pg-child-request-1` event per studio target.
Enable both `PROBLEM_GENERATION_KAFKA_ENABLED` and
`AI_PROBLEM_GENERATION_WORKER_ENABLED` only when Kafka, the adapter database, and the
AI `/v1/problems` API are reachable. The adapter persists the raw event, stable
`adapter_execution_id`, the `job_id` and canonical `execution_id` returned by POST,
and the current `SUBMIT`/`POLL` phase. A stale lock is reclaimed after
`AI_PROBLEM_GENERATION_LOCK_TIMEOUT`.

Adapter restart recovery and AI restart recovery are different guarantees. Once POST
identifiers are stored, an Adapter restart resumes polling the same `job_id`; it does
not make the AI process resume an in-memory queued job. If AI reports
`detail.reason=result_unavailable_after_restart`, the Adapter publishes a terminal
failed result with that reason and never submits the POLL-phase request again.

Terminal AI items are flattened into a `worker_job.succeeded` result event and written
to `problem_generation_outbox` in the same transaction that closes the inbox work.
Kafka publication is retried from that outbox. Contract-invalid Kafka requests go to
the request topic's `.dlt`; exhausted AI calls produce a durable `worker_job.failed`
result so the backend can aggregate `PARTIAL_SUCCESS` across child targets.

The v1 generation boundary accepts only `language + CONCEPT` and
`language + INFER`. Both node IDs are environment-configurable. Other cells are
stopped before HTTP with `NO_EVIDENCE_READY_TARGET`. `rejected_insufficient` remains
a forward-compatible parser value but is not expected from the wired v1 AI path.
