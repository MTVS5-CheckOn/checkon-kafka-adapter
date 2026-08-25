# CheckOn 문제 출제 Kafka–HTTP Adapter 계약

- 갱신일: 2026-08-23
- Backend 기준: `CheckOn-backend` PR #71
- AI 기준: 전달받은 `AI_BE_COMMUNICATION_MEETING_SPEC.md`, `PROBLEM_GENERATION_AI_BE_INTEGRATION_SPEC.md`

## 1. 책임 경계

```text
CheckOn Backend
  -> checkon.ai.problem-generation.requests.v1
  -> Adapter durable Inbox
  -> AI /v1/problems HTTP
  -> Adapter durable Outbox
  -> checkon.ai.problem-generation.results.v1
  -> CheckOn Backend
```

Adapter는 Backend DB를 조회하지 않고 AI는 Kafka를 소비하지 않는다. Backend가 선택한
opaque tenant/student alias, `skill_node_id`, snapshot hash와 영역별 자료만 전달한다.

## 2. 생성 요청

- event type: `problem_generation.requested`
- schema: `pg-child-request-2` (`pg-child-request-1`은 전환 호환으로만 수신)
- Kafka key: `tenant_id`와 같은 `tn_` alias
- AI POST: `/v1/problems`
- 필수 POST headers: `X-Tenant-Id`, `X-Request-Id`, `Idempotency-Key`

Adapter는 `teacher_manual`, 단일 type tag, 유효한 `manual_targets`를 검증한다. 지원 영역은
`language`, `reading`, `literature`, `speech_writing`, `media`이며 자료 조합은 다음과 같다.

| 영역 | passage | work_selection |
|---|---|---|
| language | 없음 | 없음 |
| reading | 필수 | 없음 |
| literature | 없음 | 필수 |
| speech_writing | 필수 | 없음 |
| media | 필수 | 없음 |

`reading` domain·문장 복잡도·문단 수와 speech/media `source_kind`,
`banned_topics_version=pg-banned-v1`을 AI 호출 전에 검증한다. Adapter가 node나 자료를
임의 생성하지 않는다.

## 3. AI job lifecycle

POST는 `202 + queued`를 반환해야 하며 Adapter는 `job_id`와 POST의
`meta.execution_id`를 먼저 영속화한다. 이후 다음 상태를 별도 worker phase로 보존한다.

```text
queued | leased | running | paused | succeeded | failed | cancelled
```

비종단 상태에서는 `Retry-After`를 advisory로 적용하고 같은 `job_id`를 계속 조회한다.
고정 경과 시간으로 실패시키거나 새 멱등키로 POST하지 않는다. 네트워크·timeout·5xx는
제한 재시도하고 계약 4xx는 종단 결과로 변환한다.

## 4. terminal 및 slot 결과

성공 job의 `result.set_id`를 저장하고 다음 순서로 조회한다.

1. `GET /v1/problems/{set_id}/items`
2. item이 존재하는 각 slot에 `GET /v1/problems/{set_id}/items/{slot_index}`

같은 DB 트랜잭션에서 다음 Outbox 행을 생성한다.

- `worker_job.succeeded`, schema `pg-result-reference-1`: terminal 참조 1건
- `problem_generation.slot.detail`, schema `pg-slot-detail-1`: slot별 1건

terminal에는 본문을 싣지 않고 `set_id`, worker phase, domain status, 요청·처리·미시작 수,
`status_counts`, 공개 실패 사유와 `result_ref`만 넣는다. slot에는 `dropped` 위치를 포함해
상세 원문을 보존한다.

- `item.area_tag`, `type_tag`, `skill_node_id`, `stem`, `choices`, `answer.correct_no`, `rationale`
- `choices[].why_wrong`, `choices[].misconception_tag`
- `verification`, `current_revision_no`, `available_actions`, `revisions`
- `review_reason`, `failure_reason`, `failure_detail`

참조 이벤트는 UTF-8 64 KiB, slot 상세는 1 MiB를 상한으로 하며 초과 시 fail-closed한다.
terminal과 모든 slot의 broker ack가 완료돼야 생성 Inbox를 `OUTCOME_PUBLISHED`로 바꾼다.

## 5. revision

Backend의 `problem_generation.revision.requested`, schema `pg-revision-request-1`을 생성 요청과
같은 topic에서 별도 durable Inbox로 분기한다. 지원 kind는 `ai_refine`뿐이다.

```text
POST /v1/problems/{set_id}/items/{slot_index}/revisions
GET  /v1/problems/{set_id}/items/{slot_index}
```

POST 성공 뒤 상세를 다시 조회하고 `problem_generation.revision.succeeded`, schema
`pg-revision-result-1`을 발행한다. 결과는 1 MiB 이하여야 한다. AI 409 응답의
`REVISION_CONFLICT`, `stale_base_revision | revision_in_progress`, `current_revision_no`는
Backend가 동시성 충돌을 복원할 수 있도록 실패 이벤트에 보존한다.

## 6. 내구성과 보안

- 생성·revision 요청은 event ID와 전체 payload로 멱등 검증한다.
- AI HTTP는 DB 트랜잭션 밖에서 호출한다.
- Inbox와 Outbox claim은 단조 증가 `claim_version`으로 fencing한다.
- 결과와 Outbox는 같은 트랜잭션에 저장한다.
- Kafka payload 및 AI 본문, 실명, 연락처, API key를 로그에 남기지 않는다.
- Adapter가 가명화나 학생 정답 판정을 다시 수행하지 않는다.

## 7. 검증 경계

Testcontainers와 HTTP stub 테스트는 Kafka→Inbox→AI 계약→Outbox→Kafka 경계를 검증한다.
이는 실제 AI 서버와 Backend 화면을 함께 연결한 live E2E 증거가 아니다. live E2E를 주장할
때는 Backend PR #71, Adapter의 이 변경, AI 기준 서버를 동시에 기동해 최소 한 영역의 생성,
slot 복원, revision까지 확인해야 한다.
