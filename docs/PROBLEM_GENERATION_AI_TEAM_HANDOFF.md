# CheckOn 문제 출제 Adapter 구현 현황 및 AI팀 연동 요청

작성 기준: 2026-08-13

대상 저장소: `checkon-kafka-adapter`

대상 브랜치: `codex/feature/ai-problem-cycle/48`

## 1. 전달 목적

Backend가 발행한 문제 출제 child 이벤트를 Adapter가 받아 AI HTTP API를 호출하고,
완료 문항을 다시 Kafka 결과 이벤트로 Backend에 전달하는 경계를 구현했습니다.

이 문서는 다음 내용을 AI팀에 전달하기 위한 현재 연동 기준입니다.

- Adapter에서 구현을 마친 범위
- AI HTTP API에 기대하는 요청·응답 계약
- 식별자와 재시작 처리 기준
- v1에서 실제로 허용하는 영역×유형 셀
- AI팀 확인 또는 제공이 필요한 항목

AI 저장소의 구현 완료를 주장하는 문서가 아닙니다. 최종 완료 판정은 AI팀 공식 서버와의
E2E 검증 후에 가능합니다.

## 2. 전체 호출 흐름

```text
Frontend
  -> CheckOn Backend REST
  -> problem_generation.requested (Kafka, child 단위)
  -> Adapter Inbox
  -> POST /v1/problems
  -> GET /v1/problems/{job_id}
  -> GET /v1/problems/{job_id}/items
  -> Adapter Outbox
  -> worker_job.succeeded | worker_job.failed (Kafka)
  -> CheckOn Backend DB
  -> Frontend review/save/publish
```

Adapter는 Backend DB를 조회하지 않으며, AI도 Kafka에 직접 연결하지 않습니다.

## 3. Backend에서 Adapter로 전달되는 Kafka 요청

- topic: `checkon.ai.problem-generation.requests.v1`
- schema version: `pg-child-request-1`
- key: `tenant_id`와 동일한 opaque tenant alias
- child 기준: 영역×유형 셀 하나당 한 이벤트

Adapter가 사용하는 핵심 필드는 다음과 같습니다.

```json
{
  "event_id": "<uuid>",
  "event_type": "problem_generation.requested",
  "tenant_id": "tn_<32 lowercase hex>",
  "schema_version": "pg-child-request-1",
  "correlation_id": "<problem_request_id>",
  "payload": {
    "problem_request_id": "<uuid>",
    "problem_execution_id": "<uuid>",
    "target_index": 0,
    "idempotency_key": "<stable child key>",
    "request": {
      "target_kind": "student",
      "target_ref": "<opaque alias>",
      "target_source": "teacher_manual",
      "taxonomy_version": "v1",
      "area_tag": "language",
      "type_tags": ["concept"],
      "item_format": "mcq",
      "count": 1
    }
  }
}
```

동일 `event_id`와 동일 payload의 재전달은 Inbox에서 멱등 처리합니다. 동일 `event_id`에
다른 payload가 오면 계약 충돌로 DLT에 격리합니다.

## 4. Adapter에서 AI로 보내는 HTTP 계약

모든 호출에 다음 헤더를 사용합니다.

| Header | 값 | 용도 |
|---|---|---|
| `X-Tenant-Id` | Kafka의 opaque tenant alias | 테넌트 경계 |
| `X-Request-Id` | child 요청의 추적 ID | HTTP 시도 추적 |
| `Idempotency-Key` | child의 안정적인 멱등 키 | POST 중복 실행 방지 |

`Idempotency-Key`는 POST에만 전송합니다. Adapter는 HTTP client 내부 자동 재시도를
사용하지 않고, Inbox 상태와 횟수를 기준으로 재시도를 통제합니다.

### 4.1 제출

```http
POST /v1/problems
```

Adapter는 Backend child의 `request`를 AI body로 변환합니다. `teacher_manual` 요청에는
`manual_targets`를 보완합니다.

필수 응답:

```json
{
  "data": {
    "job_id": "<stable job id>",
    "status": "queued"
  },
  "error": null,
  "meta": {
    "execution_id": "<stable execution id>"
  }
}
```

`execution_id`가 `data`에 있는 형태도 읽을 수 있지만, 현재 정본은 POST 응답에서 얻은
값이라는 점이 중요합니다.

### 4.2 job 조회

```http
GET /v1/problems/{job_id}
```

Adapter가 인식하는 종단 상태는 다음과 같습니다.

- 성공: `succeeded`
- 실패: `failed`, `cancelled`
- 그 외 상태: 같은 `job_id`를 polling

### 4.3 문항 목록 조회

```http
GET /v1/problems/{job_id}/items
```

Adapter는 성공 시 다음 필드를 필요로 합니다.

```json
{
  "data": {
    "job_id": "<job id>",
    "set_id": "<set id>",
    "items": [
      {
        "item_id": "<item id>",
        "status": "needs_review",
        "item": {
          "stem": "문제 본문",
          "choices": [{"no": 1, "text": "선택지"}],
          "answer": {"correct_no": 1},
          "rationale": "해설"
        }
      }
    ]
  },
  "error": null,
  "meta": {
    "execution_id": "<ignored until AI fix>",
    "versions": {}
  }
}
```

현재 Adapter 계약의 목록 경로 식별자는 `job_id`입니다. AI팀의 공식 계약이 `set_id`만
허용한다면, POST/job 응답에서 Adapter가 `set_id`를 얻을 수 있는 시점과 조회 순서를 함께
조정해야 합니다.

## 5. 식별자 보존 규칙

한 child의 식별자는 서로 다른 의미를 가지며 합치지 않습니다.

| 식별자 | 소유자 | Adapter 처리 |
|---|---|---|
| `problem_request_id` | Backend | 부모 요청 상관관계로 보존 |
| `problem_execution_id` | Backend | child 정본으로 보존 |
| `target_index` | Backend | child 순서로 보존 |
| `adapter_execution_id` | Adapter | 최초 Inbox 등록 시 생성하고 고정 |
| `job_id` | AI | POST 응답에서 저장하고 polling에 사용 |
| `execution_id` | AI | POST 응답 값을 정본으로 저장 |
| `set_id` | AI | 성공 items 응답에서 저장하여 Backend에 전달 |

두 GET endpoint가 호출마다 새로운 `execution_id`를 반환할 수 있다는 공유사항에 맞춰,
Adapter는 다음과 같이 구현했습니다.

1. POST 성공 직후 `job_id`와 `execution_id`를 같은 Inbox 행에 저장합니다.
2. 이후 GET 응답의 `execution_id`는 결과 식별자로 사용하지 않습니다.
3. Kafka 성공·실패 이벤트에는 저장된 POST `execution_id`만 넣습니다.

이 임시 방어 로직은 AI팀에서 한 job 생명주기 동안 식별자가 안정적으로 유지됨을 확인한
후에만 제거할 수 있습니다.

## 6. v1 유효 셀과 node 매핑

현재 Backend가 출제 요청을 허용하는 셀은 두 개입니다.

- `language × CONCEPT`
- `language × INFER`

Adapter 기본 node는 두 셀 모두 다음 값입니다.

```text
language.grammar.phonological_change
```

두 셀의 node는 별도 환경변수로 교체할 수 있습니다.

```text
AI_PROBLEM_GENERATION_LANGUAGE_CONCEPT_NODE
AI_PROBLEM_GENERATION_LANGUAGE_INFER_NODE
```

현재 기본값은 AI 그래프에서 `language` 영역이며 `concept`, `infer` affinity를 모두 가진
노드로 확인한 임시 안전값입니다. AI팀의 확정 node catalog가 전달되면 코드 변경 없이
환경값으로 우선 교체하고, 정식 기본값 변경은 별도 계약 변경으로 반영할 수 있습니다.

다음 셀은 v1에서 AI를 호출하지 않습니다.

- `reading`
- `literature`
- `speech_writing`
- `media`
- `language × FACT`
- `language × CRITIC`

이 경우 Adapter는 `NO_EVIDENCE_READY_TARGET` 실패 결과를 생성합니다. 임의 passage나
work/material 기본값을 만들어 AI에 보내지 않습니다.

## 7. 재시작과 재시도 의미

다음 두 보장은 구분합니다.

```text
Adapter 재시작 후 저장된 job polling 재개
!=
AI 재시작 후 queued job 실행 재개
```

Adapter는 Inbox에 저장된 `job_id`, POST `execution_id`, `POLL` phase를 이용해 Adapter
재시작 후 같은 job 조회를 재개합니다. AI 내부의 in-memory queued 작업을 재실행시키지는
않습니다.

AI가 다음 응답을 주면:

```json
{
  "error": {
    "detail": {
      "reason": "result_unavailable_after_restart"
    }
  }
}
```

Adapter는 이를 비일시 종단 실패로 보존하고 `worker_job.failed`를 발행합니다. 이 상태에서
POST를 다시 호출하지 않으므로 중복 job을 만들지 않습니다. 일반적인 404는 별도
`AI_HTTP_404`로 유지합니다.

네트워크 오류, timeout, 408, 429, 5xx만 제한적으로 재시도합니다. 400, 404, 409와 계약
오류는 재시도하지 않습니다.

## 8. Adapter에서 Backend로 보내는 결과

- topic: `checkon.ai.problem-generation.results.v1`
- schema version: `worker-job-1`
- 성공: `worker_job.succeeded`
- 실패: `worker_job.failed`

성공 결과의 핵심 payload:

```json
{
  "worker_kind": "problem_generation",
  "problem_request_id": "<uuid>",
  "problem_execution_id": "<uuid>",
  "target_index": 0,
  "adapter_execution_id": "<uuid>",
  "job_id": "<ai job id>",
  "execution_id": "<POST execution id>",
  "set_id": "<ai set id>",
  "result_status": "completed",
  "result": {
    "set_id": "<ai set id>",
    "items": []
  },
  "versions": {}
}
```

결과 이벤트는 Inbox 종단 전환과 같은 DB 트랜잭션에서 Outbox에 저장하고, Kafka broker
확인 후에만 `OUTCOME_PUBLISHED`로 전환합니다.

## 9. AI팀 공유사항 반영 상태

| 공유사항 | Adapter 반영 |
|---|---|
| `rejected_insufficient`는 v1 도달 불가 | 정상 AI 응답으로 기대하지 않음. 호환 파서 값만 유지 |
| 기존 job 조회와 queued 실행 재개는 다른 의미 | 문서·테스트에서 분리 |
| `result_unavailable_after_restart` 임시 사유 | 404 body에서 추출하고 비재시도 종단 실패로 전달 |
| GET 두 곳의 `execution_id`가 불안정 | POST 값을 DB 정본으로 저장하고 GET 값 무시 |
| revision 미지원 kind/area 사유 코드 | 현재 generation worker 호출 범위 밖. Step3 연동 시 사용 예정 |

`rejected_insufficient` fixture가 필요하면 실제 v1 실행 결과가 아니라 합성 계약 이벤트로
표시해야 합니다.

## 10. 구현 및 검증 완료 범위

Adapter에서 다음을 구현했습니다.

- Kafka 요청 계약 검증과 DLT
- PostgreSQL Inbox 멱등 처리
- POST/job/items HTTP 호출
- POST `execution_id`와 `job_id` 영속화
- polling 및 stale lock 재claim
- HTTP 제한 재시도
- AI 재시작 결과 유실 사유 보존
- 성공/실패 결과 Outbox
- Kafka 결과 발행과 재발행
- v1 유효 셀 제한 및 환경 기반 node 매핑

검증 명령:

```powershell
.\gradlew.bat clean test --no-daemon
```

검증 결과:

- 54 tests
- failures 0
- errors 0
- skipped 0
- Kafka 요청 → PostgreSQL Inbox → AI 계약 Stub → PostgreSQL Outbox → Kafka 결과 발행
  수직 통합 테스트 통과

이 검증은 Adapter 내부에서 AI HTTP 계약 Stub을 사용한 결과입니다. AI팀 공식 서버와의
최종 E2E 결과는 아닙니다.

## 11. AI팀 확인 및 제공 요청

최종 연동을 위해 다음을 부탁드립니다.

1. `language × CONCEPT`, `language × INFER`의 확정 evidence-ready node catalog
2. `POST /v1/problems`의 `job_id`, `execution_id` 위치와 안정성 확인
3. 문항 목록 endpoint가 공식적으로 `job_id` 또는 `set_id` 중 무엇을 정본으로 쓰는지 확인
4. `result_unavailable_after_restart` 임시 사유 코드 적용 여부와 제거 시점
5. AI 공식 서버에서 사용할 성공·실패·timeout·재시작 fixture
6. 공식 서버 준비 후 Backend → Kafka → Adapter → AI → Adapter → Kafka → Backend E2E 일정

Step3 revision 연동 시에는 공유해주신 다음 계약을 그대로 사용하겠습니다.

- 지원: `language + ai_refine`
- 미지원 kind: `teacher_direct`, `rollback`
- 미지원 area: `reading`, `literature`, `speech_writing`, `media`
- HTTP 400 / `INVALID_SCHEMA`
- `revision_kind_not_implemented` 또는 `revision_area_not_implemented`
- 거절 시 revision 생성, 원장 적재, LLM 호출 모두 0건
