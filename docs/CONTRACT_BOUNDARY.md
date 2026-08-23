# 계약 경계와 확인 상태

## 책임 경계

| 구간 | 계약 정본 | 책임 |
|---|---|---|
| CheckOn Backend <-> Kafka <-> Adapter | Backend `docs/contracts/risk-detection-kafka.asyncapi.yaml` | requested/completed/failed 이벤트와 key, correlation/causation 유지 |
| Adapter <-> AI Server | AI OpenAPI `POST /v1/detect` | 세 필수 헤더, 동기 200 응답, 400/409 비재시도, timeout/5xx 제한 재시도 |

Adapter는 CheckOn 백엔드 DB를 조회하지 않습니다. AI 입력의 선택과 가명화는 백엔드 책임이고, Adapter는 수신한 스냅샷만 전달합니다.

## 확인된 불일치

- 프로젝트와 Java 패키지 이름은 오타를 바로잡은 `adapter`, `aiadapter`를 사용합니다.
- Backend AsyncAPI 일부 설명은 Kafka 상대를 AI 서버로 표현하지만 실제 상대는 Adapter입니다.
- Backend Compose의 실제 내부 브로커 주소는 `kafka:19092`이지만 AsyncAPI에는 `kafka:9092` 표기가 남아 있습니다.
- 이 저장소는 백엔드 정책/계약 정본을 임의로 수정하지 않았습니다. 백엔드 문서는 별도 승인 후 동기화해야 합니다.

## 확정된 AI HTTP 계약

- Endpoint는 환경변수 `AI_BASE_URL`과 `AI_DETECT_PATH=/v1/detect`로 주입한다.
- Kafka envelope의 `tenant_alias`, `request_id`, `idempotency_key`를 각각 `X-Tenant-Id`, `X-Request-Id`, `Idempotency-Key`로 전달한다.
- Kafka의 `payload` JSON을 의미나 문자열 표현을 바꾸지 않고 HTTP body로 전달한다.
- read timeout은 60초보다 긴 65초를 사용한다.
- 현재 AI 로컬 서버 호환성을 위해 전송 프로토콜을 HTTP/1.1로 고정한다.
- 400·409는 재시도하지 않고 failed로 변환한다. 네트워크·timeout·5xx만 횟수 제한 재시도한다.
- 동일 key·동일 payload는 동일 결과를 반환하고, 동일 key·다른 payload는 409를 반환한다.
- 응답의 structured signal(`metric`, `observed`, `baseline`, `sample_size`), `advisory`, `lifecycle`, structured evidence(`role`, `observed`, `sample_size`, `occurred_on`), `meta.execution_id`, `meta.versions`를 손실 없이 completed payload에 보존한다.
# Problem Studio to AI mapping

Backend는 진단에서 교사가 선택한 curriculum `skill_node_id`를 `manual_targets`에 담고,
Adapter는 이를 다시 선택하거나 우선순위화하지 않습니다. `language`, `reading`,
`literature`, `speech_writing`, `media`의 `passage`·`work_selection`도 수신 JSON을
검증한 뒤 AI 요청에 그대로 보존합니다. Backend ID는 opaque alias로 유지합니다.

`POST /v1/problems` owns the canonical AI `execution_id`. The Adapter stores it with
the returned `job_id` before polling and ignores changing `execution_id` values from
both GET endpoints. This temporary compatibility rule can be removed only after the
AI team confirms stable identifiers through one job lifecycle.

AI job의 `queued | leased | running | paused`는 transport 실패가 아닙니다. Adapter는
고정 경과 시간으로 실패 처리하지 않고 같은 `job_id`를 계속 polling하며, 상태가 바뀌면
64 KiB 이하 progress 참조 이벤트를 발행합니다. `succeeded`에서는 `set_id`로 목록과
각 slot 상세를 조회해 terminal 참조 1건과 slot별 상세 이벤트를 같은 트랜잭션에 저장합니다.
상세 및 revision 결과는 이벤트당 1 MiB를 넘으면 fail-closed합니다.

Backend의 `problem_generation.revision.requested`는 생성 요청과 같은 Kafka topic에서
별도 revision Inbox로 분기합니다. Adapter는 `ai_refine` POST 후 slot 상세를 다시 조회해
`problem_generation.revision.succeeded`를 발행하며, 409의 reason과 현재 revision 번호를
실패 결과에 보존합니다.

## Adding another worker

Parent counseling and monthly report contracts are not settled, so no topics, HTTP DTOs, state machines, or tables are created for them. After a contract is approved, add a feature package with its own Kafka event, AI DTOs, state machine, result factory, Inbox/Outbox/attempt migration, timeout, and retry policy. Reuse only the fencing value type, retry calculation, executor pattern, and low-cardinality metrics. Give the worker a dedicated bounded scheduler and connect only publication to the Outbox publisher scheduler. Do not merge feature state into a generic JSON `AiJob` table.
