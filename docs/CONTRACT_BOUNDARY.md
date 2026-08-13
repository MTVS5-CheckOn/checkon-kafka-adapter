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
- 응답의 `advisory`, `lifecycle`, evidence, `meta.execution_id`, `meta.versions`를 손실 없이 completed payload에 보존한다.
# Problem Studio to AI mapping

The frontend studio selects an area/type cell while the AI contract requires an
explicit curriculum node. The v1 Backend admits only `language + CONCEPT` and
`language + INFER`; the Adapter maps those cells to separately configurable node IDs.
The current replaceable defaults both point to
`language.grammar.phonological_change`, which is present in the inspected AI graph and
has both type affinities. Reading, literature, speech/writing, media, and other type
tags are rejected before AI HTTP because their evidence/source contracts are not
ready. Backend IDs remain opaque aliases and the mapping does not add student data.

`POST /v1/problems` owns the canonical AI `execution_id`. The Adapter stores it with
the returned `job_id` before polling and ignores changing `execution_id` values from
both GET endpoints. This temporary compatibility rule can be removed only after the
AI team confirms stable identifiers through one job lifecycle.
