# 계약 경계와 확인 상태

## 책임 경계

| 구간 | 계약 정본 | 책임 |
|---|---|---|
| CheckOn Backend <-> Kafka <-> Adapter | Backend `docs/contracts/risk-detection-kafka.asyncapi.yaml` | requested/completed/failed 이벤트와 key, correlation/causation 유지 |
| Adapter <-> AI Server | 기존 Backend `/v1/detect` DTO와 테스트를 바탕으로 한 임시 HTTP 호환 경계 | HTTP endpoint, 인증, timeout, 멱등성의 최종 합의 필요 |

Adapter는 CheckOn 백엔드 DB를 조회하지 않습니다. AI 입력의 선택과 가명화는 백엔드 책임이고, Adapter는 수신한 스냅샷만 전달합니다.

## 확인된 불일치

- 프로젝트와 Java 패키지 이름은 오타를 바로잡은 `adapter`, `aiadapter`를 사용합니다.
- Backend AsyncAPI 일부 설명은 Kafka 상대를 AI 서버로 표현하지만 실제 상대는 Adapter입니다.
- Backend Compose의 실제 내부 브로커 주소는 `kafka:19092`이지만 AsyncAPI에는 `kafka:9092` 표기가 남아 있습니다.
- 이 저장소는 백엔드 정책/계약 정본을 임의로 수정하지 않았습니다. 백엔드 문서는 별도 승인 후 동기화해야 합니다.

## AI 팀 확인 필요

다음 항목이 합의되기 전까지 AI 연동은 기본 비활성화됩니다.

- 실제 LAN URL과 포트, 위험 탐지 endpoint
- 동기 `200 + 결과` 계약 유지 여부 또는 비동기 `202 + jobId` 전환 여부
- 요청/응답 JSON 최종 버전과 버전 호환 정책
- 인증 방식과 자격증명 전달/회전 방식
- connect/read timeout
- 재시도 가능한 HTTP 상태 코드
- `Idempotency-Key` 지원과 동일 key/동일 payload의 동일 결과 보장
- 동일 key/다른 payload의 `409` 보장

현재 구현은 기존 Backend의 동기 `/v1/detect` 계약과 세 헤더를 호환 기준으로 삼았지만, URL·경로·timeout은 환경변수가 없으면 활성화되지 않습니다.
