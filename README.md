# checkon-kafka-adapter

CheckOn 백엔드의 Kafka 이벤트와 HTTP만 제공하는 AI 서버를 연결하는 독립 애플리케이션입니다. 첫 번째 수직 단위는 위험 탐지이며, AI 서버는 Kafka에 직접 연결하지 않습니다.

```text
CheckOn Backend -> requested topic -> Adapter Inbox
Adapter worker -> AI HTTP API -> Adapter Outbox
Adapter Outbox -> completed/failed topic -> CheckOn Backend
```

## 현재 구현 범위

- `checkon.risk-detection.requested.v1` 소비와 계약 검증
- PostgreSQL Inbox를 이용한 `event_id` 멱등 처리와 payload 충돌 차단
- `X-Tenant-Id`, `X-Request-Id`, `Idempotency-Key`를 유지하는 AI HTTP 호출
- 요청 스냅샷 밖의 학생·반·근거를 거부하는 AI 응답 검증
- 일시적인 HTTP 장애의 제한적 지연 재시도와 계약 오류의 즉시 실패 처리
- PostgreSQL Outbox와 브로커 확인 응답 기반 completed/failed 발행
- 요청 DLT, stale lock 복구, 동일 결과 이벤트 ID 재발행
- Testcontainers Kafka/PostgreSQL 수직 통합 검증
- 문제 출제 child Kafka 요청, AI 제출·polling·items 조회, 결과 Outbox 발행
- AI POST `execution_id` 정본 영속화와 GET 식별자 변동 차단

## 기술 기준

- Java 25
- Spring Boot 4.1.0
- Spring Kafka, Spring RestClient, JDBC, Flyway
- PostgreSQL
- Testcontainers Kafka/PostgreSQL

## 로컬 실행

CheckOn 백엔드 Compose의 Kafka를 호스트 프로세스에서 사용할 때 주소는 `localhost:9094`입니다. Docker 네트워크 안에서 실행한다면 현재 백엔드 Compose의 내부 주소인 `kafka:19092`를 사용해야 합니다. 기존 AsyncAPI의 `kafka:9092` 표기는 실제 Compose와 다릅니다.

1. `.env.example`을 복사해 `.env`를 만들고 실제 로컬 값을 설정합니다. 애플리케이션은 이 파일을 자동으로 읽으며, `.env`는 Git에서 제외됩니다. 배포 환경에서는 같은 이름의 환경변수를 사용합니다.
2. Adapter 전용 PostgreSQL 데이터베이스와 계정을 준비합니다.
3. AI 연동 주소와 timeout을 확인하고, 로컬 왕복 테스트에서는 아래 세 활성화 값을 `true`로 설정합니다.
4. 저장소 루트에서 다음을 실행합니다.

```powershell
.\gradlew.bat clean test
.\gradlew.bat bootRun
```

로컬 왕복 테스트에서는 CheckOn Backend를 먼저 완전히 시작한 다음 이 Adapter를 시작합니다. Backend의 `scripts/run-local-kafka.ps1`은 과거 환경변수를 지우기 위해 Gradle daemon을 정리하므로, Adapter를 먼저 켜면 Adapter의 Gradle 실행도 함께 종료될 수 있습니다.

Kafka 소비와 AI worker를 실제로 활성화하려면 다음 세 값을 모두 설정합니다.

```text
RISK_DETECTION_KAFKA_ENABLED=true
AI_RISK_DETECTION_ENABLED=true
AI_RISK_DETECTION_WORKER_ENABLED=true
```

문제 출제 한 사이클은 아래 두 값을 함께 활성화합니다. AI팀의 최종 노드 카탈로그가
현재 기본값과 다르면 두 node 환경변수도 함께 교체합니다.

```text
PROBLEM_GENERATION_KAFKA_ENABLED=true
AI_PROBLEM_GENERATION_WORKER_ENABLED=true
AI_PROBLEM_GENERATION_LANGUAGE_CONCEPT_NODE=language.grammar.phonological_change
AI_PROBLEM_GENERATION_LANGUAGE_INFER_NODE=language.grammar.phonological_change
```

CheckOn Backend의 `RISK_DETECTION_HTTP_ADAPTER_ENABLED`는 반드시 `false`로 둡니다. 독립 Adapter와 Backend 내장 fallback을 동시에 켜면 같은 requested 이벤트가 두 번 처리됩니다.

`AI_RISK_DETECTION_LOCK_TIMEOUT`은 connect timeout과 read timeout의 합보다 충분히 길게 설정해야 합니다. 그렇지 않으면 오래 걸리는 HTTP 요청이 stale 작업으로 오인될 수 있습니다.

AI HTTP 호출은 HTTP/1.1로 고정합니다. 현재 AI 로컬 서버는 JDK HTTP 클라이언트의 기본 HTTP/2 업그레이드 요청에서 body를 빈 값처럼 읽어 `INVALID_SCHEMA`를 반환할 수 있습니다.

## 신뢰성 모델

- Kafka 요청은 at-least-once로 처리합니다.
- 동일 `event_id`와 동일 payload는 한 번만 Inbox에 저장합니다.
- 동일 `event_id`의 다른 payload는 계약 충돌로 처리하고 요청 DLT로 보냅니다.
- AI 호출은 DB 트랜잭션 밖에서 수행합니다.
- AI 호출 성공 후 프로세스가 종료되면 stale lock 복구 과정에서 같은 `Idempotency-Key`로 다시 호출될 수 있습니다. 따라서 AI 서버의 멱등성 보장이 운영 활성화의 선결 조건입니다.
- completed/failed 결과는 업무 상태와 같은 트랜잭션에서 Outbox에 기록합니다.
- Kafka 브로커가 발행을 확인하기 전에는 결과를 완료로 표시하지 않습니다.
- payload 전체와 개인정보는 운영 로그에 남기지 않습니다.

자세한 책임 경계와 미확정 항목은 [계약 경계](docs/CONTRACT_BOUNDARY.md), 운영 복구 절차는 [운영 가이드](docs/OPERATIONS.md)를 참고하세요.
