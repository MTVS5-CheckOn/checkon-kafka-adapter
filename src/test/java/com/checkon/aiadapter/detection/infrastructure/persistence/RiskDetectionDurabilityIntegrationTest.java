package com.checkon.aiadapter.detection.infrastructure.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionInboxRepository.Registration;
import com.checkon.aiadapter.detection.ai.AiRiskDetectionClient;
import com.checkon.aiadapter.detection.ai.AiDetectionResponse;
import com.checkon.aiadapter.detection.application.InvalidAiDetectionResponseException;
import com.checkon.aiadapter.detection.application.RiskDetectionOutcomeCoordinator;
import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

import tools.jackson.databind.ObjectMapper;

@Testcontainers
@SpringBootTest(properties = {
	"checkon.ai.risk-detection.worker-enabled=true",
	"checkon.ai.risk-detection.poll-delay=1h",
	"checkon.kafka.risk-detection.enabled=false"
})
class RiskDetectionDurabilityIntegrationTest {

	@Container
	static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.4");

	@DynamicPropertySource
	static void databaseProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		registry.add("spring.datasource.username", POSTGRES::getUsername);
		registry.add("spring.datasource.password", POSTGRES::getPassword);
		registry.add("spring.flyway.url", POSTGRES::getJdbcUrl);
		registry.add("spring.flyway.user", POSTGRES::getUsername);
		registry.add("spring.flyway.password", POSTGRES::getPassword);
	}

	@Autowired
	RiskDetectionInboxRepository inboxRepository;

	@Autowired
	RiskDetectionOutboxRepository outboxRepository;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	ObjectMapper objectMapper;

	@MockitoBean
	AiRiskDetectionClient aiClient;

	@Autowired
	RiskDetectionOutcomeCoordinator outcomeCoordinator;

	private RiskDetectionRequestedEvent event;
	private Instant now;

	@BeforeEach
	void setUp() throws Exception {
		jdbcTemplate.update("DELETE FROM risk_detection_outbox");
		jdbcTemplate.update("DELETE FROM risk_detection_request_inbox");
		event = objectMapper.readValue(readFixture(), RiskDetectionRequestedEvent.class);
		now = Instant.parse("2026-08-12T00:00:00Z");
	}

	@Test
	@DisplayName("Given Kafka 원본 이벤트 When Inbox에서 claim하면 Then AI payload 표현을 그대로 복구한다")
	void preservesRawAiPayloadThroughInbox() throws Exception {
		// Given
		String rawEvent = readFixture();
		RiskDetectionRequestedEvent decoded = objectMapper.readValue(
			rawEvent, RiskDetectionRequestedEvent.class);
		inboxRepository.register(decoded, rawEvent, now);

		// When
		var claimed = inboxRepository.claimNext(now, Duration.ofSeconds(30))
			.orElseThrow();

		// Then
		assertThat(objectMapper.readTree(claimed.requestBody()))
			.isEqualTo(objectMapper.readTree(rawEvent).get("payload"));
	}

	@Test
	@DisplayName("Given 같은 event_id When 동일 payload가 재수신되면 Then 한 행만 저장하고 중복으로 판정한다")
	void deduplicatesIdenticalRequestEvent() {
		// Given/When
		Registration first = inboxRepository.register(event, now);
		Registration duplicate = inboxRepository.register(event, now.plusSeconds(1));

		// Then
		assertThat(first).isEqualTo(Registration.NEW);
		assertThat(duplicate).isEqualTo(Registration.DUPLICATE);
		assertThat(rowCount("risk_detection_request_inbox")).isEqualTo(1);
	}

	@Test
	@DisplayName("Given 같은 event_id When 다른 payload가 재수신되면 Then 계약 충돌로 판정한다")
	void rejectsSameEventIdWithDifferentPayload() {
		// Given
		inboxRepository.register(event, now);
		RiskDetectionRequestedEvent changed = new RiskDetectionRequestedEvent(
			event.eventId(), event.eventType(), event.schemaVersion(), event.correlationId(),
			event.causationId(), event.tenantAlias(), event.runId(), event.attemptId(),
			event.requestId(), event.idempotencyKey(),
			"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			event.occurredAt(), event.payload()
		);

		// When
		Registration conflict = inboxRepository.register(changed, now.plusSeconds(1));

		// Then
		assertThat(conflict).isEqualTo(Registration.CONFLICT);
		assertThat(rowCount("risk_detection_request_inbox")).isEqualTo(1);
	}

	@Test
	@DisplayName("Given 처리 중 종료된 요청 When lock timeout이 지나면 Then 새 HTTP 시도로 다시 claim한다")
	void reclaimsStaleProcessingRequest() {
		// Given
		inboxRepository.register(event, now);
		var firstClaim = inboxRepository.claimNext(now, Duration.ofSeconds(30));

		// When
		var beforeTimeout = inboxRepository.claimNext(
			now.plusSeconds(29), Duration.ofSeconds(30));
		var afterTimeout = inboxRepository.claimNext(
			now.plusSeconds(31), Duration.ofSeconds(30));

		// Then
		assertThat(firstClaim).get().extracting(claimed -> claimed.httpAttempt()).isEqualTo(1);
		assertThat(beforeTimeout).isEmpty();
		assertThat(afterTimeout).get().extracting(claimed -> claimed.httpAttempt()).isEqualTo(2);
	}

	@Test
	@DisplayName("Given 일시적 HTTP 실패 When 재시각 전후에 claim하면 Then 예정 시각 이후에만 재시도한다")
	void waitsUntilConfiguredRetryTime() {
		// Given
		inboxRepository.register(event, now);
		inboxRepository.claimNext(now, Duration.ofSeconds(30));
		inboxRepository.markRetry(
			event.eventId(), now.plusSeconds(2), "AI_HTTP_503", now);

		// When/Then
		assertThat(inboxRepository.claimNext(
			now.plusSeconds(1), Duration.ofSeconds(30))).isEmpty();
		assertThat(inboxRepository.claimNext(
			now.plusSeconds(2), Duration.ofSeconds(30))).isPresent();
	}

	@Test
	@DisplayName("Given pending Outbox When 발행 중 종료되면 Then lock timeout 뒤 같은 event_id를 다시 claim한다")
	void reclaimsStaleOutboxWithoutChangingEventIdentity() {
		// Given
		inboxRepository.register(event, now);
		var claimedRequest = inboxRepository.claimNext(now, Duration.ofSeconds(30)).orElseThrow();
		outboxRepository.insert(new RiskDetectionOutboxRepository.NewOutboxEvent(
			claimedRequest.event().eventId(), claimedRequest.event().eventId(),
			"checkon.risk-detection.completed.v1", event.tenantAlias(),
			readFixtureUnchecked(), now
		));

		// When
		var first = outboxRepository.claimNext(now, Duration.ofSeconds(30)).orElseThrow();
		var recovered = outboxRepository.claimNext(
			now.plusSeconds(31), Duration.ofSeconds(30)).orElseThrow();

		// Then
		assertThat(recovered.eventId()).isEqualTo(first.eventId());
		assertThat(recovered.publishAttempt()).isEqualTo(2);
	}

	@Test
	@DisplayName("Given 유효한 AI 응답 When 완료 처리하면 Then Inbox 상태와 completed Outbox를 원자적으로 저장한다")
	void storesCompletedOutcomeAtomically() {
		// Given
		inboxRepository.register(event, now);
		inboxRepository.claimNext(now, Duration.ofSeconds(30));
		AiDetectionResponse response = validResponse();

		// When
		outcomeCoordinator.complete(event, response);

		// Then
		assertThat(inboxStatus()).isEqualTo("OUTCOME_PENDING");
		String payload = jdbcTemplate.queryForObject("""
			SELECT event_payload::text FROM risk_detection_outbox
			WHERE source_event_id = ?
			""", String.class, event.eventId());
		assertThat(payload)
			.contains("\"event_type\": \"risk-detection.completed\"")
			.contains("\"causation_id\": \"" + event.eventId() + "\"")
			.contains("\"tenant_alias\": \"" + event.tenantAlias() + "\"");
	}

	@Test
	@DisplayName("Given 스냅샷 밖 학생 응답 When 완료 처리하면 Then Inbox와 Outbox를 변경하지 않는다")
	void rollsBackInvalidAiResponse() {
		// Given
		inboxRepository.register(event, now);
		inboxRepository.claimNext(now, Duration.ofSeconds(30));
		AiDetectionResponse invalid = responseForStudent(
			"st_ffffffffffffffffffffffffffffffff");

		// When/Then
		org.assertj.core.api.Assertions.assertThatThrownBy(
			() -> outcomeCoordinator.complete(event, invalid))
			.isInstanceOf(InvalidAiDetectionResponseException.class);
		assertThat(inboxStatus()).isEqualTo("PROCESSING");
		assertThat(rowCount("risk_detection_outbox")).isZero();
	}

	private AiDetectionResponse validResponse() {
		return new AiDetectionResponse(
			new AiDetectionResponse.Data(
				List.of(),
				new AiDetectionResponse.Stats(1, 0, 0, 0, List.of())
			),
			null,
			new AiDetectionResponse.Meta("ai-execution-1", Map.of("contract", "0.2"))
		);
	}

	private AiDetectionResponse responseForStudent(String studentRef) {
		AiDetectionResponse.Signal signal = new AiDetectionResponse.Signal(
			"signal-1", studentRef, "cl_0123456789abcdef0123456789abcdef",
			"R1", "acc_drop", "정답률 하락", 1.0, 1, false, "new",
			new AiDetectionResponse.Brief("확인이 필요합니다", true, false),
			List.of()
		);
		return new AiDetectionResponse(
			new AiDetectionResponse.Data(
				List.of(signal),
				new AiDetectionResponse.Stats(1, 1, 0, 0, List.of())
			),
			null,
			new AiDetectionResponse.Meta("ai-execution-1", Map.of("contract", "0.2"))
		);
	}

	private String inboxStatus() {
		return jdbcTemplate.queryForObject("""
			SELECT status FROM risk_detection_request_inbox WHERE event_id = ?
			""", String.class, event.eventId());
	}

	private int rowCount(String table) {
		return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
	}

	private String readFixtureUnchecked() {
		try {
			return readFixture();
		}
		catch (IOException exception) {
			throw new IllegalStateException(exception);
		}
	}

	private String readFixture() throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream(
			"contracts/risk-detection-requested-v0.2.json")) {
			assertThat(input).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
