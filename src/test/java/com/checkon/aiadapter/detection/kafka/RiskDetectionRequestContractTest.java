package com.checkon.aiadapter.detection.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class RiskDetectionRequestContractTest {

	private static final String TENANT_ALIAS = "tn_0123456789abcdef0123456789abcdef";

	private final ObjectMapper objectMapper = JsonMapper.builder()
		.findAndAddModules()
		.build();

	private RiskDetectionRequestedEvent event;

	@BeforeEach
	void setUp() throws Exception {
		event = objectMapper.readValue(readFixture(), RiskDetectionRequestedEvent.class);
	}

	@Test
	@DisplayName("Given 최신 v0.2 요청 When 계약을 검증하면 Then detection_evidence를 포함해 통과한다")
	void acceptsLatestContract() {
		// Given: setUp에서 최신 요청 fixture를 역직렬화한다.

		// When
		RiskDetectionRequestValidator.validate(TENANT_ALIAS, event);

		// Then
		assertThat(event.payload().detectionEvidence()).hasSize(1);
	}

	@Test
	@DisplayName("Given v0.1 요청 When detection_evidence가 누락되면 Then 빈 배열로 호환한다")
	void acceptsLegacyRequestWithoutDetectionEvidence() throws Exception {
		// Given
		String legacyJson = readFixture().replace(
			"    \"detection_evidence\": [\n"
				+ "      {\n"
				+ "        \"kind\": \"assignment_window\",\n"
				+ "        \"source_table\": \"assignment_week_summary\",\n"
				+ "        \"record_id\": \"assignment-week-1\",\n"
				+ "        \"student_ref\": \"st_abcdef0123456789abcdef0123456789\",\n"
				+ "        \"week_start\": \"2026-08-10\",\n"
				+ "        \"expected_count\": 2,\n"
				+ "        \"submitted_count\": 1\n"
				+ "      }\n"
				+ "    ]\n",
			""
		).replace("    \"alert_context\": [],\n  }", "    \"alert_context\": []\n  }");
		RiskDetectionRequestedEvent legacyEvent = objectMapper.readValue(
			legacyJson, RiskDetectionRequestedEvent.class);

		// When
		RiskDetectionRequestValidator.validate(TENANT_ALIAS, legacyEvent);

		// Then
		assertThat(legacyEvent.payload().detectionEvidence()).isEmpty();
	}

	@Test
	@DisplayName("Given tenant_alias와 다른 Kafka key When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsMismatchedKafkaKey() {
		// Given
		String differentKey = "tn_ffffffffffffffffffffffffffffffff";

		// When/Then
		assertThatThrownBy(() -> RiskDetectionRequestValidator.validate(differentKey, event))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("Kafka key must equal tenant_alias");
	}

	@Test
	@DisplayName("Given 지원하지 않는 schema_version When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsUnsupportedSchemaVersion() {
		// Given
		RiskDetectionRequestedEvent invalidEvent = copy(event, "2.0", event.tenantAlias(),
			event.causationId());

		// When/Then
		assertThatThrownBy(() -> RiskDetectionRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("schema_version must be 1.0");
	}

	@Test
	@DisplayName("Given 원본 UUID 형태 tenant 값 When 계약을 검증하면 Then 개인정보 경계를 보호한다")
	void rejectsNonOpaqueTenantAlias() {
		// Given
		RiskDetectionRequestedEvent invalidEvent = copy(event, event.schemaVersion(),
			"019b0000-0000-7000-8000-000000000099", event.causationId());

		// When/Then
		assertThatThrownBy(() -> RiskDetectionRequestValidator.validate(
			invalidEvent.tenantAlias(), invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("tenant_alias has an invalid format");
	}

	@Test
	@DisplayName("Given 요청 event_id와 다른 causation_id When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsDifferentRequestCausationId() {
		// Given
		UUID differentCausationId = UUID.fromString("019b0000-0000-7000-8000-000000000099");
		RiskDetectionRequestedEvent invalidEvent = copy(event, event.schemaVersion(),
			event.tenantAlias(), differentCausationId);

		// When/Then
		assertThatThrownBy(() -> RiskDetectionRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("requested causation_id must equal event_id");
	}

	private RiskDetectionRequestedEvent copy(
		RiskDetectionRequestedEvent source,
		String schemaVersion,
		String tenantAlias,
		UUID causationId
	) {
		return new RiskDetectionRequestedEvent(
			source.eventId(), source.eventType(), schemaVersion, source.correlationId(),
			causationId, tenantAlias, source.runId(), source.attemptId(), source.requestId(),
			source.idempotencyKey(), source.snapshotHash(), source.occurredAt(), source.payload()
		);
	}

	private String readFixture() throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream(
			"contracts/risk-detection-requested-v0.2.json")) {
			assertThat(input).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
