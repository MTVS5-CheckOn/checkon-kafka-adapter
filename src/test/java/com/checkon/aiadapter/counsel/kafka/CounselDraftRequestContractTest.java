package com.checkon.aiadapter.counsel.kafka;

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

import com.checkon.aiadapter.counsel.ai.AiCounselDraftRequest;

class CounselDraftRequestContractTest {

	private static final String TENANT_ALIAS = "tn_0123456789abcdef0123456789abcdef";

	private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();

	private CounselDraftRequestedEvent event;

	@BeforeEach
	void setUp() throws Exception {
		event = objectMapper.readValue(readFixture(), CounselDraftRequestedEvent.class);
	}

	@Test
	@DisplayName("Given 최신 요청 When 계약을 검증하면 Then 통과한다")
	void acceptsLatestContract() {
		CounselDraftRequestValidator.validate(TENANT_ALIAS, event);

		assertThat(event.payload().context().facts()).hasSize(1);
	}

	@Test
	@DisplayName("Given tenant_alias와 다른 Kafka key When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsMismatchedKafkaKey() {
		assertThatThrownBy(() -> CounselDraftRequestValidator.validate(
			"tn_ffffffffffffffffffffffffffffffff", event))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("Kafka key must equal tenant_alias");
	}

	@Test
	@DisplayName("Given 지원하지 않는 schema_version When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsUnsupportedSchemaVersion() {
		CounselDraftRequestedEvent invalidEvent = copy(event, "2.0", event.causationId(),
			event.runId(), event.attemptId(), event.idempotencyKey());

		assertThatThrownBy(() -> CounselDraftRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("schema_version must be 1.0");
	}

	@Test
	@DisplayName("Given 요청 event_id와 다른 causation_id When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsDifferentRequestCausationId() {
		UUID differentCausationId = UUID.fromString("019b0000-0000-7000-8000-000000000099");
		CounselDraftRequestedEvent invalidEvent = copy(event, event.schemaVersion(),
			differentCausationId, event.runId(), event.attemptId(), event.idempotencyKey());

		assertThatThrownBy(() -> CounselDraftRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("requested causation_id must equal event_id");
	}

	@Test
	@DisplayName("Given attempt_id가 correlation_id와 다를 때 When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsAttemptIdNotEqualToCorrelationId() {
		UUID differentAttemptId = UUID.fromString("019b0000-0000-7000-8000-000000000098");
		CounselDraftRequestedEvent invalidEvent = copy(event, event.schemaVersion(),
			event.causationId(), event.runId(), differentAttemptId, event.idempotencyKey());

		assertThatThrownBy(() -> CounselDraftRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("attempt_id must equal correlation_id (one job = one attempt in v1)");
	}

	@Test
	@DisplayName("Given request_id가 attempt_id와 달라도 When 계약을 검증하면 Then 정상 통과한다(risk-detection과 다른 규약)")
	void acceptsRequestIdIndependentOfAttemptId() {
		// counsel mints request_id independently of attempt_id -- unlike risk-detection,
		// where request_id must equal attempt_id.
		assertThat(event.requestId()).isNotEqualTo(event.attemptId().toString());

		CounselDraftRequestValidator.validate(TENANT_ALIAS, event);
	}

	@Test
	@DisplayName("Given 자유 형식 idempotency_key(6자) When 계약을 검증하면 Then 정상 통과한다")
	void acceptsAShortFreeformIdempotencyKey() {
		// The contract's own example key ("iq_884") is 6 characters -- unlike
		// risk-detection's tenant-prefixed date-suffixed key.
		assertThat(event.idempotencyKey()).isEqualTo("iq_884");

		CounselDraftRequestValidator.validate(TENANT_ALIAS, event);
	}

	@Test
	@DisplayName("Given 알 수 없는 topic 값 When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsUnknownTopic() {
		AiCounselDraftRequest source = event.payload();
		AiCounselDraftRequest payload = new AiCounselDraftRequest(
			new AiCounselDraftRequest.Inquiry(
				source.inquiry().inquiryRef(), "complaint", source.inquiry().urgency(),
				source.inquiry().receivedAt(), source.inquiry().textMasked()
			),
			source.studentRef(), source.parentRef(), source.classRef(),
			source.labels(), source.dismissedSuggestions(), source.context()
		);
		CounselDraftRequestedEvent invalidEvent = withPayload(payload);

		assertThatThrownBy(() -> CounselDraftRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("payload.inquiry.topic has an unknown value: complaint");
	}

	@Test
	@DisplayName("Given payload.context.snapshot_hash가 envelope와 다를 때 When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsMismatchedSnapshotHash() {
		AiCounselDraftRequest source = event.payload();
		AiCounselDraftRequest payload = new AiCounselDraftRequest(
			source.inquiry(), source.studentRef(), source.parentRef(), source.classRef(),
			source.labels(), source.dismissedSuggestions(),
			new AiCounselDraftRequest.Context(
				"sha256:" + "f".repeat(64), source.context().periodLabel(), source.context().facts()
			)
		);
		CounselDraftRequestedEvent invalidEvent = withPayload(payload);

		assertThatThrownBy(() -> CounselDraftRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("payload.context.snapshot_hash must equal envelope snapshot_hash");
	}

	@Test
	@DisplayName("Given 형식이 어긋난 student_ref When 계약을 검증하면 Then 요청을 거절한다")
	void rejectsInvalidStudentRef() {
		AiCounselDraftRequest source = event.payload();
		AiCounselDraftRequest payload = new AiCounselDraftRequest(
			source.inquiry(), "student-not-opaque", source.parentRef(), source.classRef(),
			source.labels(), source.dismissedSuggestions(), source.context()
		);
		CounselDraftRequestedEvent invalidEvent = withPayload(payload);

		assertThatThrownBy(() -> CounselDraftRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("payload.student_ref has an invalid format");
	}

	private CounselDraftRequestedEvent copy(
		CounselDraftRequestedEvent source,
		String schemaVersion,
		UUID causationId,
		UUID runId,
		UUID attemptId,
		String idempotencyKey
	) {
		return new CounselDraftRequestedEvent(
			source.eventId(), source.eventType(), schemaVersion, source.correlationId(),
			causationId, source.tenantAlias(), runId, attemptId, source.requestId(),
			idempotencyKey, source.snapshotHash(), source.occurredAt(), source.payload()
		);
	}

	private CounselDraftRequestedEvent withPayload(AiCounselDraftRequest payload) {
		return new CounselDraftRequestedEvent(
			event.eventId(), event.eventType(), event.schemaVersion(), event.correlationId(),
			event.causationId(), event.tenantAlias(), event.runId(), event.attemptId(),
			event.requestId(), event.idempotencyKey(), event.snapshotHash(), event.occurredAt(),
			payload
		);
	}

	private String readFixture() throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream(
			"contracts/counsel-draft-requested-v1.json")) {
			assertThat(input).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
