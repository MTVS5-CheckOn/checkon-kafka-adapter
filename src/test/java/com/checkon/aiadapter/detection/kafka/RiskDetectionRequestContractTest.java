package com.checkon.aiadapter.detection.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import com.checkon.aiadapter.detection.ai.AiDetectionRequest;

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
	@DisplayName("Given 최신 요청 When 계약을 검증하면 Then enrolled_seconds를 포함해 통과한다")
	void acceptsLatestContract() {
		// Given: setUp에서 최신 요청 fixture를 역직렬화한다.

		// When
		RiskDetectionRequestValidator.validate(TENANT_ALIAS, event);

		// Then
		assertThat(event.payload().detectionEvidence()).hasSize(2);
		assertThat(event.payload().detectionEvidence())
			.filteredOn(item -> "weekly_activity".equals(item.kind()))
			.singleElement()
			.extracting(AiDetectionRequest.DetectionEvidence::enrolledSeconds)
			.isEqualTo(432_000L);
	}

	@Test
	@DisplayName("Given Adapter 선배포 중 legacy weekly activity When 검증하면 Then enrolled_seconds 누락을 허용한다")
	void acceptsLegacyWeeklyActivityWithoutEnrolledSeconds() throws Exception {
		// Given
		var root = (tools.jackson.databind.node.ObjectNode)objectMapper.readTree(readFixture());
		var evidence = (tools.jackson.databind.node.ArrayNode)root.at("/payload/detection_evidence");
		((tools.jackson.databind.node.ObjectNode)evidence.get(1)).remove("enrolled_seconds");
		RiskDetectionRequestedEvent legacyEvent = objectMapper.treeToValue(
			root, RiskDetectionRequestedEvent.class);

		// When
		RiskDetectionRequestValidator.validate(TENANT_ALIAS, legacyEvent);

		// Then
		assertThat(legacyEvent.payload().detectionEvidence().get(1).enrolledSeconds()).isNull();
	}

	@Test
	@DisplayName("Given weekly activity의 재원 시간이 범위를 벗어나면 When 검증하면 Then 요청을 거절한다")
	void rejectsOutOfRangeEnrolledSeconds() throws Exception {
		// Given
		RiskDetectionRequestedEvent invalidEvent = withEnrolledSeconds(0);

		// When / Then
		assertThatThrownBy(() -> RiskDetectionRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage(
				"payload.detection_evidence[].enrolled_seconds must be between 1 and 604800"
			);
	}

	@Test
	@DisplayName("Given weekly activity가 아닌 근거에 재원 시간이 있으면 When 검증하면 Then 요청을 거절한다")
	void rejectsEnrolledSecondsOnNonWeeklyEvidence() throws Exception {
		// Given
		var root = (tools.jackson.databind.node.ObjectNode)objectMapper.readTree(readFixture());
		var evidence = (tools.jackson.databind.node.ArrayNode)root.at("/payload/detection_evidence");
		((tools.jackson.databind.node.ObjectNode)evidence.get(0)).put("enrolled_seconds", 60);
		RiskDetectionRequestedEvent invalidEvent = objectMapper.treeToValue(
			root, RiskDetectionRequestedEvent.class);

		// When / Then
		assertThatThrownBy(() -> RiskDetectionRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage(
				"payload.detection_evidence[].enrolled_seconds is only valid for weekly_activity"
			);
	}

	@Test
	@DisplayName("Given 반 미배정 학생 When 계약을 검증하면 Then cl_unassigned를 허용한다")
	void givenUnassignedStudent_whenValidating_thenAcceptsClassSentinel() {
		// Given
		RiskDetectionRequestedEvent unassignedEvent = withStudentClassRef("cl_unassigned");

		// When
		RiskDetectionRequestValidator.validate(TENANT_ALIAS, unassignedEvent);

		// Then
		assertThat(unassignedEvent.payload().students())
			.singleElement()
			.extracting(AiDetectionRequest.StudentSnapshot::classRef)
			.isEqualTo("cl_unassigned");
	}

	@Test
	@DisplayName("Given 참조할 실제 반이 없는 snapshot When 계약을 검증하면 Then 빈 classes 배열을 허용한다")
	void givenNoReferencedClasses_whenValidating_thenAcceptsEmptyClasses() {
		// Given
		AiDetectionRequest source = event.payload();
		AiDetectionRequest.SnapshotMeta meta = source.snapshotMeta();
		AiDetectionRequest payload = new AiDetectionRequest(
			new AiDetectionRequest.SnapshotMeta(
				meta.weekStart(), meta.snapshotHash(), meta.termContext(), List.of()
			),
			source.students(), source.learningEvents(), source.alertContext(),
			source.detectionEvidence()
		);
		RiskDetectionRequestedEvent withoutClasses = withPayload(payload);

		// When
		RiskDetectionRequestValidator.validate(TENANT_ALIAS, withoutClasses);

		// Then
		assertThat(withoutClasses.payload().snapshotMeta().classes()).isEmpty();
	}

	@Test
	@DisplayName("Given 허용되지 않은 class_ref When 계약을 검증하면 Then 요청을 거절한다")
	void givenInvalidClassRef_whenValidating_thenRejectsRequest() {
		// Given
		RiskDetectionRequestedEvent invalidEvent = withStudentClassRef("cl_unknown");

		// When/Then
		assertThatThrownBy(() -> RiskDetectionRequestValidator.validate(TENANT_ALIAS, invalidEvent))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("payload.students[].class_ref has an invalid format");
	}

	@Test
	@DisplayName("Given v0.1 요청 When detection_evidence가 누락되면 Then 빈 배열로 호환한다")
	void acceptsLegacyRequestWithoutDetectionEvidence() throws Exception {
		// Given
		var legacyRoot = (tools.jackson.databind.node.ObjectNode)objectMapper.readTree(readFixture());
		var legacyPayload = (tools.jackson.databind.node.ObjectNode)legacyRoot.get("payload");
		legacyPayload.remove("detection_evidence");
		String legacyJson = objectMapper.writeValueAsString(legacyRoot);
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

	private RiskDetectionRequestedEvent withStudentClassRef(String classRef) {
		AiDetectionRequest source = event.payload();
		AiDetectionRequest.StudentSnapshot student = source.students().getFirst();
		AiDetectionRequest payload = new AiDetectionRequest(
			source.snapshotMeta(),
			List.of(new AiDetectionRequest.StudentSnapshot(
				student.studentRef(), classRef, student.enrolledWeeks(), student.status(),
				student.consent()
			)),
			source.learningEvents(), source.alertContext(), source.detectionEvidence()
		);
		return withPayload(payload);
	}

	private RiskDetectionRequestedEvent withPayload(AiDetectionRequest payload) {
		return new RiskDetectionRequestedEvent(
			event.eventId(), event.eventType(), event.schemaVersion(), event.correlationId(),
			event.causationId(), event.tenantAlias(), event.runId(), event.attemptId(),
			event.requestId(), event.idempotencyKey(), event.snapshotHash(), event.occurredAt(),
			payload
		);
	}

	private RiskDetectionRequestedEvent withEnrolledSeconds(long enrolledSeconds)
		throws Exception {
		var root = (tools.jackson.databind.node.ObjectNode)objectMapper.readTree(readFixture());
		var evidence = (tools.jackson.databind.node.ArrayNode)root.at("/payload/detection_evidence");
		((tools.jackson.databind.node.ObjectNode)evidence.get(1))
			.put("enrolled_seconds", enrolledSeconds);
		return objectMapper.treeToValue(root, RiskDetectionRequestedEvent.class);
	}

	private String readFixture() throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream(
			"contracts/risk-detection-requested-v0.2.json")) {
			assertThat(input).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
