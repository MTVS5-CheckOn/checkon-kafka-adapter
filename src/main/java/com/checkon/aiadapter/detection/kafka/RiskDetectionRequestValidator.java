package com.checkon.aiadapter.detection.kafka;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.checkon.aiadapter.detection.ai.AiDetectionRequest;

public final class RiskDetectionRequestValidator {

	private static final Pattern TENANT_ALIAS = Pattern.compile("^tn_[0-9a-f]{32}$");
	private static final Pattern STUDENT_ALIAS = Pattern.compile("^st_[0-9a-f]{32}$");
	private static final Pattern CLASS_ALIAS = Pattern.compile(
		"^cl_(?:[0-9a-f]{32}|unassigned)$"
	);
	private static final Pattern SNAPSHOT_HASH = Pattern.compile("^sha256:[0-9a-f]{64}$");

	private RiskDetectionRequestValidator() {
	}

	public static void validate(String kafkaKey, RiskDetectionRequestedEvent event) {
		Objects.requireNonNull(event, "event must not be null");
		requireUuidV7(event.eventId(), "event_id");
		requireUuidV7(event.correlationId(), "correlation_id");
		requireUuidV7(event.causationId(), "causation_id");
		requireUuidV7(event.runId(), "run_id");
		requireUuidV7(event.attemptId(), "attempt_id");

		if (!RiskDetectionRequestedEvent.EVENT_TYPE.equals(event.eventType())) {
			throw new IllegalArgumentException("event_type must be risk-detection.requested");
		}
		if (!RiskDetectionRequestedEvent.SCHEMA_VERSION.equals(event.schemaVersion())) {
			throw new IllegalArgumentException("schema_version must be 1.0");
		}
		requirePattern(event.tenantAlias(), TENANT_ALIAS, "tenant_alias");
		if (!event.tenantAlias().equals(kafkaKey)) {
			throw new IllegalArgumentException("Kafka key must equal tenant_alias");
		}
		if (!event.runId().equals(event.correlationId())) {
			throw new IllegalArgumentException("correlation_id must equal run_id");
		}
		if (!event.eventId().equals(event.causationId())) {
			throw new IllegalArgumentException("requested causation_id must equal event_id");
		}
		if (!event.attemptId().toString().equals(event.requestId())) {
			throw new IllegalArgumentException("request_id must equal attempt_id");
		}
		validateIdempotencyKey(event.tenantAlias(), event.idempotencyKey());
		requirePattern(event.snapshotHash(), SNAPSHOT_HASH, "snapshot_hash");
		Objects.requireNonNull(event.occurredAt(), "occurred_at must not be null");
		validatePayload(event);
	}

	private static void validatePayload(RiskDetectionRequestedEvent event) {
		AiDetectionRequest payload = Objects.requireNonNull(event.payload(), "payload must not be null");
		AiDetectionRequest.SnapshotMeta meta = Objects.requireNonNull(
			payload.snapshotMeta(), "payload.snapshot_meta must not be null");
		if (!event.snapshotHash().equals(meta.snapshotHash())) {
			throw new IllegalArgumentException("payload snapshot_hash must equal envelope snapshot_hash");
		}
		Objects.requireNonNull(meta.weekStart(), "payload.snapshot_meta.week_start must not be null");
		requireText(meta.termContext(), "payload.snapshot_meta.term_context");
		validateAliases(meta.classes(), AiDetectionRequest.ClassReference::classRef,
			CLASS_ALIAS, "payload.snapshot_meta.classes[].class_ref");

		List<AiDetectionRequest.StudentSnapshot> students = requireList(payload.students(), "payload.students");
		for (AiDetectionRequest.StudentSnapshot student : students) {
			requirePattern(student.studentRef(), STUDENT_ALIAS, "payload.students[].student_ref");
			requirePattern(student.classRef(), CLASS_ALIAS, "payload.students[].class_ref");
		}
		validateAliases(payload.learningEvents(), AiDetectionRequest.LearningEventSnapshot::studentRef,
			STUDENT_ALIAS, "payload.learning_events[].student_ref");
		validateAliases(payload.alertContext(), AiDetectionRequest.AlertContext::studentRef,
			STUDENT_ALIAS, "payload.alert_context[].student_ref");
		List<AiDetectionRequest.DetectionEvidence> evidence = requireList(
			payload.detectionEvidence(), "payload.detection_evidence");
		validateAliases(evidence, AiDetectionRequest.DetectionEvidence::studentRef,
			STUDENT_ALIAS, "payload.detection_evidence[].student_ref");
		for (AiDetectionRequest.DetectionEvidence item : evidence) {
			validateEnrolledSeconds(item);
		}
	}

	private static void validateEnrolledSeconds(
		AiDetectionRequest.DetectionEvidence evidence
	) {
		Long enrolledSeconds = evidence.enrolledSeconds();
		if (!"weekly_activity".equals(evidence.kind())) {
			if (enrolledSeconds != null) {
				throw new IllegalArgumentException(
					"payload.detection_evidence[].enrolled_seconds is only valid for weekly_activity"
				);
			}
			return;
		}
		// Adapter-first rollout compatibility: legacy Backend requests can omit the new
		// field. New Backend requests require it and any supplied value is validated.
		if (enrolledSeconds == null) return;
		if (enrolledSeconds < 1 || enrolledSeconds > 604_800) {
			throw new IllegalArgumentException(
				"payload.detection_evidence[].enrolled_seconds must be between 1 and 604800"
			);
		}
	}

	private static void validateIdempotencyKey(String tenantAlias, String idempotencyKey) {
		requireText(idempotencyKey, "idempotency_key");
		String prefix = tenantAlias + ":";
		if (!idempotencyKey.startsWith(prefix)) {
			throw new IllegalArgumentException("idempotency_key must start with tenant_alias");
		}
		try {
			LocalDate.parse(idempotencyKey.substring(prefix.length()));
		}
		catch (DateTimeParseException exception) {
			throw new IllegalArgumentException("idempotency_key must end with an ISO date", exception);
		}
	}

	private static void requireUuidV7(UUID value, String field) {
		Objects.requireNonNull(value, field + " must not be null");
		if (value.version() != 7) {
			throw new IllegalArgumentException(field + " must be UUIDv7");
		}
	}

	private static void requirePattern(String value, Pattern pattern, String field) {
		requireText(value, field);
		if (!pattern.matcher(value).matches()) {
			throw new IllegalArgumentException(field + " has an invalid format");
		}
	}

	private static void requireText(String value, String field) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(field + " must not be blank");
		}
	}

	private static <T> List<T> requireList(List<T> values, String field) {
		return Objects.requireNonNull(values, field + " must not be null");
	}

	private static <T> void validateAliases(
		List<T> values,
		Function<T, String> alias,
		Pattern pattern,
		String field
	) {
		for (T value : requireList(values, field)) {
			T item = Objects.requireNonNull(value, field + " item must not be null");
			requirePattern(alias.apply(item), pattern, field);
		}
	}
}
