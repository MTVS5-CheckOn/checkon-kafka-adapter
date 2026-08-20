package com.checkon.aiadapter.counsel.kafka;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import com.checkon.aiadapter.counsel.ai.AiCounselDraftRequest;

/**
 * Counsel's id mapping and idempotency-key shape differ from risk-detection's
 * (see {@link CounselDraftRequestedEvent}), so this is its own validator
 * rather than a reuse of {@code RiskDetectionRequestValidator}.
 */
public final class CounselDraftRequestValidator {

	private static final Pattern TENANT_ALIAS = Pattern.compile("^tn_[0-9a-f]{32}$");
	private static final Pattern STUDENT_ALIAS = Pattern.compile("^st_[0-9a-f]{32}$");
	private static final Pattern PARENT_ALIAS = Pattern.compile("^pa_[0-9a-f]{32}$");
	private static final Pattern CLASS_ALIAS = Pattern.compile("^cl_[0-9a-f]{32}$");
	private static final Pattern SNAPSHOT_HASH = Pattern.compile("^sha256:[0-9a-f]{64}$");
	// Mirrors CounselDraftService.IDEMPOTENCY_KEY_PATTERN in CheckOn-backend --
	// the contract's own example key ("iq_884") is 6 characters.
	private static final Pattern IDEMPOTENCY_KEY = Pattern.compile("^[A-Za-z0-9._:-]{1,200}$");
	private static final Set<String> TOPICS = Set.of("grade", "schedule", "counsel_request", "etc");
	private static final Set<String> URGENCIES = Set.of("immediate", "normal");

	private CounselDraftRequestValidator() {
	}

	public static void validate(String kafkaKey, CounselDraftRequestedEvent event) {
		Objects.requireNonNull(event, "event must not be null");
		Objects.requireNonNull(event.eventId(), "event_id must not be null");
		Objects.requireNonNull(event.correlationId(), "correlation_id must not be null");
		Objects.requireNonNull(event.causationId(), "causation_id must not be null");
		Objects.requireNonNull(event.runId(), "run_id must not be null");
		Objects.requireNonNull(event.attemptId(), "attempt_id must not be null");

		if (!CounselDraftRequestedEvent.EVENT_TYPE.equals(event.eventType())) {
			throw new IllegalArgumentException("event_type must be counsel-draft.requested");
		}
		if (!CounselDraftRequestedEvent.SCHEMA_VERSION.equals(event.schemaVersion())) {
			throw new IllegalArgumentException("schema_version must be 1.0");
		}
		requirePattern(event.tenantAlias(), TENANT_ALIAS, "tenant_alias");
		if (!event.tenantAlias().equals(kafkaKey)) {
			throw new IllegalArgumentException("Kafka key must equal tenant_alias");
		}
		if (!event.runId().equals(event.correlationId())) {
			throw new IllegalArgumentException("run_id must equal correlation_id");
		}
		if (!event.attemptId().equals(event.correlationId())) {
			throw new IllegalArgumentException("attempt_id must equal correlation_id (one job = one attempt in v1)");
		}
		if (!event.eventId().equals(event.causationId())) {
			throw new IllegalArgumentException("requested causation_id must equal event_id");
		}
		requirePattern(event.idempotencyKey(), IDEMPOTENCY_KEY, "idempotency_key");
		requirePattern(event.snapshotHash(), SNAPSHOT_HASH, "snapshot_hash");
		requireText(event.requestId(), "request_id");
		Objects.requireNonNull(event.occurredAt(), "occurred_at must not be null");
		validatePayload(event);
	}

	private static void validatePayload(CounselDraftRequestedEvent event) {
		AiCounselDraftRequest payload = Objects.requireNonNull(event.payload(), "payload must not be null");
		AiCounselDraftRequest.Inquiry inquiry = Objects.requireNonNull(
			payload.inquiry(), "payload.inquiry must not be null");
		requireText(inquiry.inquiryRef(), "payload.inquiry.inquiry_ref");
		requireMember(inquiry.topic(), TOPICS, "payload.inquiry.topic");
		requireMember(inquiry.urgency(), URGENCIES, "payload.inquiry.urgency");
		Objects.requireNonNull(inquiry.receivedAt(), "payload.inquiry.received_at must not be null");
		requireText(inquiry.textMasked(), "payload.inquiry.text_masked");

		requirePattern(payload.studentRef(), STUDENT_ALIAS, "payload.student_ref");
		requirePattern(payload.parentRef(), PARENT_ALIAS, "payload.parent_ref");
		requirePattern(payload.classRef(), CLASS_ALIAS, "payload.class_ref");

		AiCounselDraftRequest.Context context = Objects.requireNonNull(
			payload.context(), "payload.context must not be null");
		requirePattern(context.snapshotHash(), SNAPSHOT_HASH, "payload.context.snapshot_hash");
		if (!event.snapshotHash().equals(context.snapshotHash())) {
			throw new IllegalArgumentException("payload.context.snapshot_hash must equal envelope snapshot_hash");
		}
		requireText(context.periodLabel(), "payload.context.period_label");
		List<AiCounselDraftRequest.Fact> facts = Objects.requireNonNull(
			context.facts(), "payload.context.facts must not be null (an empty list is allowed)");
		for (AiCounselDraftRequest.Fact fact : facts) {
			requireText(fact.summary(), "payload.context.facts[].summary");
		}
	}

	private static void requireMember(String value, Set<String> allowed, String field) {
		requireText(value, field);
		if (!allowed.contains(value)) {
			throw new IllegalArgumentException(field + " has an unknown value: " + value);
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
}
