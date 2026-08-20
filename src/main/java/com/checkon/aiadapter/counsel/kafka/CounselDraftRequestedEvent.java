package com.checkon.aiadapter.counsel.kafka;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.checkon.aiadapter.counsel.ai.AiCounselDraftRequest;

/**
 * Same 13-field envelope shape as {@code RiskDetectionRequestedEvent}, but
 * counsel's id mapping differs: {@code run_id == attempt_id ==
 * correlation_id} (the backend mints one UUID per job and reuses it three
 * times, since counsel has no multi-attempt run concept), and
 * {@code request_id} is an independent fresh id, not equal to
 * {@code attempt_id} (unlike risk-detection). See
 * {@link CounselDraftRequestValidator} for the exact invariants.
 */
public record CounselDraftRequestedEvent(
	@JsonProperty("event_id") UUID eventId,
	@JsonProperty("event_type") String eventType,
	@JsonProperty("schema_version") String schemaVersion,
	@JsonProperty("correlation_id") UUID correlationId,
	@JsonProperty("causation_id") UUID causationId,
	@JsonProperty("tenant_alias") String tenantAlias,
	@JsonProperty("run_id") UUID runId,
	@JsonProperty("attempt_id") UUID attemptId,
	@JsonProperty("request_id") String requestId,
	@JsonProperty("idempotency_key") String idempotencyKey,
	@JsonProperty("snapshot_hash") String snapshotHash,
	@JsonProperty("occurred_at") Instant occurredAt,
	AiCounselDraftRequest payload
) {

	public static final String EVENT_TYPE = "counsel-draft.requested";
	public static final String SCHEMA_VERSION = "1.0";
}
