package com.checkon.aiadapter.detection.kafka;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RiskDetectionOutcomeEvent<T>(
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
	T payload
) {

	public static final String COMPLETED = "risk-detection.completed";
	public static final String FAILED = "risk-detection.failed";
}
