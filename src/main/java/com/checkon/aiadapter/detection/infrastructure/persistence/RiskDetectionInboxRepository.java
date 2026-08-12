package com.checkon.aiadapter.detection.infrastructure.persistence;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Repository
@ConditionalOnProperty(
	prefix = "checkon.ai.risk-detection",
	name = "worker-enabled",
	havingValue = "true"
)
public class RiskDetectionInboxRepository {

	private final JdbcTemplate jdbcTemplate;
	private final ObjectMapper objectMapper;

	public RiskDetectionInboxRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
		this.jdbcTemplate = jdbcTemplate;
		this.objectMapper = objectMapper;
	}

	public Registration register(RiskDetectionRequestedEvent event, Instant now) {
		String payload = write(event);
		int inserted = jdbcTemplate.update("""
			INSERT INTO risk_detection_request_inbox (
			    event_id, tenant_alias, run_id, attempt_id, request_id,
			    idempotency_key, snapshot_hash, event_payload, status,
			    next_attempt_at, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), 'RECEIVED', ?, ?, ?)
			ON CONFLICT (event_id) DO NOTHING
			""",
			event.eventId(), event.tenantAlias(), event.runId(), event.attemptId(),
			event.requestId(), event.idempotencyKey(), event.snapshotHash(), payload,
			Timestamp.from(now), Timestamp.from(now), Timestamp.from(now)
		);
		if (inserted == 1) {
			return Registration.NEW;
		}

		String existing = jdbcTemplate.queryForObject("""
			SELECT event_payload::text
			FROM risk_detection_request_inbox
			WHERE event_id = ?
			""", String.class, event.eventId());
		return sameJson(existing, payload) ? Registration.DUPLICATE : Registration.CONFLICT;
	}

	public Optional<ClaimedRequest> claimNext(Instant now, Duration lockTimeout) {
		Instant staleBefore = now.minus(lockTimeout);
		List<ClaimedRequest> claimed = jdbcTemplate.query("""
			WITH candidate AS (
			    SELECT event_id
			    FROM risk_detection_request_inbox
			    WHERE (
			        status IN ('RECEIVED', 'RETRY_PENDING')
			        AND next_attempt_at <= ?
			    ) OR (
			        status = 'PROCESSING'
			        AND locked_at <= ?
			    )
			    ORDER BY next_attempt_at, created_at
			    FOR UPDATE SKIP LOCKED
			    LIMIT 1
			)
			UPDATE risk_detection_request_inbox inbox
			SET status = 'PROCESSING',
			    http_attempts = http_attempts + 1,
			    locked_at = ?,
			    updated_at = ?
			FROM candidate
			WHERE inbox.event_id = candidate.event_id
			RETURNING inbox.event_payload::text, inbox.http_attempts
			""",
			(resultSet, rowNumber) -> new ClaimedRequest(
				read(resultSet.getString(1)), resultSet.getInt(2)),
			Timestamp.from(now), Timestamp.from(staleBefore),
			Timestamp.from(now), Timestamp.from(now)
		);
		return claimed.stream().findFirst();
	}

	public void markRetry(UUID eventId, Instant nextAttemptAt, String errorCode, Instant now) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_request_inbox
			SET status = 'RETRY_PENDING', next_attempt_at = ?, locked_at = NULL,
			    last_error_code = ?, updated_at = ?
			WHERE event_id = ? AND status = 'PROCESSING'
			""", Timestamp.from(nextAttemptAt), errorCode, Timestamp.from(now), eventId), eventId);
	}

	public void markOutcomePending(UUID eventId, Instant now) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_request_inbox
			SET status = 'OUTCOME_PENDING', locked_at = NULL, updated_at = ?
			WHERE event_id = ? AND status = 'PROCESSING'
			""", Timestamp.from(now), eventId), eventId);
	}

	public void markOutcomePublished(UUID eventId, Instant now) {
		requireOutcomeUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_request_inbox
			SET status = 'OUTCOME_PUBLISHED', updated_at = ?
			WHERE event_id = ? AND status = 'OUTCOME_PENDING'
			""", Timestamp.from(now), eventId), eventId);
	}

	public void markOutcomeDead(UUID eventId, String errorCode, Instant now) {
		requireOutcomeUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_request_inbox
			SET status = 'OUTCOME_DEAD', last_error_code = ?, updated_at = ?
			WHERE event_id = ? AND status = 'OUTCOME_PENDING'
			""", errorCode, Timestamp.from(now), eventId), eventId);
	}

	private void requireUpdated(int updated, UUID eventId) {
		if (updated != 1) {
			throw new IllegalStateException("Inbox request is not PROCESSING: " + eventId);
		}
	}

	private void requireOutcomeUpdated(int updated, UUID eventId) {
		if (updated != 1) {
			throw new IllegalStateException("Inbox outcome is not pending: " + eventId);
		}
	}

	private String write(RiskDetectionRequestedEvent event) {
		try {
			return objectMapper.writeValueAsString(event);
		}
		catch (JacksonException exception) {
			throw new IllegalArgumentException("Risk detection event cannot be serialized", exception);
		}
	}

	private RiskDetectionRequestedEvent read(String payload) {
		try {
			return objectMapper.readValue(payload, RiskDetectionRequestedEvent.class);
		}
		catch (JacksonException exception) {
			throw new IllegalStateException("Stored risk detection event is invalid", exception);
		}
	}

	private boolean sameJson(String left, String right) {
		try {
			JsonNode leftNode = objectMapper.readTree(left);
			JsonNode rightNode = objectMapper.readTree(right);
			return leftNode.equals(rightNode);
		}
		catch (JacksonException exception) {
			throw new IllegalStateException("Stored risk detection event is invalid", exception);
		}
	}

	public enum Registration {
		NEW,
		DUPLICATE,
		CONFLICT
	}

	public record ClaimedRequest(RiskDetectionRequestedEvent event, int httpAttempt) {
	}
}
