package com.checkon.aiadapter.counsel.infrastructure.persistence;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.checkon.aiadapter.common.kafka.UuidV7Generator;
import com.checkon.aiadapter.common.observability.DurabilityMetrics;
import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** Mirrors {@code RiskDetectionInboxRepository} -- see that class for the durability model this implements. */
@Repository
@ConditionalOnProperty(
	prefix = "checkon.ai.counsel-draft",
	name = "worker-enabled",
	havingValue = "true"
)
public class CounselDraftInboxRepository {

	private final JdbcTemplate jdbcTemplate;
	private final ObjectMapper objectMapper;
	private final UuidV7Generator idGenerator;

	public CounselDraftInboxRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper,
		UuidV7Generator idGenerator) {
		this.jdbcTemplate = jdbcTemplate;
		this.objectMapper = objectMapper;
		this.idGenerator = idGenerator;
	}

	public Registration register(CounselDraftRequestedEvent event, String rawEvent, Instant now) {
		String payload = requireJson(rawEvent);
		int inserted = jdbcTemplate.update("""
			INSERT INTO counsel_draft_request_inbox (
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
			FROM counsel_draft_request_inbox
			WHERE event_id = ?
			""", String.class, event.eventId());
		return sameJson(existing, payload) ? Registration.DUPLICATE : Registration.CONFLICT;
	}

	@Transactional
	public Optional<ClaimedRequest> claimNext(Instant now, Duration lockTimeout) {
		Instant staleBefore = now.minus(lockTimeout);
		UUID attemptId = idGenerator.next();
		List<ClaimedRequest> claimed = jdbcTemplate.query("""
			WITH candidate AS (
			    SELECT event_id, status = 'PROCESSING' AS stale_reclaim
			    FROM counsel_draft_request_inbox
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
			), updated AS (
			UPDATE counsel_draft_request_inbox inbox
			SET status = 'PROCESSING',
			    http_attempts = http_attempts + 1,
			    claim_version = claim_version + 1,
			    locked_at = ?,
			    updated_at = ?
			FROM candidate
			WHERE inbox.event_id = candidate.event_id
			RETURNING inbox.event_id, inbox.event_payload::text, inbox.http_attempts,
			          inbox.claim_version, candidate.stale_reclaim
			), attempt AS (
			INSERT INTO counsel_draft_attempt
			(attempt_id, source_event_id, phase, claim_version, started_at, stale_reclaim)
			SELECT ?, event_id, 'AI_HTTP', claim_version, ?, stale_reclaim FROM updated
			)
			SELECT event_payload, http_attempts, claim_version, stale_reclaim FROM updated
			""",
			(resultSet, rowNumber) -> claimedRequest(
				resultSet.getString(1), resultSet.getInt(2), resultSet.getLong(3),
				resultSet.getBoolean(4)),
			Timestamp.from(now), Timestamp.from(staleBefore),
			Timestamp.from(now), Timestamp.from(now), attemptId, Timestamp.from(now)
		);
		claimed.stream().filter(ClaimedRequest::staleReclaim).findFirst().ifPresent(request ->
			{ DurabilityMetrics.staleReclaim("counsel_draft", "inbox"); jdbcTemplate.update("""
				UPDATE counsel_draft_attempt SET finished_at=?, result_status='SUPERSEDED', superseded=TRUE
				WHERE source_event_id=? AND claim_version<? AND finished_at IS NULL
				""", Timestamp.from(now), request.event().eventId(), request.claimVersion()); });
		return claimed.stream().findFirst();
	}

	public void markRetry(UUID eventId, long claimVersion, Instant nextAttemptAt, String errorCode, Instant now) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE counsel_draft_request_inbox
			SET status = 'RETRY_PENDING', next_attempt_at = ?, locked_at = NULL,
			    last_error_code = ?, updated_at = ?
			WHERE event_id = ? AND status = 'PROCESSING' AND claim_version = ?
			""", Timestamp.from(nextAttemptAt), errorCode, Timestamp.from(now), eventId, claimVersion), eventId);
		finishAttempt(eventId, claimVersion, "RETRY", errorCode, now);
	}

	public void markOutcomePending(UUID eventId, long claimVersion, String resultStatus, String errorCode, Instant now) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE counsel_draft_request_inbox
			SET status = 'OUTCOME_PENDING', locked_at = NULL, updated_at = ?
			WHERE event_id = ? AND status = 'PROCESSING' AND claim_version = ?
			""", Timestamp.from(now), eventId, claimVersion), eventId);
		finishAttempt(eventId, claimVersion, resultStatus, errorCode, now);
	}

	private void finishAttempt(UUID eventId, long claimVersion, String resultStatus, String errorCode, Instant now) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE counsel_draft_attempt
			SET finished_at = ?, result_status = ?, error_code = ?
			WHERE source_event_id = ? AND claim_version = ? AND finished_at IS NULL
			""", Timestamp.from(now), resultStatus, errorCode, eventId, claimVersion), eventId);
	}

	public void markOutcomePublished(UUID eventId, Instant now) {
		requireOutcomeUpdated(jdbcTemplate.update("""
			UPDATE counsel_draft_request_inbox
			SET status = 'OUTCOME_PUBLISHED', updated_at = ?
			WHERE event_id = ? AND status = 'OUTCOME_PENDING'
			""", Timestamp.from(now), eventId), eventId);
	}

	public void markOutcomeDead(UUID eventId, String errorCode, Instant now) {
		requireOutcomeUpdated(jdbcTemplate.update("""
			UPDATE counsel_draft_request_inbox
			SET status = 'OUTCOME_DEAD', last_error_code = ?, updated_at = ?
			WHERE event_id = ? AND status = 'OUTCOME_PENDING'
			""", errorCode, Timestamp.from(now), eventId), eventId);
	}

	private void requireUpdated(int updated, UUID eventId) {
		if (updated != 1) {
			DurabilityMetrics.fencingRejected("counsel_draft", "inbox");
			throw new IllegalStateException("Inbox request is not PROCESSING: " + eventId);
		}
	}

	private void requireOutcomeUpdated(int updated, UUID eventId) {
		if (updated != 1) {
			throw new IllegalStateException("Inbox outcome is not pending: " + eventId);
		}
	}

	private ClaimedRequest claimedRequest(String eventPayload, int httpAttempt, long claimVersion, boolean staleReclaim) {
		try {
			JsonNode root = objectMapper.readTree(eventPayload);
			JsonNode aiPayload = root.get("payload");
			if (aiPayload == null || aiPayload.isNull()) {
				throw new IllegalStateException("Stored counsel draft payload is missing");
			}
			return new ClaimedRequest(
				objectMapper.treeToValue(root, CounselDraftRequestedEvent.class),
				objectMapper.writeValueAsString(aiPayload),
				httpAttempt, claimVersion, staleReclaim
			);
		}
		catch (JacksonException exception) {
			throw new IllegalStateException("Stored counsel draft event is invalid", exception);
		}
	}

	private String requireJson(String payload) {
		if (payload == null || payload.isBlank()) {
			throw new IllegalArgumentException("Counsel draft event cannot be empty");
		}
		try {
			objectMapper.readTree(payload);
			return payload;
		}
		catch (JacksonException exception) {
			throw new IllegalArgumentException("Counsel draft event cannot be serialized", exception);
		}
	}

	private boolean sameJson(String left, String right) {
		try {
			JsonNode leftNode = objectMapper.readTree(left);
			JsonNode rightNode = objectMapper.readTree(right);
			return leftNode.equals(rightNode);
		}
		catch (JacksonException exception) {
			throw new IllegalStateException("Stored counsel draft event is invalid", exception);
		}
	}

	public enum Registration {
		NEW,
		DUPLICATE,
		CONFLICT
	}

	public record ClaimedRequest(
		CounselDraftRequestedEvent event,
		String requestBody,
		int httpAttempt,
		long claimVersion,
		boolean staleReclaim
	) {
	}
}
