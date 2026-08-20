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
import org.springframework.transaction.annotation.Transactional;

import com.checkon.aiadapter.common.kafka.UuidV7Generator;
import com.checkon.aiadapter.common.observability.DurabilityMetrics;

@Repository
@ConditionalOnProperty(
	prefix = "checkon.ai.risk-detection",
	name = "worker-enabled",
	havingValue = "true"
)
public class RiskDetectionOutboxRepository {

	private final JdbcTemplate jdbcTemplate;
	private final UuidV7Generator idGenerator;

	public RiskDetectionOutboxRepository(JdbcTemplate jdbcTemplate, UuidV7Generator idGenerator) {
		this.jdbcTemplate = jdbcTemplate;
		this.idGenerator = idGenerator;
	}

	public void insert(NewOutboxEvent event) {
		int inserted = jdbcTemplate.update("""
			INSERT INTO risk_detection_outbox (
			    event_id, source_event_id, topic, message_key, event_payload,
			    status, available_at, created_at
			) VALUES (?, ?, ?, ?, CAST(? AS jsonb), 'PENDING', ?, ?)
			ON CONFLICT (source_event_id) DO NOTHING
			""",
			event.eventId(), event.sourceEventId(), event.topic(), event.messageKey(),
			event.eventPayload(), Timestamp.from(event.createdAt()),
			Timestamp.from(event.createdAt())
		);
		if (inserted != 1) {
			throw new IllegalStateException(
				"An outcome already exists for request event " + event.sourceEventId());
		}
	}

	@Transactional
	public Optional<ClaimedOutboxEvent> claimNext(Instant now, Duration lockTimeout) {
		Instant staleBefore = now.minus(lockTimeout);
		UUID attemptId = idGenerator.next();
		List<ClaimedOutboxEvent> claimed = jdbcTemplate.query("""
			WITH candidate AS (
			    SELECT event_id, status = 'PUBLISHING' AS stale_reclaim
			    FROM risk_detection_outbox
			    WHERE (
			        status = 'PENDING' AND available_at <= ?
			    ) OR (
			        status = 'PUBLISHING' AND locked_at <= ?
			    )
			    ORDER BY available_at, created_at
			    FOR UPDATE SKIP LOCKED
			    LIMIT 1
			), updated AS (
			UPDATE risk_detection_outbox outbox
			SET status = 'PUBLISHING',
			    publish_attempts = publish_attempts + 1,
			    claim_version = claim_version + 1,
			    locked_at = ?
			FROM candidate
			WHERE outbox.event_id = candidate.event_id
			RETURNING outbox.event_id, outbox.source_event_id, outbox.topic,
			          outbox.message_key, outbox.event_payload::text,
			          outbox.publish_attempts, outbox.claim_version, candidate.stale_reclaim
			), attempt AS (
			INSERT INTO outbox_publish_attempt
			(attempt_id, worker_kind, outbox_event_id, source_event_id, claim_version, started_at, stale_reclaim)
			SELECT ?, 'risk_detection', event_id, source_event_id, claim_version, ?, stale_reclaim FROM updated
			)
			SELECT event_id, source_event_id, topic, message_key, event_payload,
			       publish_attempts, claim_version, stale_reclaim FROM updated
			""",
			(resultSet, rowNumber) -> new ClaimedOutboxEvent(
				resultSet.getObject(1, UUID.class),
				resultSet.getObject(2, UUID.class),
				resultSet.getString(3),
				resultSet.getString(4),
				resultSet.getString(5),
				resultSet.getInt(6), resultSet.getLong(7), resultSet.getBoolean(8)
			),
			Timestamp.from(now), Timestamp.from(staleBefore), Timestamp.from(now),
			attemptId, Timestamp.from(now)
		);
		claimed.stream().filter(ClaimedOutboxEvent::staleReclaim).findFirst().ifPresent(event ->
			{ DurabilityMetrics.staleReclaim("risk_detection","outbox"); jdbcTemplate.update("""
				UPDATE outbox_publish_attempt SET finished_at=?, result_status='SUPERSEDED', superseded=TRUE
				WHERE worker_kind='risk_detection' AND outbox_event_id=? AND claim_version<? AND finished_at IS NULL
				""", Timestamp.from(now), event.eventId(), event.claimVersion()); });
		return claimed.stream().findFirst();
	}

	public void markPublished(UUID eventId, long claimVersion, Instant publishedAt) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_outbox
			SET status = 'PUBLISHED', locked_at = NULL, published_at = ?,
			    last_error_code = NULL
			WHERE event_id = ? AND status = 'PUBLISHING' AND claim_version = ?
			""", Timestamp.from(publishedAt), eventId, claimVersion), eventId);
		finishAttempt(eventId, claimVersion, "PUBLISHED", null, publishedAt);
	}

	public void markRetry(UUID eventId, long claimVersion, Instant availableAt, String errorCode) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_outbox
			SET status = 'PENDING', available_at = ?, locked_at = NULL,
			    last_error_code = ?
			WHERE event_id = ? AND status = 'PUBLISHING' AND claim_version = ?
			""", Timestamp.from(availableAt), errorCode, eventId, claimVersion), eventId);
		finishAttempt(eventId, claimVersion, "RETRY", errorCode, Instant.now());
	}

	public void markDead(UUID eventId, long claimVersion, String errorCode) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_outbox
			SET status = 'DEAD', locked_at = NULL, last_error_code = ?
			WHERE event_id = ? AND status = 'PUBLISHING' AND claim_version = ?
			""", errorCode, eventId, claimVersion), eventId);
		finishAttempt(eventId, claimVersion, "DEAD", errorCode, Instant.now());
	}

	private void finishAttempt(UUID eventId, long claimVersion, String status, String errorCode, Instant now) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE outbox_publish_attempt SET finished_at=?, result_status=?, error_code=?
			WHERE worker_kind='risk_detection' AND outbox_event_id=? AND claim_version=? AND finished_at IS NULL
			""", Timestamp.from(now), status, errorCode, eventId, claimVersion), eventId);
	}

	private void requireUpdated(int updated, UUID eventId) {
		if (updated != 1) {
			DurabilityMetrics.fencingRejected("risk_detection","outbox");
			throw new IllegalStateException("Outbox event is not PUBLISHING: " + eventId);
		}
	}

	public record NewOutboxEvent(
		UUID eventId,
		UUID sourceEventId,
		String topic,
		String messageKey,
		String eventPayload,
		Instant createdAt
	) {
	}

	public record ClaimedOutboxEvent(
		UUID eventId,
		UUID sourceEventId,
		String topic,
		String messageKey,
		String eventPayload,
		int publishAttempt,
		long claimVersion,
		boolean staleReclaim
	) {
	}
}
