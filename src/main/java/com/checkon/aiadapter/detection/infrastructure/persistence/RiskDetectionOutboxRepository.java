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

@Repository
@ConditionalOnProperty(
	prefix = "checkon.ai.risk-detection",
	name = "worker-enabled",
	havingValue = "true"
)
public class RiskDetectionOutboxRepository {

	private final JdbcTemplate jdbcTemplate;

	public RiskDetectionOutboxRepository(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
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

	public Optional<ClaimedOutboxEvent> claimNext(Instant now, Duration lockTimeout) {
		Instant staleBefore = now.minus(lockTimeout);
		List<ClaimedOutboxEvent> claimed = jdbcTemplate.query("""
			WITH candidate AS (
			    SELECT event_id
			    FROM risk_detection_outbox
			    WHERE (
			        status = 'PENDING' AND available_at <= ?
			    ) OR (
			        status = 'PUBLISHING' AND locked_at <= ?
			    )
			    ORDER BY available_at, created_at
			    FOR UPDATE SKIP LOCKED
			    LIMIT 1
			)
			UPDATE risk_detection_outbox outbox
			SET status = 'PUBLISHING',
			    publish_attempts = publish_attempts + 1,
			    locked_at = ?
			FROM candidate
			WHERE outbox.event_id = candidate.event_id
			RETURNING outbox.event_id, outbox.source_event_id, outbox.topic,
			          outbox.message_key, outbox.event_payload::text,
			          outbox.publish_attempts
			""",
			(resultSet, rowNumber) -> new ClaimedOutboxEvent(
				resultSet.getObject(1, UUID.class),
				resultSet.getObject(2, UUID.class),
				resultSet.getString(3),
				resultSet.getString(4),
				resultSet.getString(5),
				resultSet.getInt(6)
			),
			Timestamp.from(now), Timestamp.from(staleBefore), Timestamp.from(now)
		);
		return claimed.stream().findFirst();
	}

	public void markPublished(UUID eventId, Instant publishedAt) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_outbox
			SET status = 'PUBLISHED', locked_at = NULL, published_at = ?,
			    last_error_code = NULL
			WHERE event_id = ? AND status = 'PUBLISHING'
			""", Timestamp.from(publishedAt), eventId), eventId);
	}

	public void markRetry(UUID eventId, Instant availableAt, String errorCode) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_outbox
			SET status = 'PENDING', available_at = ?, locked_at = NULL,
			    last_error_code = ?
			WHERE event_id = ? AND status = 'PUBLISHING'
			""", Timestamp.from(availableAt), errorCode, eventId), eventId);
	}

	public void markDead(UUID eventId, String errorCode) {
		requireUpdated(jdbcTemplate.update("""
			UPDATE risk_detection_outbox
			SET status = 'DEAD', locked_at = NULL, last_error_code = ?
			WHERE event_id = ? AND status = 'PUBLISHING'
			""", errorCode, eventId), eventId);
	}

	private void requireUpdated(int updated, UUID eventId) {
		if (updated != 1) {
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
		int publishAttempt
	) {
	}
}
