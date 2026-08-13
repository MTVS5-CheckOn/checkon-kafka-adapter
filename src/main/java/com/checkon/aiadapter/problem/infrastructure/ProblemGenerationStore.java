package com.checkon.aiadapter.problem.infrastructure;

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

import com.checkon.aiadapter.problem.kafka.ProblemGenerationRequestedEvent;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Repository
@ConditionalOnProperty(prefix = "checkon.ai.problem-generation", name = "worker-enabled", havingValue = "true")
public class ProblemGenerationStore {
	private final JdbcTemplate jdbc;
	private final ObjectMapper objectMapper;

	public ProblemGenerationStore(JdbcTemplate jdbc, ObjectMapper objectMapper) {
		this.jdbc = jdbc;
		this.objectMapper = objectMapper;
	}

	public Registration register(ProblemGenerationRequestedEvent event, UUID adapterExecutionId,
		String rawPayload, Instant now) {
		int inserted = jdbc.update("""
			INSERT INTO problem_generation_request_inbox (
			 event_id,adapter_execution_id,tenant_alias,problem_request_id,problem_execution_id,
			 target_index,request_id,idempotency_key,event_payload,phase,status,next_attempt_at,created_at,updated_at
			) VALUES (?,?,?,?,?,?,?,?,CAST(? AS jsonb),'SUBMIT','RECEIVED',?,?,?)
			ON CONFLICT (event_id) DO NOTHING
			""", event.eventId(), adapterExecutionId, event.tenantAlias(), event.problemRequestId(),
			event.problemExecutionId(), event.targetIndex(), event.eventId().toString(),
			event.idempotencyKey(), rawPayload, Timestamp.from(now), Timestamp.from(now), Timestamp.from(now));
		if (inserted == 1) return Registration.NEW;
		String existing = jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_request_inbox WHERE event_id=?",
			String.class, event.eventId());
		return sameJson(existing, rawPayload) ? Registration.DUPLICATE : Registration.CONFLICT;
	}

	public Optional<ClaimedRequest> claimNext(Instant now, Duration lockTimeout) {
		List<ClaimedRequest> claimed = jdbc.query("""
			WITH candidate AS (
			 SELECT event_id FROM problem_generation_request_inbox
			 WHERE ((status IN ('RECEIVED','WAITING','RETRY_PENDING') AND next_attempt_at<=?)
			    OR (status='PROCESSING' AND locked_at<=?))
			 ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1
			)
			UPDATE problem_generation_request_inbox inbox
			SET status='PROCESSING',http_attempts=http_attempts+1,locked_at=?,updated_at=?
			FROM candidate WHERE inbox.event_id=candidate.event_id
			RETURNING inbox.event_id,inbox.adapter_execution_id,inbox.tenant_alias,
			 inbox.problem_request_id,inbox.problem_execution_id,inbox.target_index,
			 inbox.request_id,inbox.idempotency_key,inbox.event_payload::text,
			 inbox.phase,inbox.ai_job_id,inbox.ai_execution_id,inbox.http_attempts,inbox.created_at
			""", (rs, row) -> claimed(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
			rs.getString(3), rs.getObject(4, UUID.class), rs.getObject(5, UUID.class), rs.getInt(6),
			rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10), rs.getString(11),
			rs.getString(12), rs.getInt(13),rs.getTimestamp(14).toInstant()),
			Timestamp.from(now), Timestamp.from(now.minus(lockTimeout)), Timestamp.from(now), Timestamp.from(now));
		return claimed.stream().findFirst();
	}

	public void markSubmitted(UUID eventId, String jobId, String executionId,
		Instant nextAttemptAt, Instant now) {
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox
			SET status='WAITING',phase='POLL',ai_job_id=?,ai_execution_id=?,next_attempt_at=?,
			    locked_at=NULL,last_error_code=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING'
			""", jobId, executionId, Timestamp.from(nextAttemptAt), Timestamp.from(now), eventId), eventId);
	}

	public void markWaiting(UUID eventId, Instant nextAttemptAt, Instant now) {
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox
			SET status='WAITING',next_attempt_at=?,locked_at=NULL,last_error_code=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING' AND phase='POLL'
			""", Timestamp.from(nextAttemptAt), Timestamp.from(now), eventId), eventId);
	}

	public void markRetry(UUID eventId, Instant nextAttemptAt, String errorCode, Instant now) {
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox
			SET status='RETRY_PENDING',next_attempt_at=?,locked_at=NULL,last_error_code=?,updated_at=?
			WHERE event_id=? AND status='PROCESSING'
			""", Timestamp.from(nextAttemptAt), errorCode, Timestamp.from(now), eventId), eventId);
	}

	@Transactional
	public void saveOutcome(UUID sourceEventId, UUID outcomeEventId, String topic, String key,
		String eventPayload, Instant now) {
		jdbc.update("""
			INSERT INTO problem_generation_outbox
			(event_id,source_event_id,topic,message_key,event_payload,status,available_at,created_at)
			VALUES (?,?,?,?,CAST(? AS jsonb),'PENDING',?,?) ON CONFLICT (source_event_id) DO NOTHING
			""", outcomeEventId, sourceEventId, topic, key, eventPayload, Timestamp.from(now), Timestamp.from(now));
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox
			SET status='OUTCOME_PENDING',locked_at=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING'
			""", Timestamp.from(now), sourceEventId), sourceEventId);
	}

	public Optional<ClaimedOutbox> claimOutbox(Instant now, Duration lockTimeout) {
		List<ClaimedOutbox> claimed = jdbc.query("""
			WITH candidate AS (
			 SELECT event_id FROM problem_generation_outbox
			 WHERE (status='PENDING' AND available_at<=?) OR (status='PUBLISHING' AND locked_at<=?)
			 ORDER BY available_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1
			)
			UPDATE problem_generation_outbox outbox
			SET status='PUBLISHING',publish_attempts=publish_attempts+1,locked_at=?
			FROM candidate WHERE outbox.event_id=candidate.event_id
			RETURNING outbox.event_id,outbox.source_event_id,outbox.topic,outbox.message_key,
			 outbox.event_payload::text,outbox.publish_attempts
			""", (rs, row) -> new ClaimedOutbox(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
			rs.getString(3), rs.getString(4), rs.getString(5), rs.getInt(6)), Timestamp.from(now),
			Timestamp.from(now.minus(lockTimeout)), Timestamp.from(now));
		return claimed.stream().findFirst();
	}

	@Transactional
	public void outboxPublished(UUID outboxId, UUID sourceId, Instant now) {
		requireOne(jdbc.update("UPDATE problem_generation_outbox SET status='PUBLISHED',published_at=?,locked_at=NULL WHERE event_id=? AND status='PUBLISHING'",
			Timestamp.from(now), outboxId), outboxId);
		requireOne(jdbc.update("UPDATE problem_generation_request_inbox SET status='OUTCOME_PUBLISHED',updated_at=? WHERE event_id=? AND status='OUTCOME_PENDING'",
			Timestamp.from(now), sourceId), sourceId);
	}

	public void outboxRetry(UUID outboxId, Instant availableAt, String errorCode) {
		requireOne(jdbc.update("UPDATE problem_generation_outbox SET status='PENDING',available_at=?,locked_at=NULL,last_error_code=? WHERE event_id=? AND status='PUBLISHING'",
			Timestamp.from(availableAt), errorCode, outboxId), outboxId);
	}

	@Transactional
	public void outboxDead(UUID outboxId, UUID sourceId, String errorCode, Instant now) {
		requireOne(jdbc.update("UPDATE problem_generation_outbox SET status='DEAD',locked_at=NULL,last_error_code=? WHERE event_id=? AND status='PUBLISHING'",
			errorCode, outboxId), outboxId);
		requireOne(jdbc.update("UPDATE problem_generation_request_inbox SET status='OUTCOME_DEAD',last_error_code=?,updated_at=? WHERE event_id=? AND status='OUTCOME_PENDING'",
			errorCode, Timestamp.from(now), sourceId), sourceId);
	}

	private ClaimedRequest claimed(UUID eventId, UUID adapterExecutionId, String tenantAlias,
		UUID requestId, UUID executionId, int targetIndex, String requestHeader, String idem,
		String rawPayload, String phase, String jobId, String aiExecutionId, int attempts,Instant createdAt) {
		try {
			JsonNode request = objectMapper.readTree(rawPayload).get("payload").get("request");
			return new ClaimedRequest(eventId, adapterExecutionId, tenantAlias, requestId, executionId,
				targetIndex, requestHeader, idem, objectMapper.writeValueAsString(request), phase, jobId,
				aiExecutionId, attempts,createdAt);
		}
		catch (JacksonException exception) {
			throw new IllegalStateException("Stored problem generation event is invalid", exception);
		}
	}

	private boolean sameJson(String left, String right) {
		try { return objectMapper.readTree(left).equals(objectMapper.readTree(right)); }
		catch (JacksonException exception) { throw new IllegalStateException("Stored event is invalid", exception); }
	}

	private static void requireOne(int count, UUID id) {
		if (count != 1) throw new IllegalStateException("Durable state transition failed: " + id);
	}

	public enum Registration { NEW, DUPLICATE, CONFLICT }
	public record ClaimedRequest(UUID eventId, UUID adapterExecutionId, String tenantAlias,
		UUID problemRequestId, UUID problemExecutionId, int targetIndex, String requestId,
		String idempotencyKey, String requestBody, String phase, String aiJobId,
		String aiExecutionId, int httpAttempt,Instant createdAt) { }
	public record ClaimedOutbox(UUID eventId, UUID sourceEventId, String topic,
		String messageKey, String eventPayload, int publishAttempt) { }
}
