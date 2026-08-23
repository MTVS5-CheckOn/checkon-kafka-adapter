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
import com.checkon.aiadapter.problem.kafka.ProblemGenerationRevisionRequestedEvent;
import com.checkon.aiadapter.problem.application.ProblemGenerationOutcomeFactory.OutgoingEvent;
import com.checkon.aiadapter.common.kafka.UuidV7Generator;
import com.checkon.aiadapter.common.observability.DurabilityMetrics;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Repository
@ConditionalOnProperty(prefix = "checkon.ai.problem-generation", name = "worker-enabled", havingValue = "true")
public class ProblemGenerationStore {
	private final JdbcTemplate jdbc;
	private final ObjectMapper objectMapper;
	private final UuidV7Generator ids;

	public ProblemGenerationStore(JdbcTemplate jdbc, ObjectMapper objectMapper, UuidV7Generator ids) {
		this.jdbc = jdbc;
		this.objectMapper = objectMapper;
		this.ids = ids;
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

	public Registration registerRevision(ProblemGenerationRevisionRequestedEvent event,String rawPayload,Instant now) {
		UUID generationSource=jdbc.query("""
			SELECT event_id FROM problem_generation_request_inbox
			WHERE tenant_alias=? AND problem_request_id=? AND problem_execution_id=? AND ai_set_id=?
			""",(rs,row)->rs.getObject(1,UUID.class),event.tenantAlias(),event.problemRequestId(),
			event.problemExecutionId(),event.setId()).stream().findFirst()
			.orElseThrow(()->new IllegalArgumentException("revision target generation is not registered"));
		int inserted=jdbc.update("""
			INSERT INTO problem_generation_revision_inbox
			(event_id,revision_request_id,generation_source_event_id,tenant_alias,problem_request_id,
			 problem_execution_id,set_id,slot_index,request_id,idempotency_key,event_payload,request_payload,status,
			 next_attempt_at,created_at,updated_at)
			VALUES (?,?,?,?,?,?,?,?,?,?,CAST(? AS jsonb),CAST(? AS jsonb),'RECEIVED',?,?,?) ON CONFLICT(event_id) DO NOTHING
			""",event.eventId(),event.revisionRequestId(),generationSource,event.tenantAlias(),event.problemRequestId(),
			event.problemExecutionId(),event.setId(),event.slotIndex(),event.eventId().toString(),event.idempotencyKey(),rawPayload,
			json(event.request()),Timestamp.from(now),Timestamp.from(now),Timestamp.from(now));
		if(inserted==1)return Registration.NEW;
		String existing=jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_revision_inbox WHERE event_id=?",String.class,event.eventId());
		return sameJson(existing,rawPayload)?Registration.DUPLICATE:Registration.CONFLICT;
	}

	@Transactional
	public Optional<ClaimedRequest> claimNext(Instant now, Duration lockTimeout) {
		UUID attemptId = ids.next();
		List<ClaimedRequest> claimed = jdbc.query("""
			WITH candidate AS (
			 SELECT event_id,status='PROCESSING' AS stale_reclaim FROM problem_generation_request_inbox
			 WHERE ((status IN ('RECEIVED','WAITING','RETRY_PENDING') AND next_attempt_at<=?)
			    OR (status='PROCESSING' AND locked_at<=?))
			 ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1
			), updated AS (
			UPDATE problem_generation_request_inbox inbox
			SET status='PROCESSING',http_attempts=http_attempts+1,claim_version=claim_version+1,locked_at=?,updated_at=?
			FROM candidate WHERE inbox.event_id=candidate.event_id
			RETURNING inbox.event_id,inbox.adapter_execution_id,inbox.tenant_alias,
			 inbox.problem_request_id,inbox.problem_execution_id,inbox.target_index,
			 inbox.request_id,inbox.idempotency_key,inbox.event_payload::text,
			 inbox.phase,inbox.ai_job_id,inbox.ai_execution_id,inbox.http_attempts,inbox.created_at,
			 inbox.claim_version,candidate.stale_reclaim
			), attempt AS (
			 INSERT INTO problem_generation_attempt
			 (attempt_id,source_event_id,phase,claim_version,started_at,stale_reclaim)
			 SELECT ?,event_id,phase,claim_version,?,stale_reclaim FROM updated
			)
			SELECT * FROM updated
			""", (rs, row) -> claimed(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
			rs.getString(3), rs.getObject(4, UUID.class), rs.getObject(5, UUID.class), rs.getInt(6),
			rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10), rs.getString(11),
			rs.getString(12), rs.getInt(13),rs.getTimestamp(14).toInstant(),rs.getLong(15),rs.getBoolean(16)),
			Timestamp.from(now), Timestamp.from(now.minus(lockTimeout)), Timestamp.from(now), Timestamp.from(now),
			attemptId,Timestamp.from(now));
		claimed.stream().filter(ClaimedRequest::staleReclaim).findFirst().ifPresent(request -> jdbc.update("""
			UPDATE problem_generation_attempt SET finished_at=?,result_status='SUPERSEDED',superseded=TRUE
			WHERE source_event_id=? AND claim_version<? AND finished_at IS NULL
			""",Timestamp.from(now),request.eventId(),request.claimVersion()));
		claimed.stream().filter(ClaimedRequest::staleReclaim).findFirst().ifPresent(request -> DurabilityMetrics.staleReclaim("problem_generation","inbox"));
		return claimed.stream().findFirst();
	}

	public void markSubmitted(UUID eventId, long claimVersion, String jobId, String executionId,
		Instant nextAttemptAt, Instant now) {
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox
			SET status='WAITING',phase='POLL',worker_phase='queued',ai_job_id=?,ai_execution_id=?,next_attempt_at=?,
			    locked_at=NULL,last_error_code=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING' AND claim_version=?
			""", jobId, executionId, Timestamp.from(nextAttemptAt), Timestamp.from(now), eventId, claimVersion), eventId);
		finishAttempt(eventId, claimVersion, "SUBMITTED", null, now);
	}

	public void markWaiting(UUID eventId, long claimVersion, Instant nextAttemptAt, Instant now) {
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox
			SET status='WAITING',next_attempt_at=?,locked_at=NULL,last_error_code=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING' AND phase='POLL' AND claim_version=?
			""", Timestamp.from(nextAttemptAt), Timestamp.from(now), eventId, claimVersion), eventId);
		finishAttempt(eventId, claimVersion, "WAITING", null, now);
	}

	@Transactional
	public void markWaitingWithProgress(UUID eventId,long claimVersion,Instant nextAttemptAt,String workerPhase,
		OutgoingEvent progress,String topic,String key,Instant now) {
		insertOutbox(eventId,null,progress,topic,key,now);
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox SET status='WAITING',worker_phase=?,next_attempt_at=?,
			locked_at=NULL,last_error_code=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING' AND phase='POLL' AND claim_version=?
			""",workerPhase,Timestamp.from(nextAttemptAt),Timestamp.from(now),eventId,claimVersion),eventId);
		finishAttempt(eventId,claimVersion,"WAITING",null,now);
	}

	public void markRetry(UUID eventId, long claimVersion, Instant nextAttemptAt, String errorCode, Instant now) {
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox
			SET status='RETRY_PENDING',next_attempt_at=?,locked_at=NULL,last_error_code=?,updated_at=?
			WHERE event_id=? AND status='PROCESSING' AND claim_version=?
			""", Timestamp.from(nextAttemptAt), errorCode, Timestamp.from(now), eventId, claimVersion), eventId);
		finishAttempt(eventId, claimVersion, "RETRY", errorCode, now);
	}

	@Transactional
	public void saveOutcome(UUID sourceEventId, long claimVersion, UUID outcomeEventId, String topic, String key,
		String eventPayload, Instant now) {
		jdbc.update("""
			INSERT INTO problem_generation_outbox
			(event_id,source_event_id,event_kind,terminal_outcome,topic,message_key,event_payload,status,available_at,created_at)
			VALUES (?,?,'legacy-terminal',TRUE,?,?,CAST(? AS jsonb),'PENDING',?,?)
			ON CONFLICT (source_event_id,event_kind,slot_index) DO NOTHING
			""", outcomeEventId, sourceEventId, topic, key, eventPayload, Timestamp.from(now), Timestamp.from(now));
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox
			SET status='OUTCOME_PENDING',locked_at=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING' AND claim_version=?
			""", Timestamp.from(now), sourceEventId, claimVersion), sourceEventId);
		finishAttempt(sourceEventId, claimVersion, "OUTCOME_STORED", null, now);
	}

	@Transactional
	public Optional<ClaimedRevision> claimRevision(Instant now,Duration lockTimeout) {
		UUID attemptId=ids.next(); List<ClaimedRevision> claimed=jdbc.query("""
			WITH candidate AS (
			 SELECT event_id,status='PROCESSING' AS stale_reclaim FROM problem_generation_revision_inbox
			 WHERE ((status IN ('RECEIVED','RETRY_PENDING') AND next_attempt_at<=?)
			    OR (status='PROCESSING' AND locked_at<=?))
			 ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1
			), updated AS (
			 UPDATE problem_generation_revision_inbox inbox
			 SET status='PROCESSING',http_attempts=http_attempts+1,claim_version=claim_version+1,locked_at=?,updated_at=?
			 FROM candidate WHERE inbox.event_id=candidate.event_id
			 RETURNING inbox.event_id,inbox.revision_request_id,inbox.generation_source_event_id,inbox.tenant_alias,
			 inbox.problem_request_id,inbox.problem_execution_id,inbox.set_id,inbox.slot_index,inbox.request_id,
			 inbox.idempotency_key,inbox.request_payload::text,inbox.http_attempts,inbox.claim_version,candidate.stale_reclaim
			), attempt AS (
			 INSERT INTO problem_generation_revision_attempt
			 (attempt_id,source_event_id,claim_version,started_at,stale_reclaim)
			 SELECT ?,event_id,claim_version,?,stale_reclaim FROM updated)
			SELECT * FROM updated
			""",(rs,row)->new ClaimedRevision(rs.getObject(1,UUID.class),rs.getObject(2,UUID.class),rs.getObject(3,UUID.class),
			rs.getString(4),rs.getObject(5,UUID.class),rs.getObject(6,UUID.class),rs.getString(7),rs.getInt(8),
			rs.getString(9),rs.getString(10),rs.getString(11),rs.getInt(12),rs.getLong(13),rs.getBoolean(14)),
			Timestamp.from(now),Timestamp.from(now.minus(lockTimeout)),Timestamp.from(now),Timestamp.from(now),attemptId,Timestamp.from(now));
		claimed.stream().filter(ClaimedRevision::staleReclaim).findFirst().ifPresent(value->jdbc.update("""
			UPDATE problem_generation_revision_attempt SET finished_at=?,result_status='SUPERSEDED',superseded=TRUE
			WHERE source_event_id=? AND claim_version<? AND finished_at IS NULL
			""",Timestamp.from(now),value.eventId(),value.claimVersion()));
		return claimed.stream().findFirst();
	}

	public void markRevisionRetry(ClaimedRevision request,Instant nextAttemptAt,String code,Instant now) {
		requireOne(jdbc.update("""
			UPDATE problem_generation_revision_inbox SET status='RETRY_PENDING',next_attempt_at=?,locked_at=NULL,
			last_error_code=?,updated_at=? WHERE event_id=? AND status='PROCESSING' AND claim_version=?
			""",Timestamp.from(nextAttemptAt),code,Timestamp.from(now),request.eventId(),request.claimVersion()),request.eventId());
		finishRevisionAttempt(request.eventId(),request.claimVersion(),"RETRY",code,now);
	}

	@Transactional
	public void saveRevisionOutcome(ClaimedRevision request,OutgoingEvent outcome,String topic,Instant now) {
		insertOutbox(request.generationSourceEventId(),request.eventId(),outcome,topic,request.tenantAlias(),now);
		requireOne(jdbc.update("""
			UPDATE problem_generation_revision_inbox SET status='OUTCOME_PENDING',locked_at=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING' AND claim_version=?
			""",Timestamp.from(now),request.eventId(),request.claimVersion()),request.eventId());
		finishRevisionAttempt(request.eventId(),request.claimVersion(),"OUTCOME_STORED",null,now);
	}

	@Transactional
	public void saveTerminalEvents(ClaimedRequest request,List<OutgoingEvent> events,String topic,
		String setId,String domainStatus,Instant now) {
		for(OutgoingEvent event:events) insertOutbox(request.eventId(),null,event,topic,request.tenantAlias(),now);
		requireOne(jdbc.update("""
			UPDATE problem_generation_request_inbox SET status='OUTCOME_PENDING',worker_phase='succeeded',
			domain_status=?,ai_set_id=?,locked_at=NULL,updated_at=?
			WHERE event_id=? AND status='PROCESSING' AND claim_version=?
			""",domainStatus,setId,Timestamp.from(now),request.eventId(),request.claimVersion()),request.eventId());
		finishAttempt(request.eventId(),request.claimVersion(),"OUTCOME_STORED",null,now);
	}

	private void insertOutbox(UUID sourceEventId,UUID revisionSourceEventId,OutgoingEvent event,String topic,String key,Instant now) {
		if(jdbc.update("""
			INSERT INTO problem_generation_outbox
			(event_id,source_event_id,revision_source_event_id,event_kind,slot_index,sequence_no,terminal_outcome,
			 topic,message_key,event_payload,status,available_at,created_at)
			VALUES (?,?,?,?,?,?,?, ?,?,CAST(? AS jsonb),'PENDING',?,?)
			ON CONFLICT (source_event_id,event_kind,slot_index) DO NOTHING
			""",event.eventId(),sourceEventId,revisionSourceEventId,event.eventKind(),event.slotIndex(),
			event.slotIndex()==null?0:event.slotIndex()+1,event.terminalOutcome(),
			topic,key,event.payload(),Timestamp.from(now),Timestamp.from(now))==0&&revisionSourceEventId!=null)
			throw new IllegalStateException("revision outcome already exists");
	}

	@Transactional
	public Optional<ClaimedOutbox> claimOutbox(Instant now, Duration lockTimeout) {
		UUID attemptId = ids.next();
		List<ClaimedOutbox> claimed = jdbc.query("""
			WITH candidate AS (
			 SELECT event_id,status='PUBLISHING' AS stale_reclaim FROM problem_generation_outbox
			 WHERE (status='PENDING' AND available_at<=?) OR (status='PUBLISHING' AND locked_at<=?)
			 ORDER BY available_at,created_at,sequence_no FOR UPDATE SKIP LOCKED LIMIT 1
			), updated AS (
			UPDATE problem_generation_outbox outbox
			SET status='PUBLISHING',publish_attempts=publish_attempts+1,claim_version=claim_version+1,locked_at=?
			FROM candidate WHERE outbox.event_id=candidate.event_id
			RETURNING outbox.event_id,outbox.source_event_id,outbox.revision_source_event_id,
			 outbox.terminal_outcome,outbox.topic,outbox.message_key,
			 outbox.event_payload::text,outbox.publish_attempts,outbox.claim_version,candidate.stale_reclaim
			), attempt AS (
			 INSERT INTO outbox_publish_attempt
			 (attempt_id,worker_kind,outbox_event_id,source_event_id,claim_version,started_at,stale_reclaim)
			 SELECT ?,'problem_generation',event_id,source_event_id,claim_version,?,stale_reclaim FROM updated
			)
			SELECT * FROM updated
			""", (rs, row) -> new ClaimedOutbox(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),rs.getObject(3,UUID.class),
			rs.getBoolean(4),rs.getString(5), rs.getString(6), rs.getString(7), rs.getInt(8),rs.getLong(9),rs.getBoolean(10)), Timestamp.from(now),
			Timestamp.from(now.minus(lockTimeout)), Timestamp.from(now),attemptId,Timestamp.from(now));
		claimed.stream().filter(ClaimedOutbox::staleReclaim).findFirst().ifPresent(event -> jdbc.update("""
			UPDATE outbox_publish_attempt SET finished_at=?,result_status='SUPERSEDED',superseded=TRUE
			WHERE worker_kind='problem_generation' AND outbox_event_id=? AND claim_version<? AND finished_at IS NULL
			""",Timestamp.from(now),event.eventId(),event.claimVersion()));
		claimed.stream().filter(ClaimedOutbox::staleReclaim).findFirst().ifPresent(event -> DurabilityMetrics.staleReclaim("problem_generation","outbox"));
		return claimed.stream().findFirst();
	}

	@Transactional
	public void outboxPublished(UUID outboxId, UUID sourceId, long claimVersion, Instant now) {
		ClaimedOutboxState state=outboxState(outboxId);
		requireOne(jdbc.update("UPDATE problem_generation_outbox SET status='PUBLISHED',published_at=?,locked_at=NULL WHERE event_id=? AND status='PUBLISHING' AND claim_version=?",
			Timestamp.from(now), outboxId, claimVersion), outboxId);
		if(state.revisionSourceEventId()!=null) requireOne(jdbc.update("UPDATE problem_generation_revision_inbox SET status='OUTCOME_PUBLISHED',updated_at=? WHERE event_id=? AND status='OUTCOME_PENDING'",
			Timestamp.from(now),state.revisionSourceEventId()),state.revisionSourceEventId());
		else if(state.terminalOutcome()) jdbc.update("""
			UPDATE problem_generation_request_inbox SET status='OUTCOME_PUBLISHED',updated_at=?
			WHERE event_id=? AND status='OUTCOME_PENDING' AND NOT EXISTS(
			 SELECT 1 FROM problem_generation_outbox WHERE source_event_id=? AND terminal_outcome=TRUE AND status<>'PUBLISHED')
			""",Timestamp.from(now),sourceId,sourceId);
		finishOutboxAttempt(outboxId, claimVersion, "PUBLISHED", null, now);
	}

	public void outboxRetry(UUID outboxId, long claimVersion, Instant availableAt, String errorCode, Instant now) {
		requireOne(jdbc.update("UPDATE problem_generation_outbox SET status='PENDING',available_at=?,locked_at=NULL,last_error_code=? WHERE event_id=? AND status='PUBLISHING' AND claim_version=?",
			Timestamp.from(availableAt), errorCode, outboxId, claimVersion), outboxId);
		finishOutboxAttempt(outboxId, claimVersion, "RETRY", errorCode, now);
	}

	@Transactional
	public void outboxDead(UUID outboxId, UUID sourceId, long claimVersion, String errorCode, Instant now) {
		ClaimedOutboxState state=outboxState(outboxId);
		requireOne(jdbc.update("UPDATE problem_generation_outbox SET status='DEAD',locked_at=NULL,last_error_code=? WHERE event_id=? AND status='PUBLISHING' AND claim_version=?",
			errorCode, outboxId, claimVersion), outboxId);
		if(state.revisionSourceEventId()!=null) requireOne(jdbc.update("UPDATE problem_generation_revision_inbox SET status='OUTCOME_DEAD',last_error_code=?,updated_at=? WHERE event_id=? AND status='OUTCOME_PENDING'",
			errorCode,Timestamp.from(now),state.revisionSourceEventId()),state.revisionSourceEventId());
		else if(state.terminalOutcome()) jdbc.update("UPDATE problem_generation_request_inbox SET status='OUTCOME_DEAD',last_error_code=?,updated_at=? WHERE event_id=? AND status='OUTCOME_PENDING'",
			errorCode,Timestamp.from(now),sourceId);
		finishOutboxAttempt(outboxId, claimVersion, "DEAD", errorCode, now);
	}

	private void finishAttempt(UUID eventId, long claimVersion, String status, String errorCode, Instant now) {
		requireOne(jdbc.update("UPDATE problem_generation_attempt SET finished_at=?,result_status=?,error_code=? WHERE source_event_id=? AND claim_version=? AND finished_at IS NULL",
			Timestamp.from(now),status,errorCode,eventId,claimVersion),eventId);
	}

	private void finishOutboxAttempt(UUID eventId, long claimVersion, String status, String errorCode, Instant now) {
		requireOne(jdbc.update("UPDATE outbox_publish_attempt SET finished_at=?,result_status=?,error_code=? WHERE worker_kind='problem_generation' AND outbox_event_id=? AND claim_version=? AND finished_at IS NULL",
			Timestamp.from(now),status,errorCode,eventId,claimVersion),eventId);
	}

	private void finishRevisionAttempt(UUID eventId,long claimVersion,String status,String code,Instant now) {
		requireOne(jdbc.update("UPDATE problem_generation_revision_attempt SET finished_at=?,result_status=?,error_code=? WHERE source_event_id=? AND claim_version=? AND finished_at IS NULL",
			Timestamp.from(now),status,code,eventId,claimVersion),eventId);
	}

	private ClaimedOutboxState outboxState(UUID eventId) {
		return jdbc.queryForObject("SELECT revision_source_event_id,terminal_outcome FROM problem_generation_outbox WHERE event_id=?",
			(rs,row)->new ClaimedOutboxState(rs.getObject(1,UUID.class),rs.getBoolean(2)),eventId);
	}

	private ClaimedRequest claimed(UUID eventId, UUID adapterExecutionId, String tenantAlias,
		UUID requestId, UUID executionId, int targetIndex, String requestHeader, String idem,
		String rawPayload, String phase, String jobId, String aiExecutionId, int attempts,
		Instant createdAt, long claimVersion, boolean staleReclaim) {
		try {
			JsonNode request = objectMapper.readTree(rawPayload).get("payload").get("request");
			return new ClaimedRequest(eventId, adapterExecutionId, tenantAlias, requestId, executionId,
				targetIndex, requestHeader, idem, objectMapper.writeValueAsString(request), phase, jobId,
				aiExecutionId, attempts,createdAt,claimVersion,staleReclaim);
		}
		catch (JacksonException exception) {
			throw new IllegalStateException("Stored problem generation event is invalid", exception);
		}
	}

	private boolean sameJson(String left, String right) {
		try { return objectMapper.readTree(left).equals(objectMapper.readTree(right)); }
		catch (JacksonException exception) { throw new IllegalStateException("Stored event is invalid", exception); }
	}
	private String json(JsonNode value){try{return objectMapper.writeValueAsString(value);}catch(JacksonException exception){throw new IllegalStateException("JSON serialization failed",exception);}}

	private static void requireOne(int count, UUID id) {
		if (count != 1) { DurabilityMetrics.fencingRejected("problem_generation","durable_transition"); throw new IllegalStateException("Durable state transition failed: " + id); }
	}

	public enum Registration { NEW, DUPLICATE, CONFLICT }
	public record ClaimedRequest(UUID eventId, UUID adapterExecutionId, String tenantAlias,
		UUID problemRequestId, UUID problemExecutionId, int targetIndex, String requestId,
		String idempotencyKey, String requestBody, String phase, String aiJobId,
		String aiExecutionId, int httpAttempt,Instant createdAt,long claimVersion,boolean staleReclaim) { }
	public record ClaimedOutbox(UUID eventId, UUID sourceEventId,UUID revisionSourceEventId,boolean terminalOutcome,String topic,
		String messageKey, String eventPayload, int publishAttempt,long claimVersion,boolean staleReclaim) { }
	public record ClaimedRevision(UUID eventId,UUID revisionRequestId,UUID generationSourceEventId,String tenantAlias,
		UUID problemRequestId,UUID problemExecutionId,String setId,int slotIndex,String requestId,String idempotencyKey,
		String requestBody,int httpAttempt,long claimVersion,boolean staleReclaim) { }
	private record ClaimedOutboxState(UUID revisionSourceEventId,boolean terminalOutcome) { }
}
