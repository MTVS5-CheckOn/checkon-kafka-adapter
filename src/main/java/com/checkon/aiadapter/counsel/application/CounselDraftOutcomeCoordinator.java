package com.checkon.aiadapter.counsel.application;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.checkon.aiadapter.common.kafka.CounselDraftKafkaProperties;
import com.checkon.aiadapter.common.kafka.UuidV7Generator;
import com.checkon.aiadapter.common.observability.DurabilityMetrics;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftResponse;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftOutboxRepository;
import com.checkon.aiadapter.counsel.kafka.AiCounselDraftFailure;
import com.checkon.aiadapter.counsel.kafka.CounselDraftOutcome;
import com.checkon.aiadapter.counsel.kafka.CounselDraftOutcomeEvent;
import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
@ConditionalOnProperty(
	prefix = "checkon.ai.counsel-draft",
	name = "worker-enabled",
	havingValue = "true"
)
public class CounselDraftOutcomeCoordinator {

	private final CounselDraftInboxRepository inboxRepository;
	private final CounselDraftOutboxRepository outboxRepository;
	private final CounselDraftKafkaProperties kafkaProperties;
	private final AiCounselDraftResponseValidator responseValidator;
	private final UuidV7Generator idGenerator;
	private final ObjectMapper objectMapper;
	private final Clock clock;

	public CounselDraftOutcomeCoordinator(
		CounselDraftInboxRepository inboxRepository,
		CounselDraftOutboxRepository outboxRepository,
		CounselDraftKafkaProperties kafkaProperties,
		AiCounselDraftResponseValidator responseValidator,
		UuidV7Generator idGenerator,
		ObjectMapper objectMapper,
		Clock clock
	) {
		this.inboxRepository = inboxRepository;
		this.outboxRepository = outboxRepository;
		this.kafkaProperties = kafkaProperties;
		this.responseValidator = responseValidator;
		this.idGenerator = idGenerator;
		this.objectMapper = objectMapper;
		this.clock = clock;
	}

	/**
	 * {@code canonicalExecutionId} comes from the original SUBMIT (POST)
	 * response, not necessarily {@code response.meta().executionId()} --
	 * once a job has gone through POLL, the terminal GET's own execution_id
	 * may differ, and POST's value is authoritative (mirrors
	 * ProblemGenerationOutcomeFactory using {@code request.aiExecutionId()},
	 * "POST 정본 실행 ID ... GET 값이 달라져도 POST 값을 결과에 보존한다").
	 */
	@Transactional
	public void complete(
		CounselDraftRequestedEvent request,
		long claimVersion,
		AiCounselDraftResponse response,
		String canonicalExecutionId
	) {
		responseValidator.validate(response);
		CounselDraftOutcome outcome = new CounselDraftOutcome(
			response.data().jobId(), response.data().status(), canonicalExecutionId);
		storeOutcome(request, claimVersion, CounselDraftOutcomeEvent.COMPLETED,
			kafkaProperties.completedTopic(), outcome);
	}

	@Transactional
	public void fail(
		CounselDraftRequestedEvent request,
		long claimVersion,
		String code,
		String message,
		Object detail
	) {
		storeOutcome(request, claimVersion, CounselDraftOutcomeEvent.FAILED,
			kafkaProperties.failedTopic(),
			new AiCounselDraftFailure(code, message, detail, false));
	}

	@Transactional
	public void retry(UUID eventId, long claimVersion, Instant nextAttemptAt, String errorCode) {
		inboxRepository.markRetry(eventId, claimVersion, nextAttemptAt, errorCode, Instant.now(clock));
		DurabilityMetrics.transition("counsel_draft", "retry");
	}

	private void storeOutcome(
		CounselDraftRequestedEvent request,
		long claimVersion,
		String eventType,
		String topic,
		Object payload
	) {
		Instant now = Instant.now(clock);
		UUID outcomeEventId = idGenerator.next();
		CounselDraftOutcomeEvent<Object> outcome = new CounselDraftOutcomeEvent<>(
			outcomeEventId, eventType, CounselDraftRequestedEvent.SCHEMA_VERSION,
			request.correlationId(), request.eventId(), request.tenantAlias(), request.runId(),
			request.attemptId(), request.requestId(), request.idempotencyKey(),
			request.snapshotHash(), now, payload
		);
		outboxRepository.insert(new CounselDraftOutboxRepository.NewOutboxEvent(
			outcomeEventId, request.eventId(), topic, request.tenantAlias(),
			write(outcome), now
		));
		inboxRepository.markOutcomePending(request.eventId(), claimVersion,
			eventType.equals(CounselDraftOutcomeEvent.COMPLETED) ? "SUCCEEDED" : "FAILED",
			eventType.equals(CounselDraftOutcomeEvent.COMPLETED) ? null : "AI_FAILED", now);
		DurabilityMetrics.transition("counsel_draft", eventType.equals(CounselDraftOutcomeEvent.COMPLETED) ? "success" : "failure");
	}

	private String write(CounselDraftOutcomeEvent<Object> event) {
		try {
			return objectMapper.writeValueAsString(event);
		}
		catch (JacksonException exception) {
			throw new IllegalStateException("Counsel draft outcome cannot be serialized", exception);
		}
	}
}
