package com.checkon.aiadapter.detection.application;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.checkon.aiadapter.common.kafka.RiskDetectionKafkaProperties;
import com.checkon.aiadapter.common.kafka.UuidV7Generator;
import com.checkon.aiadapter.detection.ai.AiDetectionResponse;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionInboxRepository;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionOutboxRepository;
import com.checkon.aiadapter.detection.kafka.AiDetectionFailure;
import com.checkon.aiadapter.detection.kafka.RiskDetectionOutcomeEvent;
import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import com.checkon.aiadapter.common.observability.DurabilityMetrics;

@Service
@ConditionalOnProperty(
	prefix = "checkon.ai.risk-detection",
	name = "worker-enabled",
	havingValue = "true"
)
public class RiskDetectionOutcomeCoordinator {

	private final RiskDetectionInboxRepository inboxRepository;
	private final RiskDetectionOutboxRepository outboxRepository;
	private final RiskDetectionKafkaProperties kafkaProperties;
	private final AiDetectionResponseValidator responseValidator;
	private final UuidV7Generator idGenerator;
	private final ObjectMapper objectMapper;
	private final Clock clock;

	public RiskDetectionOutcomeCoordinator(
		RiskDetectionInboxRepository inboxRepository,
		RiskDetectionOutboxRepository outboxRepository,
		RiskDetectionKafkaProperties kafkaProperties,
		AiDetectionResponseValidator responseValidator,
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

	@Transactional
	public void complete(RiskDetectionRequestedEvent request, long claimVersion, AiDetectionResponse response) {
		responseValidator.validate(request.payload(), response);
		storeOutcome(request, claimVersion, RiskDetectionOutcomeEvent.COMPLETED,
			kafkaProperties.completedTopic(), response);
	}

	@Transactional
	public void fail(
		RiskDetectionRequestedEvent request,
		long claimVersion,
		String code,
		String message,
		Object detail
	) {
		storeOutcome(request, claimVersion, RiskDetectionOutcomeEvent.FAILED,
			kafkaProperties.failedTopic(),
			new AiDetectionFailure(code, message, detail, false));
	}

	@Transactional
	public void retry(UUID eventId, long claimVersion, Instant nextAttemptAt, String errorCode) {
		inboxRepository.markRetry(eventId, claimVersion, nextAttemptAt, errorCode, Instant.now(clock));
		DurabilityMetrics.transition("risk_detection","retry");
	}

	private void storeOutcome(
		RiskDetectionRequestedEvent request,
		long claimVersion,
		String eventType,
		String topic,
		Object payload
	) {
		Instant now = Instant.now(clock);
		UUID outcomeEventId = idGenerator.next();
		RiskDetectionOutcomeEvent<Object> outcome = new RiskDetectionOutcomeEvent<>(
			outcomeEventId, eventType, RiskDetectionRequestedEvent.SCHEMA_VERSION,
			request.runId(), request.eventId(), request.tenantAlias(), request.runId(),
			request.attemptId(), request.requestId(), request.idempotencyKey(),
			request.snapshotHash(), now, payload
		);
		outboxRepository.insert(new RiskDetectionOutboxRepository.NewOutboxEvent(
			outcomeEventId, request.eventId(), topic, request.tenantAlias(),
			write(outcome), now
		));
		inboxRepository.markOutcomePending(request.eventId(), claimVersion,
			eventType.equals(RiskDetectionOutcomeEvent.COMPLETED) ? "SUCCEEDED" : "FAILED",
			eventType.equals(RiskDetectionOutcomeEvent.COMPLETED) ? null : "AI_FAILED", now);
		DurabilityMetrics.transition("risk_detection",eventType.equals(RiskDetectionOutcomeEvent.COMPLETED)?"success":"failure");
	}

	private String write(RiskDetectionOutcomeEvent<Object> event) {
		try {
			return objectMapper.writeValueAsString(event);
		}
		catch (JacksonException exception) {
			throw new IllegalStateException("Risk detection outcome cannot be serialized", exception);
		}
	}
}
