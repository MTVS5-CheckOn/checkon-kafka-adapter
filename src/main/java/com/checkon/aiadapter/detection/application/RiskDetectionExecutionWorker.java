package com.checkon.aiadapter.detection.application;

import java.time.Clock;
import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.checkon.aiadapter.detection.ai.AiDetectionRequestHeaders;
import com.checkon.aiadapter.detection.ai.AiDetectionResponse;
import com.checkon.aiadapter.detection.ai.AiRiskDetectionClient;
import com.checkon.aiadapter.detection.ai.AiRiskDetectionClientException;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionInboxRepository;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionInboxRepository.ClaimedRequest;
import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

@Component
@ConditionalOnProperty(
	prefix = "checkon.ai.risk-detection",
	name = "worker-enabled",
	havingValue = "true"
)
public class RiskDetectionExecutionWorker {
	private static final Logger log = LoggerFactory.getLogger(
		RiskDetectionExecutionWorker.class);

	private final RiskDetectionInboxRepository inboxRepository;
	private final AiRiskDetectionClient aiClient;
	private final RiskDetectionOutcomeCoordinator outcomeCoordinator;
	private final RiskDetectionProcessingProperties properties;
	private final Clock clock;

	public RiskDetectionExecutionWorker(
		RiskDetectionInboxRepository inboxRepository,
		AiRiskDetectionClient aiClient,
		RiskDetectionOutcomeCoordinator outcomeCoordinator,
		RiskDetectionProcessingProperties properties,
		Clock clock
	) {
		this.inboxRepository = inboxRepository;
		this.aiClient = aiClient;
		this.outcomeCoordinator = outcomeCoordinator;
		this.properties = properties;
		this.clock = clock;
	}

	public boolean processOne() {
		Instant now = Instant.now(clock);
		return inboxRepository.claimNext(now, properties.lockTimeout())
			.map(this::execute)
			.orElse(false);
	}

	private boolean execute(ClaimedRequest claimed) {
		RiskDetectionRequestedEvent event = claimed.event();
		try {
			AiDetectionResponse response = aiClient.detectRaw(
				claimed.requestBody(),
				new AiDetectionRequestHeaders(
					event.tenantAlias(), event.requestId(), event.idempotencyKey())
			);
			outcomeCoordinator.complete(event, claimed.claimVersion(), response);
		}
		catch (AiRiskDetectionClientException exception) {
			handleClientFailure(claimed, exception);
		}
		catch (InvalidAiDetectionResponseException exception) {
			outcomeCoordinator.fail(
				event, claimed.claimVersion(), "AI_RESPONSE_INVALID", "AI response contract is invalid", null);
		}
		return true;
	}

	private void handleClientFailure(
		ClaimedRequest claimed,
		AiRiskDetectionClientException exception
	) {
		RiskDetectionRequestedEvent event = claimed.event();
		String code = failureCode(exception);
		if (exception.isTransientFailure()
			&& claimed.httpAttempt() < properties.maxAttempts()) {
			Instant nextAttemptAt = Instant.now(clock)
				.plus(properties.retryDelayAfter(claimed.httpAttempt()));
			outcomeCoordinator.retry(event.eventId(), claimed.claimVersion(), nextAttemptAt, code);
			return;
		}
		log.warn("AI detection request failed: errorCode={}, httpStatus={}", code, exception.httpStatus());
		outcomeCoordinator.fail(
			event, claimed.claimVersion(), code, failureMessage(exception), null);
	}

	private String failureCode(AiRiskDetectionClientException exception) {
		return switch (exception.reason()) {
			case IDEMPOTENCY_CONFLICT -> "IDEMPOTENCY_CONFLICT";
			case EMPTY_RESPONSE -> "AI_EMPTY_RESPONSE";
			case NETWORK_ERROR -> "AI_NETWORK_ERROR";
			case HTTP_ERROR -> "AI_HTTP_" + exception.httpStatus();
		};
	}

	private String failureMessage(AiRiskDetectionClientException exception) {
		return exception.isTransientFailure()
			? "AI service remained unavailable after adapter retries"
			: "AI service rejected or could not complete the request";
	}
}
