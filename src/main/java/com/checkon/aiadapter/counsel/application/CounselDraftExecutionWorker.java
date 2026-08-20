package com.checkon.aiadapter.counsel.application;

import java.time.Clock;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.counsel.ai.AiCounselDraftClient;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftClientException;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftRequestHeaders;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftResponse;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository.ClaimedRequest;
import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

@Component
@ConditionalOnProperty(
	prefix = "checkon.ai.counsel-draft",
	name = "worker-enabled",
	havingValue = "true"
)
public class CounselDraftExecutionWorker {
	private static final Logger log = LoggerFactory.getLogger(CounselDraftExecutionWorker.class);

	private final CounselDraftInboxRepository inboxRepository;
	private final AiCounselDraftClient aiClient;
	private final CounselDraftOutcomeCoordinator outcomeCoordinator;
	private final CounselDraftProcessingProperties properties;
	private final Clock clock;

	public CounselDraftExecutionWorker(
		CounselDraftInboxRepository inboxRepository,
		AiCounselDraftClient aiClient,
		CounselDraftOutcomeCoordinator outcomeCoordinator,
		CounselDraftProcessingProperties properties,
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
		CounselDraftRequestedEvent event = claimed.event();
		try {
			AiCounselDraftResponse response = aiClient.createDraftRaw(
				claimed.requestBody(),
				new AiCounselDraftRequestHeaders(
					event.tenantAlias(), event.requestId(), event.idempotencyKey()));
			outcomeCoordinator.complete(event, claimed.claimVersion(), response);
		}
		catch (AiCounselDraftClientException exception) {
			handleClientFailure(claimed, exception);
		}
		catch (InvalidAiCounselDraftResponseException exception) {
			outcomeCoordinator.fail(
				event, claimed.claimVersion(), "AI_RESPONSE_INVALID", "AI response contract is invalid", null);
		}
		return true;
	}

	private void handleClientFailure(
		ClaimedRequest claimed,
		AiCounselDraftClientException exception
	) {
		CounselDraftRequestedEvent event = claimed.event();
		String code = failureCode(exception);
		if (exception.isTransientFailure()
			&& claimed.httpAttempt() < properties.maxAttempts()) {
			Instant nextAttemptAt = Instant.now(clock)
				.plus(properties.retryDelayAfter(claimed.httpAttempt()));
			outcomeCoordinator.retry(event.eventId(), claimed.claimVersion(), nextAttemptAt, code);
			return;
		}
		log.warn("AI counsel draft request failed: errorCode={}, httpStatus={}", code, exception.httpStatus());
		outcomeCoordinator.fail(
			event, claimed.claimVersion(), code, failureMessage(exception), null);
	}

	private String failureCode(AiCounselDraftClientException exception) {
		return switch (exception.reason()) {
			case IDEMPOTENCY_CONFLICT -> "IDEMPOTENCY_CONFLICT";
			case EMPTY_RESPONSE -> "AI_EMPTY_RESPONSE";
			case NETWORK_ERROR -> "AI_NETWORK_ERROR";
			case HTTP_ERROR -> "AI_HTTP_" + exception.httpStatus();
		};
	}

	private String failureMessage(AiCounselDraftClientException exception) {
		return exception.isTransientFailure()
			? "AI service remained unavailable after adapter retries"
			: "AI service rejected or could not complete the request";
	}
}
