package com.checkon.aiadapter.counsel.application;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.counsel.ai.AiCounselDraftClient;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftClient.PollResponse;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftClientException;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftRequestHeaders;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftResponse;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository.ClaimedRequest;
import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

/**
 * SUBMIT/POLL two-phase worker -- mirrors {@code ProblemGenerationExecutionWorker}.
 * The 2026-08-20 POST-계약변경 doc made the real AI's POST load-only ("적재만
 * 하고 즉시 반환"): a job that comes back non-terminal (queued/leased/running/
 * paused) must be polled via GET, honoring the {@code Retry-After} header the
 * AI emits on every non-terminal GET response rather than a fixed adapter
 * constant ("어댑터가 자기 상수로 돌면 ... 그 주기가 안 따라옵니다").
 */
@Component
@ConditionalOnProperty(
	prefix = "checkon.ai.counsel-draft",
	name = "worker-enabled",
	havingValue = "true"
)
public class CounselDraftExecutionWorker {
	private static final Logger log = LoggerFactory.getLogger(CounselDraftExecutionWorker.class);

	// succeeded/failed/cancelled -- the two other CounselJobPhase values
	// (leased/running/paused) are POST/GET-only mid-flight states.
	private static final Set<String> TERMINAL_STATUSES = Set.of("succeeded", "failed", "cancelled");

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
		if (!Instant.now(clock).isBefore(claimed.createdAt().plus(properties.maxElapsed()))) {
			outcomeCoordinator.fail(claimed.event(), claimed.claimVersion(),
				"ADAPTER_TIME_LIMIT_EXCEEDED", "Counsel draft job did not reach a terminal status in time", null);
			return true;
		}
		CounselDraftRequestedEvent event = claimed.event();
		AiCounselDraftRequestHeaders headers = new AiCounselDraftRequestHeaders(
			event.tenantAlias(), event.requestId(), event.idempotencyKey());
		try {
			if ("SUBMIT".equals(claimed.phase())) {
				submit(claimed, headers);
			}
			else {
				poll(claimed, headers);
			}
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

	private void submit(ClaimedRequest claimed, AiCounselDraftRequestHeaders headers) {
		AiCounselDraftResponse response = aiClient.createDraftRaw(claimed.requestBody(), headers);
		String status = requiredStatus(response);
		if (TERMINAL_STATUSES.contains(status)) {
			outcomeCoordinator.complete(claimed.event(), claimed.claimVersion(), response, executionId(response));
			return;
		}
		Instant now = Instant.now(clock);
		inboxRepository.markSubmitted(claimed.event().eventId(), claimed.claimVersion(),
			requiredJobId(response), executionId(response), now.plus(properties.pollInterval()), now);
	}

	private void poll(ClaimedRequest claimed, AiCounselDraftRequestHeaders headers) {
		PollResponse polled = aiClient.getDraft(claimed.aiJobId(), headers);
		AiCounselDraftResponse response = polled.body();
		String status = requiredStatus(response);
		if (TERMINAL_STATUSES.contains(status)) {
			// canonical execution_id is the one stored at SUBMIT time, not this GET's --
			// see CounselDraftOutcomeCoordinator.complete's javadoc.
			outcomeCoordinator.complete(claimed.event(), claimed.claimVersion(), response, claimed.aiExecutionId());
			return;
		}
		Instant now = Instant.now(clock);
		Duration delay = polled.retryAfter() != null ? polled.retryAfter() : properties.pollInterval();
		Instant next = now.plus(delay);
		Instant cap = claimed.createdAt().plus(properties.maxElapsed());
		inboxRepository.markWaiting(claimed.event().eventId(), claimed.claimVersion(), next.isAfter(cap) ? cap : next, now);
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

	private static String requiredStatus(AiCounselDraftResponse response) {
		if (response == null || response.data() == null || response.data().status() == null) {
			throw new InvalidAiCounselDraftResponseException("data.status is required");
		}
		return response.data().status();
	}

	private static String requiredJobId(AiCounselDraftResponse response) {
		String jobId = response.data() == null ? null : response.data().jobId();
		if (jobId == null || jobId.isBlank()) {
			throw new InvalidAiCounselDraftResponseException("data.job_id is required");
		}
		return jobId;
	}

	private static String executionId(AiCounselDraftResponse response) {
		return response.meta() == null ? null : response.meta().executionId();
	}
}
