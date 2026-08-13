package com.checkon.aiadapter.problem.application;

import java.time.Clock;
import java.time.Instant;
import java.util.Locale;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.common.kafka.UuidV7Generator;
import com.checkon.aiadapter.problem.ai.AiProblemClient;
import com.checkon.aiadapter.problem.ai.AiProblemClient.Headers;
import com.checkon.aiadapter.problem.ai.AiProblemClientException;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedRequest;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationKafkaProperties;

import tools.jackson.databind.JsonNode;

@Component
@ConditionalOnProperty(prefix = "checkon.ai.problem-generation", name = "worker-enabled", havingValue = "true")
public class ProblemGenerationExecutionWorker {
	private final ProblemGenerationStore store;
	private final AiProblemClient client;
	private final ProblemGenerationOutcomeFactory outcomes;
	private final ProblemGenerationAiRequestMapper requestMapper;
	private final ProblemGenerationProperties properties;
	private final ProblemGenerationKafkaProperties kafka;
	private final UuidV7Generator ids;
	private final Clock clock;

	public ProblemGenerationExecutionWorker(ProblemGenerationStore store, AiProblemClient client,
		ProblemGenerationOutcomeFactory outcomes, ProblemGenerationAiRequestMapper requestMapper,
		ProblemGenerationProperties properties,
		ProblemGenerationKafkaProperties kafka, UuidV7Generator ids, Clock clock) {
		this.store = store; this.client = client; this.outcomes = outcomes; this.requestMapper = requestMapper;
		this.properties = properties;
		this.kafka = kafka; this.ids = ids; this.clock = clock;
	}

	@Scheduled(fixedDelayString = "${checkon.ai.problem-generation.poll-delay:1s}",
		initialDelayString = "${checkon.ai.problem-generation.poll-delay:1s}")
	public void poll() { processOne(); }

	public boolean processOne() {
		return store.claimNext(Instant.now(clock), properties.lockTimeout()).map(this::execute).orElse(false);
	}

	private boolean execute(ClaimedRequest request) {
		Headers headers = new Headers(request.tenantAlias(), request.requestId(), request.idempotencyKey());
		try {
			if ("SUBMIT".equals(request.phase())) {
				JsonNode submitted = client.submit(requestMapper.map(request.requestBody()), headers);
				String jobId = text(submitted.path("data"), "job_id");
				String executionId = executionId(submitted);
				Instant now = Instant.now(clock);
				store.markSubmitted(request.eventId(), jobId, executionId,
					now.plus(properties.pollInterval()), now);
				return true;
			}
			JsonNode job = client.job(request.aiJobId(), headers);
			String jobId = request.aiJobId();
			String status = text(job.path("data"), "status").toLowerCase(Locale.ROOT);
			if (status.equals("succeeded")) {
				JsonNode items = client.items(jobId, headers);
				saveSuccess(request, jobId, job, items);
			}
			else if (status.equals("failed") || status.equals("cancelled")) {
				saveFailure(request, "AI_JOB_" + status.toUpperCase(Locale.ROOT));
			}
			else {
				Instant now = Instant.now(clock);
				store.markWaiting(request.eventId(), now.plus(properties.pollInterval()), now);
			}
		}
		catch (AiProblemClientException exception) {
			handleFailure(request, exception.code(), exception.isTransientFailure());
		}
		catch (ProblemGenerationMappingException exception) {
			saveFailure(request, exception.code());
		}
		catch (RuntimeException exception) {
			handleFailure(request, "AI_RESPONSE_INVALID", false);
		}
		return true;
	}

	private void saveSuccess(ClaimedRequest request, String jobId, JsonNode job, JsonNode items) {
		Instant now = Instant.now(clock);
		var eventId = ids.next();
		store.saveOutcome(request.eventId(), eventId, kafka.resultTopic(), request.tenantAlias(),
			outcomes.succeeded(eventId, request, jobId, job, items, now), now);
	}

	private void saveFailure(ClaimedRequest request, String code) {
		Instant now = Instant.now(clock);
		var eventId = ids.next();
		store.saveOutcome(request.eventId(), eventId, kafka.resultTopic(), request.tenantAlias(),
			outcomes.failed(eventId, request, code, now), now);
	}

	private void handleFailure(ClaimedRequest request, String code, boolean transientFailure) {
		Instant now = Instant.now(clock);
		if (transientFailure && request.httpAttempt() < properties.maxAttempts()) {
			store.markRetry(request.eventId(), now.plus(properties.retryDelayAfter(request.httpAttempt())), code, now);
		}
		else saveFailure(request, code);
	}

	private static String text(JsonNode node, String field) {
		JsonNode value = node == null ? null : node.get(field);
		if (value == null || !value.isTextual() || value.asText().isBlank()) {
			throw new IllegalArgumentException(field + " must not be blank");
		}
		return value.asText();
	}

	private static String executionId(JsonNode submitted) {
		JsonNode data = submitted.path("data");
		JsonNode dataValue = data.get("execution_id");
		if (dataValue != null && dataValue.isTextual() && !dataValue.asText().isBlank()) {
			return dataValue.asText();
		}
		return text(submitted.path("meta"), "execution_id");
	}
}
