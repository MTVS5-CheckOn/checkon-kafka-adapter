package com.checkon.aiadapter.problem.application;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.common.kafka.UuidV7Generator;
import com.checkon.aiadapter.problem.ai.AiProblemClient;
import com.checkon.aiadapter.problem.ai.AiProblemClient.Headers;
import com.checkon.aiadapter.problem.ai.AiProblemClientException;
import com.checkon.aiadapter.problem.ai.ProblemSubmissionResponse;
import com.checkon.aiadapter.problem.ai.ProblemJobResponse;
import com.checkon.aiadapter.problem.ai.ProblemItemSetResponse;
import com.checkon.aiadapter.problem.ai.ProblemItemDetailResponse;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedRequest;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationKafkaProperties;
import com.checkon.aiadapter.common.observability.DurabilityMetrics;

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

	public boolean processOne() {
		return store.claimNext(Instant.now(clock), properties.lockTimeout()).map(this::execute).orElse(false);
	}

	private boolean execute(ClaimedRequest request) {
		if(!Instant.now(clock).isBefore(request.createdAt().plus(properties.maxElapsed()))) {
			saveFailure(request,"ADAPTER_TIME_LIMIT_EXCEEDED","timed_out"); return true;
		}
		Headers headers = new Headers(request.tenantAlias(), request.requestId(), request.idempotencyKey());
		try {
			if ("SUBMIT".equals(request.phase())) {
				ProblemSubmissionResponse submitted = client.submit(requestMapper.map(request.requestBody()), headers);
				String jobId = submitted.requiredJobId();
				String executionId = submitted.canonicalExecutionId();
				Instant now = Instant.now(clock);
				store.markSubmitted(request.eventId(), request.claimVersion(), jobId, executionId,
					now.plus(properties.pollInterval()), now);
				return true;
			}
			var jobResponse = client.job(request.aiJobId(), headers);
			ProblemJobResponse job = jobResponse.body();
			String jobId = request.aiJobId();
			ProblemJobResponse.JobStatus status = job.requiredStatus();
			if (status == ProblemJobResponse.JobStatus.SUCCEEDED) {
				String setId=job.requiredSetId();
				ProblemItemSetResponse items = client.items(setId, headers);
				var details=new ArrayList<ProblemItemDetailResponse>();
				if(items.data()==null||items.data().items()==null) throw new IllegalArgumentException("items must be an array");
				for(ProblemItemSetResponse.ItemSummary summary:items.data().items()) {
					if(summary.slotIndex()==null) throw new IllegalArgumentException("slot_index is required");
					if(summary.itemId()!=null) details.add(client.item(setId,summary.slotIndex(),headers));
				}
				saveSuccess(request, jobId, job, items,details);
			}
			else if (status == ProblemJobResponse.JobStatus.FAILED || status == ProblemJobResponse.JobStatus.CANCELLED) {
				saveFailure(request, "AI_JOB_" + status.name());
			}
			else {
				Instant now = Instant.now(clock);
				java.time.Duration delay=jobResponse.retryAfter()==null?properties.pollInterval():
					(jobResponse.retryAfter().compareTo(properties.pollInterval())>0?jobResponse.retryAfter():properties.pollInterval());
				Instant cap=request.createdAt().plus(properties.maxElapsed());
				Instant next=now.plus(delay); store.markWaiting(request.eventId(),request.claimVersion(),next.isAfter(cap)?cap:next,now);
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

	private void saveSuccess(ClaimedRequest request, String jobId, ProblemJobResponse job,
		ProblemItemSetResponse items,List<ProblemItemDetailResponse> details) {
		Instant now = Instant.now(clock);
		var eventId = ids.next();
		String outcome=outcomes.succeeded(eventId, request, jobId, job, items,details, now);
		if(outcome.getBytes(StandardCharsets.UTF_8).length>properties.maxNormalizedResultBytes()) {
			saveFailure(request,"RESULT_TOO_LARGE"); return;
		}
		store.saveOutcome(request.eventId(), request.claimVersion(), eventId, kafka.resultTopic(), request.tenantAlias(),
			outcome, now);
		DurabilityMetrics.transition("problem_generation","success");
	}

	private void saveFailure(ClaimedRequest request, String code) {
		saveFailure(request,code,"failed");
	}
	private void saveFailure(ClaimedRequest request,String code,String childStatus) {
		Instant now = Instant.now(clock);
		var eventId = ids.next();
		store.saveOutcome(request.eventId(), request.claimVersion(), eventId, kafka.resultTopic(), request.tenantAlias(),
			outcomes.failed(eventId, request, code,childStatus, now), now);
		DurabilityMetrics.transition("problem_generation","failure");
	}

	private void handleFailure(ClaimedRequest request, String code, boolean transientFailure) {
		Instant now = Instant.now(clock);
		if (transientFailure && request.httpAttempt() < properties.maxAttempts()) {
			store.markRetry(request.eventId(), request.claimVersion(), now.plus(properties.retryDelayAfter(request.httpAttempt())), code, now);
			DurabilityMetrics.transition("problem_generation","retry");
		}
		else saveFailure(request, code);
	}

}
