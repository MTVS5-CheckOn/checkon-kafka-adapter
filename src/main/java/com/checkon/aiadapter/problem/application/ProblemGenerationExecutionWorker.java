package com.checkon.aiadapter.problem.application;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
		Instant now=Instant.now(clock);
		if(store.claimRevision(now,properties.lockTimeout()).map(this::executeRevision).orElse(false)) return true;
		return store.claimNext(now, properties.lockTimeout()).map(this::execute).orElse(false);
	}

	private boolean execute(ClaimedRequest request) {
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
			if(job.data()!=null&&job.data().jobId()!=null&&!jobId.equals(job.data().jobId()))
				throw new IllegalArgumentException("job_id changed during polling");
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
				saveFailure(request, "AI_JOB_" + status.name(),status.name().toLowerCase(java.util.Locale.ROOT));
			}
			else {
				Instant now = Instant.now(clock);
				java.time.Duration delay=jobResponse.retryAfter()==null?properties.pollInterval():
					(jobResponse.retryAfter().compareTo(properties.pollInterval())>0?jobResponse.retryAfter():properties.pollInterval());
				Instant next=now.plus(delay); var progress=outcomes.progress(ids.next(),request,status,now);
				ensureSize(progress,properties.maxReferenceEventBytes(),"REFERENCE_EVENT_TOO_LARGE");
				store.markWaitingWithProgress(request.eventId(),request.claimVersion(),next,
					status.name().toLowerCase(java.util.Locale.ROOT),progress,kafka.resultTopic(),request.tenantAlias(),now);
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
		List<UUID> eventIds=new ArrayList<>(); for(int i=0;i<items.data().items().size()+1;i++)eventIds.add(ids.next());
		var events=outcomes.terminal(eventIds.get(0),eventIds.subList(1,eventIds.size()),request,jobId,job,items,details,now);
		ensureSize(events.get(0),properties.maxReferenceEventBytes(),"REFERENCE_EVENT_TOO_LARGE");
		for(var event:events.subList(1,events.size())) ensureSize(event,properties.maxDetailEventBytes(),"DETAIL_EVENT_TOO_LARGE");
		String domain=job.data()!=null&&job.data().result()!=null&&job.data().result().status()!=null?
			job.data().result().status():fallbackDomain(items);
		store.saveTerminalEvents(request,events,kafka.resultTopic(),job.requiredSetId(),domain,now);
		DurabilityMetrics.transition("problem_generation","success");
	}

	private boolean executeRevision(com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedRevision request) {
		Headers headers=new Headers(request.tenantAlias(),request.requestId(),request.idempotencyKey()); Instant now=Instant.now(clock);
		try {
			var response=client.revise(request.setId(),request.slotIndex(),request.requestBody(),headers);
			var detail=client.item(request.setId(),request.slotIndex(),headers);
			var outcome=outcomes.revisionSucceeded(ids.next(),request,response.executionId(),detail,now);
			ensureSize(outcome,properties.maxDetailEventBytes(),"REVISION_EVENT_TOO_LARGE");
			store.saveRevisionOutcome(request,outcome,kafka.resultTopic(),now); DurabilityMetrics.transition("problem_generation_revision","success");
		}
		catch(AiProblemClientException exception){
			if(exception.isTransientFailure()&&request.httpAttempt()<properties.maxAttempts())
				store.markRevisionRetry(request,now.plus(properties.retryDelayAfter(request.httpAttempt())),exception.code(),now);
			else {var outcome=outcomes.revisionFailed(ids.next(),request,exception,now);
				ensureSize(outcome,properties.maxReferenceEventBytes(),"REVISION_EVENT_TOO_LARGE");
				store.saveRevisionOutcome(request,outcome,kafka.resultTopic(),now);}
		}
		catch(RuntimeException exception){var failure=new AiProblemClientException("AI_RESPONSE_INVALID",false,exception);
			store.saveRevisionOutcome(request,outcomes.revisionFailed(ids.next(),request,failure,now),kafka.resultTopic(),now);}
		return true;
	}

	private void ensureSize(com.checkon.aiadapter.problem.application.ProblemGenerationOutcomeFactory.OutgoingEvent event,
		int maximum,String code){if(event.payload().getBytes(StandardCharsets.UTF_8).length>maximum)throw new ProblemGenerationMappingException(code,"event payload exceeds byte limit");}
	private static String fallbackDomain(ProblemItemSetResponse items){int size=items.data().items().size();int dropped=items.data().statusCounts()==null?0:items.data().statusCounts().getOrDefault("dropped",0);return dropped==0?"generated":dropped>=size?"failed":"partial_success";}

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
