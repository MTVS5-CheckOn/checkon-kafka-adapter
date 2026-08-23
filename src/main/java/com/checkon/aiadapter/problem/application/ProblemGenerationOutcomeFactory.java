package com.checkon.aiadapter.problem.application;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedRequest;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedRevision;
import com.checkon.aiadapter.problem.ai.AiProblemClientException;
import com.checkon.aiadapter.problem.ai.ProblemJobResponse;
import com.checkon.aiadapter.problem.ai.ProblemItemSetResponse;
import com.checkon.aiadapter.problem.ai.ProblemItemDetailResponse;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class ProblemGenerationOutcomeFactory {
	private final ObjectMapper objectMapper;

	public ProblemGenerationOutcomeFactory(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public OutgoingEvent progress(UUID eventId,ClaimedRequest request,ProblemJobResponse.JobStatus phase,Instant now) {
		Map<String,Object> payload=basePayload(request); payload.put("worker_phase",phase.name().toLowerCase(java.util.Locale.ROOT));
		payload.put("job_id",request.aiJobId()); payload.put("execution_id",request.aiExecutionId());
		return new OutgoingEvent(eventId,"progress:"+phase.name(),null,
			envelope(eventId,"worker_job.progress",request,payload,now,"pg-result-reference-1"),false);
	}

	public List<OutgoingEvent> terminal(UUID referenceEventId,List<UUID> slotEventIds,ClaimedRequest request,
		String jobId,ProblemJobResponse job,ProblemItemSetResponse summary,
		List<ProblemItemDetailResponse> details,Instant now) {
		if(summary.data()==null||summary.data().items()==null) throw new IllegalArgumentException("items must be an array");
		String setId=required(summary.data().setId(),"set_id");
		if(!setId.equals(job.requiredSetId())) throw new IllegalArgumentException("job and summary set_id mismatch");
		if(summary.data().jobId()!=null&&!jobId.equals(summary.data().jobId())) throw new IllegalArgumentException("job and summary job_id mismatch");
		Map<Integer,ProblemItemDetailResponse.Data> bySlot=detailMap(setId,details);
		validateSummaries(summary.data().items(),bySlot);
		if(slotEventIds.size()!=summary.data().items().size()) throw new IllegalArgumentException("slot event ID count mismatch");
		int requested=count(job.data()==null?null:job.data().result(),summary.data().items().size(),"requested");
		int processed=count(job.data()==null?null:job.data().result(),summary.data().items().size(),"processed");
		int unstarted=count(job.data()==null?null:job.data().result(),0,"unstarted");
		String domain=domainStatus(job.data()==null?null:job.data().result(),summary.data().statusCounts(),requested);
		Map<String,Object> payload=basePayload(request); payload.put("worker_phase","succeeded");
		payload.put("domain_status",domain); payload.put("job_id",jobId); payload.put("execution_id",request.aiExecutionId());
		payload.put("set_id",setId); payload.put("requested_count",requested); payload.put("processed_count",processed);
		payload.put("unstarted_count",unstarted); payload.put("status_counts",summary.data().statusCounts());
		ProblemJobResponse.Result result=job.data()==null?null:job.data().result();
		if(result!=null&&result.stopReason()!=null) payload.put("stop_reason",result.stopReason());
		if(result!=null&&result.publicFailureReason()!=null) payload.put("public_failure_reason",result.publicFailureReason());
		payload.put("result_ref","problem-set:"+setId);
		List<OutgoingEvent> events=new ArrayList<>(); events.add(new OutgoingEvent(referenceEventId,"terminal",null,
			envelope(referenceEventId,"worker_job.succeeded",request,payload,now,"pg-result-reference-1"),true));
		for(int index=0;index<summary.data().items().size();index++) {
			ProblemItemSetResponse.ItemSummary item=summary.data().items().get(index);
			Map<String,Object> slot=slot(item,bySlot.get(item.slotIndex())); Map<String,Object> slotPayload=basePayload(request);
			slotPayload.put("job_id",jobId); slotPayload.put("execution_id",request.aiExecutionId()); slotPayload.put("set_id",setId);
			slotPayload.put("slot",slot); UUID eventId=slotEventIds.get(index);
			events.add(new OutgoingEvent(eventId,"slot",item.slotIndex(),
				envelope(eventId,"problem_generation.slot.detail",request,slotPayload,now,"pg-slot-detail-1"),true));
		}
		return List.copyOf(events);
	}

	public OutgoingEvent revisionSucceeded(UUID eventId,ClaimedRevision request,String aiExecutionId,
		ProblemItemDetailResponse detail,Instant now) {
		if(detail==null||detail.data()==null||detail.data().slotIndex()==null) throw new IllegalArgumentException("revision detail is required");
		Map<String,Object> payload=revisionPayload(request); payload.put("execution_id",aiExecutionId);
		payload.put("slot",slot(null,detail.data()));
		return new OutgoingEvent(eventId,"revision:"+request.revisionRequestId(),request.slotIndex(),
			revisionEnvelope(eventId,"problem_generation.revision.succeeded",request,payload,now),false);
	}

	public OutgoingEvent revisionFailed(UUID eventId,ClaimedRevision request,AiProblemClientException failure,Instant now) {
		Map<String,Object> payload=revisionPayload(request); payload.put("error_code",failure.code());
		Map<String,Object> detail=new LinkedHashMap<>(); if(failure.detailReason()!=null) detail.put("reason",failure.detailReason());
		if(failure.currentRevisionNo()!=null) detail.put("current_revision_no",failure.currentRevisionNo());
		Map<String,Object> error=new LinkedHashMap<>(); error.put("code",failure.code()); error.put("message","AI problem revision could not complete");
		if(!detail.isEmpty()) error.put("detail",detail); payload.put("error",error);
		return new OutgoingEvent(eventId,"revision:"+request.revisionRequestId(),request.slotIndex(),
			revisionEnvelope(eventId,"problem_generation.revision.failed",request,payload,now),false);
	}

	public String succeeded(UUID eventId, ClaimedRequest request, String jobId,
		ProblemJobResponse jobResponse, ProblemItemSetResponse itemsResponse,
		List<ProblemItemDetailResponse> detailResponses, Instant now) {
		if(itemsResponse.data()==null) throw new IllegalArgumentException("data must be an object");
		String setId = required(itemsResponse.data().setId(),"set_id");
		if(!setId.equals(jobResponse.requiredSetId())) throw new IllegalArgumentException("job and summary set_id mismatch");
		if(itemsResponse.data().jobId()!=null&&!jobId.equals(itemsResponse.data().jobId())) throw new IllegalArgumentException("job_id mismatch");
		List<NormalizedProblemResult.Slot> items = mergeSlots(setId,itemsResponse.data().items(),detailResponses);
		if (items.isEmpty()) throw new IllegalArgumentException("AI items response contains no slots");
		NormalizedProblemResult result = new NormalizedProblemResult(setId,items,
			itemsResponse.data().statusCounts(),items.size(),items.size(),
			itemsResponse.meta()==null?null:itemsResponse.meta().versions());
		Map<String, Object> payload = basePayload(request);
		payload.put("job_id", jobId);
		payload.put("execution_id", request.aiExecutionId());
		payload.put("set_id", setId);
		payload.put("result_status", "completed");
		payload.put("result", result);
		payload.put("versions", result.versions());
		return envelope(eventId, "worker_job.succeeded", request, payload, now);
	}

	public String failed(UUID eventId, ClaimedRequest request, String code,String childStatus, Instant now) {
		Map<String, Object> payload = basePayload(request);
		payload.put("job_id", request.aiJobId());
		payload.put("execution_id", request.aiExecutionId());
		payload.put("child_status", childStatus);
		payload.put("worker_phase",childStatus);
		payload.put("domain_status","failed");
		payload.put("result_status", "failed");
		payload.put("error_code", code);
		payload.put("error", Map.of("code", code, "message", "AI problem generation could not complete"));
		return envelope(eventId, "cancelled".equals(childStatus)?"worker_job.cancelled":"worker_job.failed", request, payload, now);
	}

	private List<NormalizedProblemResult.Slot> mergeSlots(String setId,
		List<ProblemItemSetResponse.ItemSummary> summaries,List<ProblemItemDetailResponse> details) {
		if(summaries==null) throw new IllegalArgumentException("items must be an array");
		Map<Integer,ProblemItemDetailResponse.Data> bySlot=new LinkedHashMap<>();
		for(ProblemItemDetailResponse detail:details) {
			if(detail==null||detail.data()==null||detail.data().slotIndex()==null) throw new IllegalArgumentException("detail slot_index is required");
			if(detail.data().setId()!=null&&!setId.equals(detail.data().setId())) throw new IllegalArgumentException("detail set_id mismatch");
			if(bySlot.put(detail.data().slotIndex(),detail.data())!=null) throw new IllegalArgumentException("duplicate detail slot_index");
		}
		Map<Integer,Boolean> seen=new LinkedHashMap<>();
		List<NormalizedProblemResult.Slot> result=new ArrayList<>();
		for(ProblemItemSetResponse.ItemSummary summary:summaries) {
			if(summary==null||summary.slotIndex()==null||summary.slotIndex()<0) throw new IllegalArgumentException("slot_index must be a non-negative integer");
			if(seen.put(summary.slotIndex(),Boolean.TRUE)!=null) throw new IllegalArgumentException("duplicate slot_index");
			ProblemItemDetailResponse.Data detail=bySlot.get(summary.slotIndex());
			if(summary.itemId()!=null&&detail==null) throw new IllegalArgumentException("summary item has no matching detail slot");
			if(detail!=null&&detail.itemId()!=null&&!detail.itemId().equals(summary.itemId())) throw new IllegalArgumentException("summary/detail item_id mismatch");
			result.add(new NormalizedProblemResult.Slot(summary.slotIndex(),summary.itemId(),summary.status(),
				summary.failureReason(),summary.failureDetail(),detail==null?null:detail.item(),
				detail==null?null:detail.verification(),detail==null?null:detail.availableActions()));
		}
		if(!seen.keySet().containsAll(bySlot.keySet())) throw new IllegalArgumentException("detail contains unknown slot_index");
		return List.copyOf(result);
	}

	private static Map<String, Object> basePayload(ClaimedRequest request) {
		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("worker_kind", "problem_generation");
		payload.put("problem_request_id", request.problemRequestId().toString());
		payload.put("problem_execution_id", request.problemExecutionId().toString());
		payload.put("target_index", request.targetIndex());
		payload.put("adapter_execution_id", request.adapterExecutionId().toString());
		return payload;
	}

	private String envelope(UUID eventId, String type, ClaimedRequest request,
		Map<String, Object> payload, Instant now) {
		return envelope(eventId,type,request,payload,now,"worker-job-1");
	}

	private String envelope(UUID eventId,String type,ClaimedRequest request,
		Map<String,Object> payload,Instant now,String schemaVersion) {
		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("event_id", eventId.toString());
		envelope.put("event_type", type);
		envelope.put("occurred_at", now.toString());
		envelope.put("tenant_id", request.tenantAlias());
		envelope.put("schema_version", schemaVersion);
		envelope.put("correlation_id", request.problemRequestId().toString());
		envelope.put("causation_id", request.eventId().toString());
		envelope.put("payload", payload);
		try { return objectMapper.writeValueAsString(envelope); }
		catch (JacksonException exception) { throw new IllegalStateException("Outcome serialization failed", exception); }
	}

	private String revisionEnvelope(UUID eventId,String type,ClaimedRevision request,Map<String,Object> payload,Instant now) {
		Map<String,Object> envelope=new LinkedHashMap<>(); envelope.put("event_id",eventId.toString()); envelope.put("event_type",type);
		envelope.put("occurred_at",now.toString()); envelope.put("tenant_id",request.tenantAlias());
		envelope.put("schema_version","pg-revision-result-1"); envelope.put("correlation_id",request.problemRequestId().toString());
		envelope.put("causation_id",request.eventId().toString()); envelope.put("payload",payload);
		try{return objectMapper.writeValueAsString(envelope);}catch(JacksonException exception){throw new IllegalStateException("Outcome serialization failed",exception);}
	}

	private static Map<String,Object> revisionPayload(ClaimedRevision request) {
		Map<String,Object> payload=new LinkedHashMap<>(); payload.put("problem_request_id",request.problemRequestId().toString());
		payload.put("problem_execution_id",request.problemExecutionId().toString()); payload.put("revision_request_id",request.revisionRequestId().toString());
		payload.put("target_index",request.targetIndex()); payload.put("set_id",request.setId());
		payload.put("slot_index",request.slotIndex()); return payload;
	}

	private static Map<Integer,ProblemItemDetailResponse.Data> detailMap(String setId,List<ProblemItemDetailResponse> details) {
		Map<Integer,ProblemItemDetailResponse.Data> result=new LinkedHashMap<>();
		for(ProblemItemDetailResponse response:details){if(response==null||response.data()==null||response.data().slotIndex()==null)throw new IllegalArgumentException("detail slot_index is required");
			if(response.data().setId()!=null&&!setId.equals(response.data().setId()))throw new IllegalArgumentException("detail set_id mismatch");
			if(result.put(response.data().slotIndex(),response.data())!=null)throw new IllegalArgumentException("duplicate detail slot_index");}
		return result;
	}

	private static void validateSummaries(List<ProblemItemSetResponse.ItemSummary> summaries,Map<Integer,ProblemItemDetailResponse.Data> details) {
		java.util.Set<Integer> seen=new java.util.HashSet<>(); for(var summary:summaries){if(summary==null||summary.slotIndex()==null||summary.slotIndex()<0||!seen.add(summary.slotIndex()))throw new IllegalArgumentException("duplicate or invalid slot_index");
			var detail=details.get(summary.slotIndex()); if(summary.itemId()!=null&&detail==null)throw new IllegalArgumentException("summary item has no matching detail slot");
			if(detail!=null&&detail.itemId()!=null&&!detail.itemId().equals(summary.itemId()))throw new IllegalArgumentException("summary/detail item_id mismatch");
			if(detail!=null&&detail.status()!=null&&summary.status()!=null&&!detail.status().equals(summary.status()))throw new IllegalArgumentException("summary/detail status mismatch");}
		if(!seen.containsAll(details.keySet()))throw new IllegalArgumentException("detail contains unknown slot_index");
	}

	private static Map<String,Object> slot(ProblemItemSetResponse.ItemSummary summary,ProblemItemDetailResponse.Data detail) {
		Map<String,Object> slot=new LinkedHashMap<>(); int index=detail!=null?detail.slotIndex():summary.slotIndex(); slot.put("slot_index",index);
		slot.put("item_id",detail!=null?detail.itemId():summary.itemId()); slot.put("status",detail!=null?detail.status():summary.status());
		Integer revision=detail!=null?detail.currentRevisionNo():summary.currentRevisionNo(); slot.put("current_revision_no",revision==null?0:revision);
		slot.put("available_actions",detail==null||detail.availableActions()==null?List.of():detail.availableActions());
		if(detail!=null&&detail.revisions()!=null)slot.put("revisions",detail.revisions()); if(detail!=null&&detail.reviewReason()!=null)slot.put("review_reason",detail.reviewReason());
		String failure=detail!=null&&detail.failureReason()!=null?detail.failureReason():summary==null?null:summary.failureReason();
		JsonNode failureDetail=detail!=null&&detail.failureDetail()!=null?detail.failureDetail():summary==null?null:summary.failureDetail();
		if(failure!=null)slot.put("failure_reason",failure); if(failureDetail!=null)slot.put("failure_detail",failureDetail);
		slot.put("item",detail==null?null:detail.item()); if(detail!=null&&detail.verification()!=null)slot.put("verification",detail.verification()); return slot;
	}

	private static int count(ProblemJobResponse.Result result,int fallback,String kind){Integer value=result==null?null:switch(kind){case "requested"->result.requestedCount();case "processed"->result.processedCount();default->result.unstartedCount();};return value==null?fallback:value;}
	private static String domainStatus(ProblemJobResponse.Result result,Map<String,Integer> counts,int requested){if(result!=null&&result.status()!=null&&!result.status().isBlank())return result.status();int dropped=counts==null?0:counts.getOrDefault("dropped",0);return dropped==0?"generated":dropped>=requested?"failed":"partial_success";}

	public record OutgoingEvent(UUID eventId,String eventKind,Integer slotIndex,String payload,boolean terminalOutcome) { }

	private static String required(String value,String field) {
		if(value==null||value.isBlank()) throw new IllegalArgumentException(field+" must not be blank");
		return value;
	}
}
