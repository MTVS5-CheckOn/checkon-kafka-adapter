package com.checkon.aiadapter.problem.application;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedRequest;
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
		payload.put("result_status", "failed");
		payload.put("error_code", code);
		payload.put("error", Map.of("code", code, "message", "AI problem generation could not complete"));
		return envelope(eventId, "worker_job.failed", request, payload, now);
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
		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("event_id", eventId.toString());
		envelope.put("event_type", type);
		envelope.put("occurred_at", now.toString());
		envelope.put("tenant_id", request.tenantAlias());
		envelope.put("schema_version", "worker-job-1");
		envelope.put("correlation_id", request.problemRequestId().toString());
		envelope.put("causation_id", request.eventId().toString());
		envelope.put("payload", payload);
		try { return objectMapper.writeValueAsString(envelope); }
		catch (JacksonException exception) { throw new IllegalStateException("Outcome serialization failed", exception); }
	}

	private static String required(String value,String field) {
		if(value==null||value.isBlank()) throw new IllegalArgumentException(field+" must not be blank");
		return value;
	}
}
