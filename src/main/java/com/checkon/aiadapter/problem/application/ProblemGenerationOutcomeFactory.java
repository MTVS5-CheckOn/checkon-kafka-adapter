package com.checkon.aiadapter.problem.application;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedRequest;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Component
public class ProblemGenerationOutcomeFactory {
	private final ObjectMapper objectMapper;

	public ProblemGenerationOutcomeFactory(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public String succeeded(UUID eventId, ClaimedRequest request, String jobId,
		JsonNode jobResponse, JsonNode itemsResponse, Instant now) {
		JsonNode data = requiredObject(itemsResponse, "data");
		String setId = requiredText(data, "set_id");
		List<JsonNode> items = flattenItems(data.get("items"));
		if (items.isEmpty()) throw new IllegalArgumentException("AI items response contains no projectable item");
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("set_id", setId);
		result.put("items", items);
		JsonNode meta = object(itemsResponse, "meta");
		if (meta == null) meta = object(jobResponse, "meta");
		Map<String, Object> payload = basePayload(request);
		payload.put("job_id", jobId);
		payload.put("execution_id", request.aiExecutionId());
		payload.put("set_id", setId);
		payload.put("result_status", "completed");
		payload.put("result", result);
		payload.put("versions", meta == null ? null : meta.get("versions"));
		return envelope(eventId, "worker_job.succeeded", request, payload, now);
	}

	public String failed(UUID eventId, ClaimedRequest request, String code, Instant now) {
		Map<String, Object> payload = basePayload(request);
		payload.put("job_id", request.aiJobId());
		payload.put("execution_id", request.aiExecutionId());
		payload.put("child_status", "failed");
		payload.put("result_status", "failed");
		payload.put("error_code", code);
		payload.put("error", Map.of("code", code, "message", "AI problem generation could not complete"));
		return envelope(eventId, "worker_job.failed", request, payload, now);
	}

	private List<JsonNode> flattenItems(JsonNode wrappers) {
		if (wrappers == null || !wrappers.isArray()) return List.of();
		List<JsonNode> flattened = new ArrayList<>();
		for (JsonNode wrapper : wrappers) {
			JsonNode raw = wrapper.get("item");
			if (raw == null || !raw.isObject()) continue;
			ObjectNode item = ((ObjectNode)raw).deepCopy();
			String itemId = optionalText(wrapper, "item_id");
			if (itemId != null) item.put("item_id", itemId);
			String validation = optionalText(wrapper, "status");
			if (validation != null) item.put("validation_status", validation);
			JsonNode answer = item.get("answer");
			if (answer != null && answer.isObject() && answer.get("correct_no") != null) {
				item.set("correct_answer", answer.get("correct_no"));
			}
			flattened.add(item);
		}
		return List.copyOf(flattened);
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

	private static JsonNode requiredObject(JsonNode node, String field) {
		JsonNode value = object(node, field);
		if (value == null) throw new IllegalArgumentException(field + " must be an object");
		return value;
	}

	private static JsonNode object(JsonNode node, String field) {
		JsonNode value = node == null ? null : node.get(field);
		return value != null && value.isObject() ? value : null;
	}

	private static String requiredText(JsonNode node, String field) {
		String value = optionalText(node, field);
		if (value == null) throw new IllegalArgumentException(field + " must not be blank");
		return value;
	}

	private static String optionalText(JsonNode node, String field) {
		JsonNode value = node == null ? null : node.get(field);
		return value != null && value.isTextual() && !value.asText().isBlank() ? value.asText() : null;
	}
}
