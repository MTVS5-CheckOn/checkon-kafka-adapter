package com.checkon.aiadapter.problem.kafka;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class ProblemGenerationRequestDecoder {
	private static final Pattern TENANT_ALIAS = Pattern.compile("tn_[0-9a-f]{32}");
	private static final Pattern STUDENT_ALIAS = Pattern.compile("st_[0-9a-f]{32}");
	private static final Pattern CLASS_ALIAS = Pattern.compile("cl_[0-9a-f]{32}");
	private static final Pattern IDEMPOTENCY = Pattern.compile("[A-Za-z0-9._:-]{8,200}");
	private final ObjectMapper objectMapper;

	public ProblemGenerationRequestDecoder(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public ProblemGenerationRequestedEvent decode(String messageKey, String rawPayload) {
		try {
			JsonNode root = objectMapper.readTree(rawPayload);
			if (!"problem_generation.requested".equals(text(root, "event_type"))) {
				throw invalid("event_type must be problem_generation.requested");
			}
			if (!"pg-child-request-1".equals(text(root, "schema_version"))) {
				throw invalid("schema_version must be pg-child-request-1");
			}
			String tenant = text(root, "tenant_id");
			if (!TENANT_ALIAS.matcher(tenant).matches() || !tenant.equals(messageKey)) {
				throw invalid("Kafka key and tenant_id must be the same opaque alias");
			}
			JsonNode payload = object(root, "payload");
			JsonNode request = object(payload, "request");
			validateTargetAlias(request);
			UUID requestId = uuid(payload, "problem_request_id");
			UUID correlationId = uuid(root, "correlation_id");
			if (!requestId.equals(correlationId)) {
				throw invalid("correlation_id and problem_request_id must match");
			}
			int targetIndex = integer(payload, "target_index");
			if (targetIndex < 0) throw invalid("target_index must be non-negative");
			String idempotencyKey = text(payload, "idempotency_key");
			if (!IDEMPOTENCY.matcher(idempotencyKey).matches()) {
				throw invalid("idempotency_key must be safe ASCII");
			}
			return new ProblemGenerationRequestedEvent(
				uuid(root, "event_id"), instant(root, "occurred_at"), tenant, requestId,
				uuid(payload, "problem_execution_id"), targetIndex,
				idempotencyKey, request.deepCopy());
		}
		catch (InvalidProblemGenerationRequestException exception) {
			throw exception;
		}
		catch (JacksonException | IllegalArgumentException exception) {
			throw new InvalidProblemGenerationRequestException(
				"Problem generation request is invalid", exception);
		}
	}

	private static void validateTargetAlias(JsonNode request) {
		String kind = text(request, "target_kind");
		String ref = text(request, "target_ref");
		boolean valid = "student".equals(kind)
			? STUDENT_ALIAS.matcher(ref).matches()
			: "class".equals(kind) && CLASS_ALIAS.matcher(ref).matches();
		if (!valid) throw invalid("target_ref must be an opaque alias matching target_kind");
	}

	private static JsonNode object(JsonNode node, String field) {
		JsonNode value = node == null ? null : node.get(field);
		if (value == null || !value.isObject()) throw invalid(field + " must be an object");
		return value;
	}

	private static String text(JsonNode node, String field) {
		JsonNode value = node == null ? null : node.get(field);
		if (value == null || !value.isTextual() || value.asText().isBlank()) {
			throw invalid(field + " must not be blank");
		}
		return value.asText().trim();
	}

	private static UUID uuid(JsonNode node, String field) {
		return UUID.fromString(text(node, field));
	}

	private static int integer(JsonNode node, String field) {
		JsonNode value = node == null ? null : node.get(field);
		if (value == null || !value.canConvertToInt()) throw invalid(field + " must be an integer");
		return value.asInt();
	}

	private static Instant instant(JsonNode node, String field) {
		try {
			return Instant.parse(text(node, field));
		}
		catch (DateTimeParseException exception) {
			throw invalid(field + " must be an ISO-8601 instant");
		}
	}

	private static InvalidProblemGenerationRequestException invalid(String message) {
		return new InvalidProblemGenerationRequestException(message);
	}
}
