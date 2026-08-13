package com.checkon.aiadapter.problem.application;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

@Component
public class ProblemGenerationAiRequestMapper {
	private final ObjectMapper objectMapper;
	private final ProblemGenerationNodeProperties nodes;

	public ProblemGenerationAiRequestMapper(ObjectMapper objectMapper, ProblemGenerationNodeProperties nodes) {
		this.objectMapper = objectMapper;
		this.nodes = nodes;
	}

	public String map(String backendRequest) {
		try {
			ObjectNode request = (ObjectNode)objectMapper.readTree(backendRequest);
			String target = targetFor(request);
			ArrayNode targets = objectMapper.createArrayNode().add(target);
			request.set("manual_targets", targets);
			return objectMapper.writeValueAsString(request);
		}
		catch (JacksonException exception) {
			throw new IllegalArgumentException("Backend problem request JSON is invalid", exception);
		}
	}

	private String targetFor(ObjectNode request) {
		if (!"teacher_manual".equals(request.path("target_source").asText())) {
			throw unsupported("v1 supports only teacher_manual target_source");
		}
		String area = request.path("area_tag").asText();
		if (!"language".equals(area)) {
			throw unsupported("v1 has no evidence-ready node for area_tag=" + area);
		}
		JsonNode typeTags = request.get("type_tags");
		if (typeTags == null || !typeTags.isArray() || typeTags.size() != 1
			|| !typeTags.get(0).isTextual()) {
			throw unsupported("v1 requires exactly one type_tag");
		}
		return switch (typeTags.get(0).asText().toLowerCase(java.util.Locale.ROOT)) {
			case "concept" -> nodes.languageConcept();
			case "infer" -> nodes.languageInfer();
			default -> throw unsupported("v1 supports only CONCEPT or INFER type_tag");
		};
	}

	private static ProblemGenerationMappingException unsupported(String message) {
		return new ProblemGenerationMappingException("NO_EVIDENCE_READY_TARGET", message);
	}
}
