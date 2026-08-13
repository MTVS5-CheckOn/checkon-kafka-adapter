package com.checkon.aiadapter.problem.application;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Component
public class ProblemGenerationAiRequestMapper {
	private final ObjectMapper objectMapper;
	public ProblemGenerationAiRequestMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public String map(String backendRequest) {
		try {
			ObjectNode request = (ObjectNode)objectMapper.readTree(backendRequest);
			validate(request);
			return objectMapper.writeValueAsString(request);
		}
		catch (JacksonException exception) {
			throw new IllegalArgumentException("Backend problem request JSON is invalid", exception);
		}
	}

	private void validate(ObjectNode request) {
		if (!"teacher_manual".equals(request.path("target_source").asText())) {
			throw unsupported("v1 supports only teacher_manual target_source");
		}
		JsonNode typeTags = request.get("type_tags");
		if (typeTags == null || !typeTags.isArray() || typeTags.size() != 1
			|| !typeTags.get(0).isTextual()) {
			throw unsupported("v1 requires exactly one type_tag");
		}
		JsonNode targets=request.get("manual_targets");
		if(targets==null||!targets.isArray()||targets.isEmpty()) throw unsupported("diagnosis selected no evidence-ready node");
		java.util.Set<String> unique=new java.util.HashSet<>();
		for(JsonNode target:targets) {
			if(!target.isTextual()||!target.asText().matches("[a-zA-Z0-9][a-zA-Z0-9._:-]{0,119}")||!unique.add(target.asText()))
				throw unsupported("manual_targets contains an invalid or duplicate node ID");
		}
	}

	private static ProblemGenerationMappingException unsupported(String message) {
		return new ProblemGenerationMappingException("NO_EVIDENCE_READY_TARGET", message);
	}
}
