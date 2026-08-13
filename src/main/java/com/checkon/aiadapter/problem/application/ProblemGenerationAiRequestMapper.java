package com.checkon.aiadapter.problem.application;

import java.util.Map;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

@Component
public class ProblemGenerationAiRequestMapper {
	private static final Map<String, String> DEFAULT_MANUAL_TARGETS = Map.of(
		"language", "grammar.sentence-structure",
		"reading", "reading.information.explicit",
		"literature", "literature.expression.technique",
		"speech_writing", "speech_writing.speech.strategy",
		"media", "media.reception.information"
	);
	private final ObjectMapper objectMapper;

	public ProblemGenerationAiRequestMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public String map(String backendRequest) {
		try {
			ObjectNode request = (ObjectNode)objectMapper.readTree(backendRequest);
			if ("teacher_manual".equals(request.path("target_source").asText())
				&& !request.has("manual_targets")) {
				String area = request.path("area_tag").asText();
				String target = DEFAULT_MANUAL_TARGETS.get(area);
				if (target == null) {
					throw new IllegalArgumentException("No AI manual target mapping for area_tag=" + area);
				}
				ArrayNode targets = objectMapper.createArrayNode().add(target);
				request.set("manual_targets", targets);
			}
			applySourceDefaults(request);
			return objectMapper.writeValueAsString(request);
		}
		catch (JacksonException exception) {
			throw new IllegalArgumentException("Backend problem request JSON is invalid", exception);
		}
	}

	private void applySourceDefaults(ObjectNode request) {
		String area = request.path("area_tag").asText();
		switch (area) {
			case "reading" -> request.set("passage", objectMapper.createObjectNode()
				.put("area_tag", "reading").put("domain", "fusion").put("word_count", 500)
				.put("sentence_complexity", "standard").put("paragraph_count", 3)
				.put("banned_topics_version", "v1"));
			case "literature" -> request.set("work_selection", objectMapper.createObjectNode()
				.put("genre", "modern_poetry"));
			case "speech_writing" -> request.set("passage", objectMapper.createObjectNode()
				.put("area_tag", "speech_writing").put("source_kind", "presentation")
				.put("banned_topics_version", "v1"));
			case "media" -> request.set("passage", objectMapper.createObjectNode()
				.put("area_tag", "media").put("source_kind", "single")
				.put("banned_topics_version", "v1"));
			default -> { /* language needs no source procurement object */ }
		}
	}
}
