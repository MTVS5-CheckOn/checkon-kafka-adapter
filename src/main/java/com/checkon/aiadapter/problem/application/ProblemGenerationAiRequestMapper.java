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
		String area=request.path("area_tag").asText();
		if(!java.util.Set.of("language","reading","literature","speech_writing","media").contains(area))
			throw unsupported("area_tag is not supported");
		JsonNode passage=request.get("passage");
		JsonNode work=request.get("work_selection");
		if("language".equals(area)&&(present(passage)||present(work)))
			throw unsupported("language does not accept source material");
		if("reading".equals(area)) validateReading(passage,work);
		if("literature".equals(area)) validateLiterature(passage,work);
		if("speech_writing".equals(area)||"media".equals(area)) validateSource(area,passage,work);
	}

	private static void validateReading(JsonNode passage,JsonNode work) {
		if(!object(passage)||present(work)||!"reading".equals(passage.path("area_tag").asText())
			||!java.util.Set.of("humanities","social","science","tech","art","fusion").contains(passage.path("domain").asText())
			||!java.util.Set.of("basic","standard","advanced").contains(passage.path("sentence_complexity").asText())
			||passage.path("word_count").asInt(0)<1||passage.path("paragraph_count").asInt(0)<2
			||passage.path("paragraph_count").asInt()>6||!"pg-banned-v1".equals(passage.path("banned_topics_version").asText()))
			throw unsupported("reading source contract is invalid");
	}

	private static void validateLiterature(JsonNode passage,JsonNode work) {
		if(present(passage)||!object(work)
			||!java.util.Set.of("classical_poetry","modern_poetry","modern_novel").contains(work.path("genre").asText())
			||!work.path("concept_keywords").isArray())
			throw unsupported("literature work_selection contract is invalid");
	}

	private static void validateSource(String area,JsonNode passage,JsonNode work) {
		java.util.Set<String> kinds="media".equals(area)?java.util.Set.of("single","paired"):
			java.util.Set.of("presentation","writing_draft","writing_sources");
		if(!object(passage)||present(work)||!area.equals(passage.path("area_tag").asText())
			||!kinds.contains(passage.path("source_kind").asText())
			||!"pg-banned-v1".equals(passage.path("banned_topics_version").asText()))
			throw unsupported(area+" source contract is invalid");
	}

	private static boolean object(JsonNode value){return value!=null&&value.isObject();}
	private static boolean present(JsonNode value){return value!=null&&!value.isNull();}

	private static ProblemGenerationMappingException unsupported(String message) {
		return new ProblemGenerationMappingException("NO_EVIDENCE_READY_TARGET", message);
	}
}
