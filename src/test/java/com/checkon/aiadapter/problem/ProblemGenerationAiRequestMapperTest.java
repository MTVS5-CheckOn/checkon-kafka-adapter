package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.checkon.aiadapter.problem.application.ProblemGenerationAiRequestMapper;
import com.checkon.aiadapter.problem.application.ProblemGenerationMappingException;
import com.checkon.aiadapter.problem.application.ProblemGenerationNodeProperties;

import tools.jackson.databind.ObjectMapper;

class ProblemGenerationAiRequestMapperTest {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final ProblemGenerationAiRequestMapper mapper = new ProblemGenerationAiRequestMapper(
		objectMapper, new ProblemGenerationNodeProperties("node.concept", "node.infer"));

	@ParameterizedTest
	@CsvSource({"concept,node.concept", "infer,node.infer"})
	@DisplayName("Given v1 language 셀 When AI 요청으로 변환하면 Then 유형별 설정 노드를 보완한다")
	void mapsSupportedLanguageCell(String type, String expectedNode) throws Exception {
		// Given
		String backend = """
			{"target_source":"teacher_manual","area_tag":"language","type_tags":["%s"]}
			""".formatted(type);

		// When
		var mapped = objectMapper.readTree(mapper.map(backend));

		// Then
		assertThat(mapped.get("manual_targets").get(0).asText())
			.isEqualTo(expectedNode);
	}

	@ParameterizedTest
	@CsvSource({
		"reading,infer",
		"literature,concept",
		"speech_writing,infer",
		"media,concept",
		"language,fact"
	})
	@DisplayName("Given v1 미지원 셀 When AI 요청으로 변환하면 Then AI 호출 전에 명시적 사유로 거절한다")
	void rejectsUnsupportedCell(String area, String type) {
		// Given
		String backend = """
			{"target_source":"teacher_manual","area_tag":"%s","type_tags":["%s"]}
			""".formatted(area, type);

		// When/Then
		assertThatThrownBy(() -> mapper.map(backend))
			.isInstanceOf(ProblemGenerationMappingException.class)
			.satisfies(exception -> assertThat(((ProblemGenerationMappingException)exception).code())
				.isEqualTo("NO_EVIDENCE_READY_TARGET"));
	}
}
