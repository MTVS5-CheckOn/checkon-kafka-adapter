package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.checkon.aiadapter.problem.application.ProblemGenerationAiRequestMapper;

import tools.jackson.databind.ObjectMapper;

class ProblemGenerationAiRequestMapperTest {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final ProblemGenerationAiRequestMapper mapper = new ProblemGenerationAiRequestMapper(objectMapper);

	@Test
	@DisplayName("Given Studio 수동 language 셀 When AI 요청으로 변환하면 Then 결정론 교과 노드를 보완한다")
	void mapsFrontendCellToExplicitAiTarget() throws Exception {
		// Given
		String backend = """
			{"target_source":"teacher_manual","area_tag":"language","type_tags":["infer"]}
			""";

		// When
		var mapped = objectMapper.readTree(mapper.map(backend));

		// Then
		assertThat(mapped.get("manual_targets").get(0).asText())
			.isEqualTo("grammar.sentence-structure");
	}

	@ParameterizedTest
	@CsvSource({
		"reading,reading.information.explicit,passage",
		"literature,literature.expression.technique,work_selection",
		"speech_writing,speech_writing.speech.strategy,passage",
		"media,media.reception.information,passage"
	})
	@DisplayName("Given 자료가 필요한 Studio 영역 When AI 요청으로 변환하면 Then 교과 노드와 조달 기본값을 함께 보완한다")
	void mapsAllFrontendAreas(String area, String expectedTarget, String sourceField) throws Exception {
		// Given
		String backend = """
			{"target_source":"teacher_manual","area_tag":"%s","type_tags":["infer"],"passage":null}
			""".formatted(area);

		// When
		var mapped = objectMapper.readTree(mapper.map(backend));

		// Then
		assertThat(mapped.get("manual_targets").get(0).asText()).isEqualTo(expectedTarget);
		assertThat(mapped.get(sourceField).isObject()).isTrue();
	}
}
