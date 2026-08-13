package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.checkon.aiadapter.problem.application.ProblemGenerationAiRequestMapper;
import com.checkon.aiadapter.problem.application.ProblemGenerationMappingException;

import tools.jackson.databind.ObjectMapper;

class ProblemGenerationAiRequestMapperTest {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final ProblemGenerationAiRequestMapper mapper = new ProblemGenerationAiRequestMapper(objectMapper);

	@Test
	@DisplayName("Given Backend가 진단 node를 선택했을 때 When AI 요청으로 변환하면 Then node 목록을 그대로 보존한다")
	void preservesDiagnosisSelectedNodes() throws Exception {
		// Given
		String backend = """
			{"target_source":"teacher_manual","area_tag":"language","type_tags":["infer"],
			 "manual_targets":["node.a","node.b"]}
			""";

		// When
		var mapped = objectMapper.readTree(mapper.map(backend));

		// Then
		assertThat(mapped.get("manual_targets")).extracting(value->value.asText()).containsExactly("node.a","node.b");
	}

	@ParameterizedTest
	@ValueSource(strings={"[]","[\"node.a\",\"node.a\"]","[\"bad node\"]"})
	@DisplayName("Given 비어 있거나 잘못된 node 목록 When AI 요청으로 변환하면 Then AI 호출 전에 명시적 사유로 거절한다")
	void rejectsInvalidNodes(String targets) {
		// Given
		String backend = """
			{"target_source":"teacher_manual","area_tag":"language","type_tags":["infer"],"manual_targets":%s}
			""".formatted(targets);

		// When/Then
		assertThatThrownBy(() -> mapper.map(backend))
			.isInstanceOf(ProblemGenerationMappingException.class)
			.satisfies(exception -> assertThat(((ProblemGenerationMappingException)exception).code())
				.isEqualTo("NO_EVIDENCE_READY_TARGET"));
	}
}
