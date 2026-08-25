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

	@Test
	@DisplayName("Given 독서 출제 자료 When AI 요청으로 변환하면 Then passage와 금지 주제 버전을 손실 없이 보존한다")
	void preservesReadingSourceContract() throws Exception {
		// Given
		String backend="""
			{"target_source":"teacher_manual","area_tag":"reading","type_tags":["fact"],
			 "manual_targets":["reading.fact.node"],"passage":{"area_tag":"reading","domain":"science",
			 "topic_hint":"기후 기술","word_count":900,"sentence_complexity":"standard",
			 "paragraph_count":4,"banned_topics_version":"pg-banned-v1"}}
			""";

		// When
		var mapped=objectMapper.readTree(mapper.map(backend));

		// Then
		assertThat(mapped.at("/passage/domain").asText()).isEqualTo("science");
		assertThat(mapped.at("/passage/banned_topics_version").asText()).isEqualTo("pg-banned-v1");
		assertThat(mapped.has("work_selection")).isFalse();
	}

	@Test
	@DisplayName("Given 문학 영역에 passage가 포함되면 When 변환하면 Then AI 호출 전에 잘못된 자료 조합을 거절한다")
	void rejectsMismatchedAreaSource() {
		// Given
		String backend="""
			{"target_source":"teacher_manual","area_tag":"literature","type_tags":["critic"],
			 "manual_targets":["literature.critic.node"],"passage":{"area_tag":"literature"}}
			""";

		// When/Then
		assertThatThrownBy(()->mapper.map(backend)).isInstanceOf(ProblemGenerationMappingException.class)
			.hasMessageContaining("literature work_selection contract is invalid");
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
