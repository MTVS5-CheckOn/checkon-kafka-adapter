package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.checkon.aiadapter.problem.kafka.InvalidProblemGenerationRequestException;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationRequestDecoder;

import tools.jackson.databind.ObjectMapper;

class ProblemGenerationRequestDecoderTest {
	private static final String TENANT = "tn_0123456789abcdef0123456789abcdef";
	private final ProblemGenerationRequestDecoder decoder =
		new ProblemGenerationRequestDecoder(new ObjectMapper());

	@Test
	@DisplayName("Given 원본 UUID target_ref When child 요청을 해석하면 Then opaque alias 경계에서 거절한다")
	void rejectsRawStudentIdentifier() {
		// Given
		String event = event("01980000-0000-7000-8000-000000000099");

		// When & Then
		assertThatThrownBy(() -> decoder.decode(TENANT, event))
			.isInstanceOf(InvalidProblemGenerationRequestException.class)
			.hasMessageContaining("opaque alias");
	}

	private String event(String targetRef) {
		return """
			{"event_id":"01980000-0000-7000-8000-000000000001",
			 "event_type":"problem_generation.requested","occurred_at":"2026-08-13T00:00:00Z",
			 "tenant_id":"%s","schema_version":"pg-child-request-1",
			 "correlation_id":"01980000-0000-7000-8000-000000000002","payload":{
			  "problem_request_id":"01980000-0000-7000-8000-000000000002",
			  "problem_execution_id":"01980000-0000-7000-8000-000000000003","target_index":0,
			  "idempotency_key":"child-request-0","request":{"target_kind":"student",
			  "target_ref":"%s"}}}
			""".formatted(TENANT, targetRef);
	}
}
