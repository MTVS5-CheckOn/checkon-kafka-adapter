package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import com.checkon.aiadapter.problem.ai.AiProblemClient.Headers;
import com.checkon.aiadapter.problem.ai.AiProblemClientException;
import com.checkon.aiadapter.problem.ai.HttpAiProblemClient;

import tools.jackson.databind.ObjectMapper;

class HttpAiProblemClientTest {
	private MockRestServiceServer server;
	private HttpAiProblemClient client;
	private final Headers headers = new Headers(
		"tn_0123456789abcdef0123456789abcdef", "request-1", "idem-1");

	@BeforeEach
	void setUp() {
		RestClient.Builder builder = RestClient.builder().baseUrl("http://ai.example.test");
		server = MockRestServiceServer.bindTo(builder).build();
		client = new HttpAiProblemClient(builder.build(), "/v1/problems", new ObjectMapper());
	}

	@Test
	@DisplayName("Given AI 재시작 결과 유실 404 When job을 조회하면 Then 명시 사유를 비일시 오류로 보존한다")
	void preservesResultUnavailableAfterRestartReason() {
		// Given
		server.expect(once(), requestTo("http://ai.example.test/v1/problems/job-1"))
			.andRespond(withStatus(NOT_FOUND).contentType(APPLICATION_JSON).body("""
				{"error":{"code":"NOT_FOUND","message":"result unavailable",
				 "detail":{"reason":"result_unavailable_after_restart"}}}
				"""));

		// When/Then
		assertThatThrownBy(() -> client.job("job-1", headers))
			.isInstanceOf(AiProblemClientException.class)
			.satisfies(exception -> {
				var clientException = (AiProblemClientException)exception;
				assertThat(clientException.code()).isEqualTo("result_unavailable_after_restart");
				assertThat(clientException.isTransientFailure()).isFalse();
			});
		server.verify();
	}

	@Test
	@DisplayName("Given 일반 404 When job을 조회하면 Then 기존 AI_HTTP_404 분류를 유지한다")
	void keepsGenericNotFoundDistinct() {
		// Given
		server.expect(once(), requestTo("http://ai.example.test/v1/problems/missing"))
			.andRespond(withStatus(NOT_FOUND).contentType(APPLICATION_JSON)
				.body("{\"error\":{\"detail\":{\"reason\":\"job_id_absent\"}}}"));

		// When/Then
		assertThatThrownBy(() -> client.job("missing", headers))
			.isInstanceOf(AiProblemClientException.class)
			.satisfies(exception -> assertThat(((AiProblemClientException)exception).code())
				.isEqualTo("AI_HTTP_404"));
		server.verify();
	}
}
