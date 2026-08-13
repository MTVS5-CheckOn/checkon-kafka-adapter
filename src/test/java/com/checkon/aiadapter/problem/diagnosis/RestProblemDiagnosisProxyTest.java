package com.checkon.aiadapter.problem.diagnosis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class RestProblemDiagnosisProxyTest {
	private MockRestServiceServer server;
	private RestProblemDiagnosisProxy proxy;

	@BeforeEach
	void setUp() {
		RestClient.Builder builder = RestClient.builder().baseUrl("http://ai.example.test");
		server = MockRestServiceServer.bindTo(builder).build();
		proxy = new RestProblemDiagnosisProxy(builder.build(), "/v1/diagnosis");
	}

	@Test
	@DisplayName("Given 진단 JSON과 추적 헤더 When AI로 전달하면 Then 본문과 세 헤더를 그대로 보존한다")
	void forwardsPayloadAndTraceHeadersWithoutMutation() {
		// Given
		String payload = "{\"student_id\":\"st_0123456789abcdef0123456789abcdef\"}";
		var headers = new ProblemDiagnosisProxy.Headers(
			"tn_0123456789abcdef0123456789abcdef", "diagnosis-request-48", "diagnosis-idem-48");
		server.expect(once(), requestTo("http://ai.example.test/v1/diagnosis"))
			.andExpect(method(POST))
			.andExpect(header("X-Tenant-Id", headers.tenantAlias()))
			.andExpect(header("X-Request-Id", headers.requestId()))
			.andExpect(header("Idempotency-Key", headers.idempotencyKey()))
			.andExpect(content().json(payload))
			.andRespond(withSuccess("{\"data\":{\"status\":\"generated\"}}", APPLICATION_JSON));

		// When
		var response = proxy.diagnose(payload, headers);

		// Then
		assertThat(response.getStatusCode()).isEqualTo(OK);
		assertThat(response.getBody()).contains("generated");
		server.verify();
	}
}
