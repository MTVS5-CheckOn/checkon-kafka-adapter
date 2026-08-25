package com.checkon.aiadapter.counsel.synchronous;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpStatus.BAD_GATEWAY;
import static org.springframework.http.HttpStatus.GATEWAY_TIMEOUT;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.headerDoesNotExist;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

@DisplayName("라벨 제안과 공용 확정 HTTP 프록시")
class RestCounselSynchronousProxyTest {

	private static final String CLASSIFY_BASE_URL = "http://classify-ai.example.test";
	private static final String LABELS_BASE_URL = "http://labels-ai.example.test";
	private static final String TENANT = "tn_0123456789abcdef0123456789abcdef";

	private MockRestServiceServer classifyServer;
	private MockRestServiceServer labelsServer;
	private RestCounselSynchronousProxy proxy;

	@BeforeEach
	void setUp() {
		RestClient.Builder classifyBuilder = RestClient.builder().baseUrl(CLASSIFY_BASE_URL);
		RestClient.Builder labelsBuilder = RestClient.builder().baseUrl(LABELS_BASE_URL);
		classifyServer = MockRestServiceServer.bindTo(classifyBuilder).build();
		labelsServer = MockRestServiceServer.bindTo(labelsBuilder).build();
		proxy = new RestCounselSynchronousProxy(
			classifyBuilder.build(), "/v1/confirmations",
			labelsBuilder.build(), "/v1/labels/suggest", "/v1/confirmations"
		);
	}

	@Nested
	@DisplayName("Given Backend의 라벨 제안을 중계할 때")
	class GivenForwardingLabelSuggestions {

		@Test
		@DisplayName("When 실제 계약 요청이면 Then 필수 헤더와 전체 JSON을 전달하고 Idempotency-Key는 추가하지 않는다")
		void forwardsTheCompleteBackendFixture() throws Exception {
			String request = readFixture("post_labels_suggest.request.json");
			String response = readFixture("post_labels_suggest.200.json");
			labelsServer.expect(once(), requestTo(LABELS_BASE_URL + "/v1/labels/suggest"))
				.andExpect(method(POST))
				.andExpect(header("X-Tenant-Id", TENANT))
				.andExpect(header("X-Request-Id", "req-label-3000"))
				.andExpect(headerDoesNotExist("Idempotency-Key"))
				.andExpect(content().string(request))
				.andRespond(withSuccess(response, APPLICATION_JSON));

			var actual = proxy.suggestLabels(request, headers("req-label-3000"));

			assertThat(actual.getStatusCode()).isEqualTo(OK);
			assertThat(actual.getBody()).isEqualTo(response);
			labelsServer.verify();
		}

		@Test
		@DisplayName("When AI가 빈 suggestions를 반환하면 Then 정상 200 본문을 그대로 전달한다")
		void relaysAnEmptySuggestionsResponse() throws Exception {
			String response = readFixture("post_labels_suggest.200.empty.json");
			labelsServer.expect(once(), requestTo(LABELS_BASE_URL + "/v1/labels/suggest"))
				.andRespond(withSuccess(response, APPLICATION_JSON));

			var actual = proxy.suggestLabels(readFixture("post_labels_suggest.request.json"), headers("req-empty-3001"));

			assertThat(actual.getStatusCode()).isEqualTo(OK);
			assertThat(actual.getBody()).isEqualTo(response);
			labelsServer.verify();
		}

		@ParameterizedTest(name = "When AI가 {0}을 반환하면 Then 상태와 전체 본문을 그대로 전달한다")
		@MethodSource("com.checkon.aiadapter.counsel.synchronous.RestCounselSynchronousProxyTest#errorResponses")
		void relaysEveryContractError(int status, String fixture) throws Exception {
			String response = readFixture(fixture);
			labelsServer.expect(once(), requestTo(LABELS_BASE_URL + "/v1/labels/suggest"))
				.andRespond(withStatus(HttpStatus.valueOf(status)).contentType(APPLICATION_JSON).body(response));

			var actual = proxy.suggestLabels(readFixture("post_labels_suggest.request.json"), headers("req-error-3002"));

			assertThat(actual.getStatusCode().value()).isEqualTo(status);
			assertThat(actual.getBody()).isEqualTo(response);
			labelsServer.verify();
		}

		@Test
		@DisplayName("When AI 연결이 실패하면 Then 한 번만 호출하고 502 AI_UNAVAILABLE로 변환한다")
		void mapsConnectionFailureWithoutRetry() throws Exception {
			labelsServer.expect(once(), requestTo(LABELS_BASE_URL + "/v1/labels/suggest"))
				.andRespond(request -> {
					throw new IOException(new ConnectException("connection refused"));
				});

			var actual = proxy.suggestLabels(readFixture("post_labels_suggest.request.json"), headers("req-network-3003"));

			assertThat(actual.getStatusCode()).isEqualTo(BAD_GATEWAY);
			assertThat(actual.getBody()).isEqualTo("{\"error\":{\"code\":\"AI_UNAVAILABLE\"}}");
			labelsServer.verify();
		}

		@Test
		@DisplayName("When Adapter 읽기 타임아웃이 발생하면 Then 한 번만 호출하고 504 AI_TIMEOUT을 반환한다")
		void mapsReadTimeoutWithoutRetry() throws Exception {
			labelsServer.expect(once(), requestTo(LABELS_BASE_URL + "/v1/labels/suggest"))
				.andRespond(request -> {
					throw new IOException(new HttpTimeoutException("read timed out"));
				});

			var actual = proxy.suggestLabels(readFixture("post_labels_suggest.request.json"), headers("req-timeout-3004"));

			assertThat(actual.getStatusCode()).isEqualTo(GATEWAY_TIMEOUT);
			assertThat(actual.getBody()).isEqualTo("{\"error\":{\"code\":\"AI_TIMEOUT\"}}");
			labelsServer.verify();
		}
	}

	@Nested
	@DisplayName("Given Backend의 확정 피드백을 중계할 때")
	class GivenForwardingConfirmations {

		@Test
		@DisplayName("When classification과 label을 보내면 Then kind별 AI 설정에 원문 그대로 각각 전달한다")
		void forwardsBothKindsToTheirConfiguredAiServers() throws Exception {
			String classification = """
				{"kind":"classification","suggestion_id":"iq_204","action":"corrected",
				 "corrected_value":{"topic":"counsel_request"}}
				""";
			String label = """
				{"kind":"label","suggestion_id":"gd_11b0:comm:data","action":"confirmed"}
				""";
			String accepted = readFixture("confirmation-response-accepted.json");
			classifyServer.expect(once(), requestTo(CLASSIFY_BASE_URL + "/v1/confirmations"))
				.andExpect(method(POST))
				.andExpect(header("X-Tenant-Id", TENANT))
				.andExpect(header("X-Request-Id", "req-classification-4000"))
				.andExpect(headerDoesNotExist("Idempotency-Key"))
				.andExpect(content().string(classification))
				.andRespond(withSuccess(accepted, APPLICATION_JSON));
			labelsServer.expect(once(), requestTo(LABELS_BASE_URL + "/v1/confirmations"))
				.andExpect(method(POST))
				.andExpect(header("X-Tenant-Id", TENANT))
				.andExpect(header("X-Request-Id", "req-label-4001"))
				.andExpect(headerDoesNotExist("Idempotency-Key"))
				.andExpect(content().string(label))
				.andRespond(withSuccess(accepted, APPLICATION_JSON));

			var classificationResponse = proxy.confirmClassification(
				classification, headers("req-classification-4000"));
			var labelResponse = proxy.confirmLabel(label, headers("req-label-4001"));

			assertThat(classificationResponse.getStatusCode()).isEqualTo(OK);
			assertThat(labelResponse.getStatusCode()).isEqualTo(OK);
			assertThat(classificationResponse.getBody()).isEqualTo(accepted);
			assertThat(labelResponse.getBody()).isEqualTo(accepted);
			classifyServer.verify();
			labelsServer.verify();
		}

		@Test
		@DisplayName("When label 확정이 504이면 Then 재전송하지 않고 상태와 본문을 그대로 전달한다")
		void doesNotRetryANonIdempotentLabelConfirmation() throws Exception {
			String response = readFixture("post_labels_suggest.504.timeout.json");
			labelsServer.expect(once(), requestTo(LABELS_BASE_URL + "/v1/confirmations"))
				.andRespond(withStatus(HttpStatus.GATEWAY_TIMEOUT).contentType(APPLICATION_JSON).body(response));

			var actual = proxy.confirmLabel(
				"{\"kind\":\"label\",\"suggestion_id\":\"gd_11b0:comm:data\",\"action\":\"confirmed\"}",
				headers("req-label-timeout")
			);

			assertThat(actual.getStatusCode()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
			assertThat(actual.getBody()).isEqualTo(response);
			labelsServer.verify();
		}
	}

	static Stream<Arguments> errorResponses() {
		return Stream.of(
			Arguments.of(400, "post_labels_suggest.400.body_schema.json"),
			Arguments.of(500, "post_labels_suggest.500.internal.json"),
			Arguments.of(503, "post_labels_suggest.503.upstream_down.json"),
			Arguments.of(504, "post_labels_suggest.504.timeout.json")
		);
	}

	private CounselSynchronousProxy.Headers headers(String requestId) {
		return new CounselSynchronousProxy.Headers(TENANT, requestId);
	}

	private String readFixture(String name) throws IOException {
		String path = "contracts/labels/" + name;
		try (var input = getClass().getClassLoader().getResourceAsStream(path)) {
			assertThat(input).as("fixture %s", path).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
