package com.checkon.aiadapter.counsel.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpStatus.BAD_GATEWAY;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.headerDoesNotExist;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.IOException;
import java.net.ConnectException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class RestCounselDraftProxyTest {

	private static final String BASE_URL = "http://ai.example.test";

	private MockRestServiceServer server;
	private RestCounselDraftProxy proxy;

	@BeforeEach
	void setUp() {
		RestClient.Builder builder = RestClient.builder().baseUrl(BASE_URL);
		server = MockRestServiceServer.bindTo(builder).build();
		RestClient restClient = builder.build();
		proxy = new RestCounselDraftProxy(restClient, restClient, "/v1/counsel/drafts");
	}

	@Nested
	@DisplayName("Given 초안 상태를 중계할 때")
	class GivenForwardingAGetDraft {

		@Test
		@DisplayName("When AI가 정상 응답하면 Then 본문과 X-Tenant-Id를 그대로 전달한다")
		void forwardsGetWithoutIdempotencyKey() {
			var headers = new CounselDraftProxy.Headers("tn_demo_teacher", "req-1", null);
			server.expect(once(), requestTo(BASE_URL + "/v1/counsel/drafts/019846dc-7c00-7000-8000-0000000006a1"))
				.andExpect(method(GET))
				.andExpect(header("X-Tenant-Id", "tn_demo_teacher"))
				.andExpect(header("X-Request-Id", "req-1"))
				.andExpect(headerDoesNotExist("Idempotency-Key"))
				.andRespond(withSuccess("{\"data\":{\"status\":\"succeeded\"}}", APPLICATION_JSON));

			var response = proxy.getDraft("019846dc-7c00-7000-8000-0000000006a1", headers);

			assertThat(response.getStatusCode()).isEqualTo(OK);
			assertThat(response.getBody()).contains("succeeded");
			server.verify();
		}

		@Test
		@DisplayName("When AI가 404를 반환하면 Then 상태 코드와 본문을 그대로 전달한다")
		void relaysAiErrorStatusAndBody() {
			var headers = new CounselDraftProxy.Headers("tn_demo_teacher", null, null);
			server.expect(once(), requestTo(BASE_URL + "/v1/counsel/drafts/missing-job"))
				.andExpect(method(GET))
				.andRespond(withStatus(NOT_FOUND).contentType(APPLICATION_JSON)
					.body("{\"error\":{\"code\":\"NOT_FOUND\"}}"));

			var response = proxy.getDraft("missing-job", headers);

			assertThat(response.getStatusCode()).isEqualTo(NOT_FOUND);
			assertThat(response.getBody()).contains("NOT_FOUND");
			server.verify();
		}

		@Test
		@DisplayName("When AI 서버에 연결할 수 없으면 Then 502와 AI_UNAVAILABLE을 반환한다")
		void mapsNetworkFailureToBadGateway() {
			var headers = new CounselDraftProxy.Headers("tn_demo_teacher", null, null);
			server.expect(once(), requestTo(BASE_URL + "/v1/counsel/drafts/019846dc-7c00-7000-8000-0000000006a1"))
				.andExpect(method(GET))
				.andRespond(request -> {
					throw new IOException(new ConnectException("connection refused"));
				});

			var response = proxy.getDraft("019846dc-7c00-7000-8000-0000000006a1", headers);

			assertThat(response.getStatusCode()).isEqualTo(BAD_GATEWAY);
			assertThat(response.getBody()).contains("AI_UNAVAILABLE");
			server.verify();
		}
	}

	@Nested
	@DisplayName("Given 초안을 다듬는 요청을 중계할 때")
	class GivenForwardingARefine {

		@Test
		@DisplayName("When Idempotency-Key와 본문이 있으면 Then 있는 그대로 AI에 전달한다")
		void forwardsRefineWithIdempotencyKeyAndBody() {
			String payload = "{\"instruction\":\"조금 더 부드럽게\",\"turn_no\":1}";
			var headers = new CounselDraftProxy.Headers("tn_demo_teacher", "req-2", "turn-uuid-1");
			server.expect(once(), requestTo(BASE_URL + "/v1/counsel/drafts/019846dc-7c00-7000-8000-0000000006a1/refine"))
				.andExpect(method(POST))
				.andExpect(header("X-Tenant-Id", "tn_demo_teacher"))
				.andExpect(header("X-Request-Id", "req-2"))
				.andExpect(header("Idempotency-Key", "turn-uuid-1"))
				.andExpect(content().json(payload))
				.andRespond(withSuccess("{\"data\":{\"applied\":true}}", APPLICATION_JSON));

			var response = proxy.refine("019846dc-7c00-7000-8000-0000000006a1", payload, headers);

			assertThat(response.getStatusCode()).isEqualTo(OK);
			assertThat(response.getBody()).contains("applied");
			server.verify();
		}

		@Test
		@DisplayName("When AI가 5xx를 반환하면 Then 상태 코드를 그대로 전달한다")
		void relaysAiServerErrorStatus() {
			String payload = "{\"instruction\":\"조금 더 부드럽게\",\"turn_no\":1}";
			var headers = new CounselDraftProxy.Headers("tn_demo_teacher", null, "turn-uuid-2");
			server.expect(once(), requestTo(BASE_URL + "/v1/counsel/drafts/019846dc-7c00-7000-8000-0000000006a1/refine"))
				.andExpect(method(POST))
				.andRespond(withServerError());

			var response = proxy.refine("019846dc-7c00-7000-8000-0000000006a1", payload, headers);

			assertThat(response.getStatusCode().is5xxServerError()).isTrue();
			server.verify();
		}
	}
}
