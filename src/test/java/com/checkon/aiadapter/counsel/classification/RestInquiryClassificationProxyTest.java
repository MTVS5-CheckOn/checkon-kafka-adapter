package com.checkon.aiadapter.counsel.classification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpStatus.BAD_GATEWAY;
import static org.springframework.http.HttpStatus.GATEWAY_TIMEOUT;
import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

@DisplayName("문의 분류 프록시")
class RestInquiryClassificationProxyTest {

	private static final String BASE_URL = "http://ai.example.test";

	private MockRestServiceServer server;
	private RestInquiryClassificationProxy proxy;

	@BeforeEach
	void setUp() {
		RestClient.Builder builder = RestClient.builder().baseUrl(BASE_URL);
		server = MockRestServiceServer.bindTo(builder).build();
		proxy = new RestInquiryClassificationProxy(builder.build(), "/v1/classify");
	}

	@Nested
	@DisplayName("Given Backend의 문의 분류 요청을 중계할 때")
	class GivenForwardingAClassification {

		@Test
		@DisplayName("When 유효한 요청이면 Then 필수 헤더와 본문을 그대로 AI에 전달한다")
		void forwardsRequiredHeadersAndBodyWithoutIdempotencyKey() {
			String payload = "{\"inquiry_ref\":\"iq_204\",\"body_text\":\"시간표가 궁금합니다\"}";
			server.expect(once(), requestTo(BASE_URL + "/v1/classify"))
				.andExpect(method(POST))
				.andExpect(header("X-Tenant-Id", "tn_0123456789abcdef0123456789abcdef"))
				.andExpect(header("X-Request-Id", "req-2040"))
				.andExpect(headerDoesNotExist("Idempotency-Key"))
				.andExpect(content().string(payload))
				.andRespond(withSuccess("{\"data\":{\"classified\":true}}", APPLICATION_JSON));

			var response = proxy.classify(payload, new InquiryClassificationProxy.Headers(
				"tn_0123456789abcdef0123456789abcdef", "req-2040"
			));

			assertThat(response.getStatusCode()).isEqualTo(OK);
			assertThat(response.getBody()).contains("classified");
			server.verify();
		}

		@Test
		@DisplayName("When AI 성공 응답에 내부 헤더가 있으면 Then JSON Content-Type만 보존한다")
		void keepsJsonContentTypeWithoutRelayingUpstreamHeaders() {
			String responseBody = "{\"data\":{\"classified\":true}}";
			server.expect(once(), requestTo(BASE_URL + "/v1/classify"))
				.andRespond(withSuccess(responseBody, APPLICATION_JSON)
					.header("X-Upstream-Internal", "hidden"));

			var response = proxy.classify(
				"{\"inquiry_ref\":\"iq_204\",\"body_text\":\"문의\"}",
				new InquiryClassificationProxy.Headers(
					"tn_0123456789abcdef0123456789abcdef", "req-headers-2041")
			);

			assertThat(response.getStatusCode()).isEqualTo(OK);
			assertThat(response.getBody()).isEqualTo(responseBody);
			assertThat(response.getHeaders().getContentType()).isEqualTo(APPLICATION_JSON);
			assertThat(response.getHeaders().getFirst("X-Upstream-Internal")).isNull();
			server.verify();
		}

		@Test
		@DisplayName("When AI가 계약 오류를 반환하면 Then 상태와 본문을 그대로 전달한다")
		void relaysAiErrorStatusAndBody() {
			String payload = "{\"inquiry_ref\":\"iq_205\",\"body_text\":\"문의\"}";
			server.expect(once(), requestTo(BASE_URL + "/v1/classify"))
				.andExpect(method(POST))
				.andRespond(withStatus(SERVICE_UNAVAILABLE).contentType(APPLICATION_JSON)
					.body("{\"error\":{\"code\":\"LLM_UPSTREAM_DOWN\"}}"));

			var response = proxy.classify(payload, new InquiryClassificationProxy.Headers(
				"tn_0123456789abcdef0123456789abcdef", "req-2050"
			));

			assertThat(response.getStatusCode()).isEqualTo(SERVICE_UNAVAILABLE);
			assertThat(response.getBody()).contains("LLM_UPSTREAM_DOWN");
			server.verify();
		}

		@Test
		@DisplayName("When AI가 classified=false를 반환하면 Then 상태와 원문 본문을 그대로 전달한다")
		void relaysAnUnclassifiedResponseWithoutReserialization() {
			String responseBody = "{\"data\":{\"classified\":false,\"fallback_reason\":\"tripwire_blocked\"}}";
			server.expect(once(), requestTo(BASE_URL + "/v1/classify"))
				.andRespond(withSuccess(responseBody, APPLICATION_JSON));

			var response = proxy.classify(
				"{\"inquiry_ref\":\"iq_205\",\"body_text\":\"문의\"}",
				new InquiryClassificationProxy.Headers("tn_0123456789abcdef0123456789abcdef", "req-2051")
			);

			assertThat(response.getStatusCode()).isEqualTo(OK);
			assertThat(response.getBody()).isEqualTo(responseBody);
			server.verify();
		}

		@Test
		@DisplayName("When AI에 연결할 수 없으면 Then 재시도 없이 502를 반환한다")
		void mapsNetworkFailureToBadGateway() {
			String payload = "{\"inquiry_ref\":\"iq_206\",\"body_text\":\"문의\"}";
			server.expect(once(), requestTo(BASE_URL + "/v1/classify"))
				.andExpect(method(POST))
				.andRespond(request -> {
					throw new IOException(new ConnectException("connection refused"));
				});

			var response = proxy.classify(payload, new InquiryClassificationProxy.Headers(
				"tn_0123456789abcdef0123456789abcdef", "req-2060"
			));

			assertThat(response.getStatusCode()).isEqualTo(BAD_GATEWAY);
			assertThat(response.getBody()).contains("AI_UNAVAILABLE");
			server.verify();
		}

		@Test
		@DisplayName("When Adapter 읽기 타임아웃이 발생하면 Then 재시도 없이 504를 반환한다")
		void mapsReadTimeoutToGatewayTimeout() {
			server.expect(once(), requestTo(BASE_URL + "/v1/classify"))
				.andRespond(request -> {
					throw new IOException(new HttpTimeoutException("read timed out"));
				});

			var response = proxy.classify(
				"{\"inquiry_ref\":\"iq_207\",\"body_text\":\"문의\"}",
				new InquiryClassificationProxy.Headers("tn_0123456789abcdef0123456789abcdef", "req-2070")
			);

			assertThat(response.getStatusCode()).isEqualTo(GATEWAY_TIMEOUT);
			assertThat(response.getBody()).isEqualTo("{\"error\":{\"code\":\"AI_TIMEOUT\"}}");
			server.verify();
		}
	}
}
