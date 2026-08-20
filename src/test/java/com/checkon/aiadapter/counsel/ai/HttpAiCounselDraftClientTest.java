package com.checkon.aiadapter.counsel.ai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withException;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class HttpAiCounselDraftClientTest {

	private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();

	private MockRestServiceServer server;
	private HttpAiCounselDraftClient client;
	private AiCounselDraftRequest request;
	private AiCounselDraftRequestHeaders headers;

	@BeforeEach
	void setUp() throws Exception {
		RestClient.Builder builder = RestClient.builder().baseUrl("http://ai.example.test");
		server = MockRestServiceServer.bindTo(builder).build();
		client = new HttpAiCounselDraftClient(builder.build(), "/v1/counsel/drafts", objectMapper);

		CounselDraftRequestedEvent event = objectMapper.readValue(
			readFixture("contracts/counsel-draft-requested-v1.json"), CounselDraftRequestedEvent.class);
		request = event.payload();
		headers = new AiCounselDraftRequestHeaders(
			event.tenantAlias(), event.requestId(), event.idempotencyKey());
	}

	@Test
	@DisplayName("Given 검증된 요청 When AI HTTP API를 호출하면 Then 헤더와 body를 전송하고 202 바디를 읽는다")
	void sendsRequestAndReadsSuccessfulResponse() {
		server.expect(once(), requestTo("http://ai.example.test/v1/counsel/drafts"))
			.andExpect(method(POST))
			.andExpect(header(HttpAiCounselDraftClient.TENANT_ID_HEADER, headers.tenantAlias()))
			.andExpect(header(HttpAiCounselDraftClient.REQUEST_ID_HEADER, headers.requestId()))
			.andExpect(header(HttpAiCounselDraftClient.IDEMPOTENCY_KEY_HEADER, headers.idempotencyKey()))
			.andRespond(withSuccess("""
				{"data":{"job_id":"019846dc-7c00-7000-8000-0000000006a1","status":"queued"},
				 "error":null,"meta":{"execution_id":"ai-exec-1","versions":{"pipeline":"0.1.0"}}}
				""", APPLICATION_JSON));

		AiCounselDraftResponse response = client.createDraft(request, headers);

		assertThat(response.error()).isNull();
		assertThat(response.data().jobId()).isEqualTo("019846dc-7c00-7000-8000-0000000006a1");
		assertThat(response.data().status()).isEqualTo("queued");
		assertThat(response.meta().executionId()).isEqualTo("ai-exec-1");
		server.verify();
	}

	@Test
	@DisplayName("Given Kafka 원본 payload When AI HTTP API를 호출하면 Then JSON 문자열을 변경하지 않는다")
	void sendsRawKafkaPayloadWithoutDtoReserialization() throws Exception {
		String rawEvent = readFixture("contracts/counsel-draft-requested-v1.json");
		String requestBody = objectMapper.writeValueAsString(objectMapper.readTree(rawEvent).get("payload"));
		server.expect(once(), requestTo("http://ai.example.test/v1/counsel/drafts"))
			.andExpect(method(POST))
			.andRespond(withSuccess("""
				{"data":{"job_id":"job-1","status":"queued"},"error":null,
				 "meta":{"execution_id":"ai-exec-1","versions":null}}
				""", APPLICATION_JSON));

		AiCounselDraftResponse response = client.createDraftRaw(requestBody, headers);

		assertThat(response.data().jobId()).isEqualTo("job-1");
		server.verify();
	}

	@Test
	@DisplayName("Given 같은 Idempotency-Key의 다른 요청 When AI가 409를 반환하면 Then 멱등 충돌로 분류한다")
	void mapsIdempotencyConflict() {
		server.expect(once(), requestTo("http://ai.example.test/v1/counsel/drafts"))
			.andRespond(withStatus(CONFLICT));

		assertThatThrownBy(() -> client.createDraft(request, headers))
			.isInstanceOf(AiCounselDraftClientException.class)
			.satisfies(exception -> {
				var clientException = (AiCounselDraftClientException) exception;
				assertThat(clientException.reason()).isEqualTo(AiCounselDraftClientException.Reason.IDEMPOTENCY_CONFLICT);
				assertThat(clientException.httpStatus()).isEqualTo(409);
			});
		server.verify();
	}

	@Test
	@DisplayName("Given AI 일시 장애 When 503을 반환하면 Then HTTP 상태를 보존한다")
	void mapsHttpFailureWithoutDecidingRetryPolicy() {
		String errorBody = "{\"error\":{\"code\":\"UPSTREAM_TIMEOUT\",\"message\":\"timeout\",\"detail\":null}}";
		server.expect(once(), requestTo("http://ai.example.test/v1/counsel/drafts"))
			.andRespond(withStatus(SERVICE_UNAVAILABLE).contentType(APPLICATION_JSON).body(errorBody));

		assertThatThrownBy(() -> client.createDraft(request, headers))
			.isInstanceOf(AiCounselDraftClientException.class)
			.satisfies(exception -> {
				var clientException = (AiCounselDraftClientException) exception;
				assertThat(clientException.reason()).isEqualTo(AiCounselDraftClientException.Reason.HTTP_ERROR);
				assertThat(clientException.httpStatus()).isEqualTo(503);
				assertThat(clientException.responseBody()).isEqualTo(errorBody);
			});
		server.verify();
	}

	@Test
	@DisplayName("Given 연결 실패 When AI를 호출하면 Then 네트워크 오류로 분류한다")
	void mapsNetworkFailure() {
		server.expect(once(), requestTo("http://ai.example.test/v1/counsel/drafts"))
			.andRespond(withException(new IOException("connection refused")));

		assertThatThrownBy(() -> client.createDraft(request, headers))
			.isInstanceOf(AiCounselDraftClientException.class)
			.satisfies(exception -> {
				var clientException = (AiCounselDraftClientException) exception;
				assertThat(clientException.reason()).isEqualTo(AiCounselDraftClientException.Reason.NETWORK_ERROR);
				assertThat(clientException.httpStatus()).isNull();
			});
		server.verify();
	}

	@Test
	@DisplayName("Given body 없는 성공 응답 When AI를 호출하면 Then 빈 응답 오류로 분류한다")
	void rejectsEmptySuccessfulResponse() {
		server.expect(once(), requestTo("http://ai.example.test/v1/counsel/drafts"))
			.andRespond(withSuccess());

		assertThatThrownBy(() -> client.createDraft(request, headers))
			.isInstanceOf(AiCounselDraftClientException.class)
			.satisfies(exception -> assertThat(
				((AiCounselDraftClientException) exception).reason()
			).isEqualTo(AiCounselDraftClientException.Reason.EMPTY_RESPONSE));
		server.verify();
	}

	private String readFixture(String path) throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream(path)) {
			assertThat(input).as("fixture %s", path).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
