package com.checkon.aiadapter.detection.ai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withException;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import com.sun.net.httpserver.HttpServer;

class HttpAiRiskDetectionClientTest {

	private final ObjectMapper objectMapper = JsonMapper.builder()
		.findAndAddModules()
		.build();

	private MockRestServiceServer server;
	private HttpAiRiskDetectionClient client;
	private AiDetectionRequest request;
	private AiDetectionRequestHeaders headers;

	@BeforeEach
	void setUp() throws Exception {
		RestClient.Builder builder = RestClient.builder().baseUrl("http://ai.example.test");
		server = MockRestServiceServer.bindTo(builder).build();
		client = new HttpAiRiskDetectionClient(builder.build(), "/v1/detect");

		RiskDetectionRequestedEvent event = objectMapper.readValue(
			readFixture("contracts/risk-detection-requested-v0.2.json"),
			RiskDetectionRequestedEvent.class
		);
		request = event.payload();
		headers = new AiDetectionRequestHeaders(
			event.tenantAlias(), event.requestId(), event.idempotencyKey());
	}

	@Test
	@DisplayName("Given 검증된 요청 When AI HTTP API를 호출하면 Then 헤더와 body를 전송하고 200 응답을 읽는다")
	void sendsRequestAndReadsSuccessfulResponse() throws Exception {
		// Given
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andExpect(method(POST))
			.andExpect(header(HttpAiRiskDetectionClient.TENANT_ID_HEADER, headers.tenantAlias()))
			.andExpect(header(HttpAiRiskDetectionClient.REQUEST_ID_HEADER, headers.requestId()))
			.andExpect(header(HttpAiRiskDetectionClient.IDEMPOTENCY_KEY_HEADER,
				headers.idempotencyKey()))
			.andExpect(content().json(objectMapper.writeValueAsString(request)))
			.andRespond(withSuccess(readFixture("ai/detect-success-response.json"), APPLICATION_JSON));

		// When
		AiDetectionResponse response = client.detect(request, headers);

		// Then
		assertThat(response.error()).isNull();
		assertThat(response.data().signals()).isEmpty();
		assertThat(response.meta().executionId()).isEqualTo("ai-execution-1");
		server.verify();
	}

	@Test
	@DisplayName("Given 같은 멱등 키의 다른 요청 When AI가 409를 반환하면 Then 멱등 충돌로 분류한다")
	void mapsIdempotencyConflict() {
		// Given
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andRespond(withStatus(CONFLICT));

		// When/Then
		assertThatThrownBy(() -> client.detect(request, headers))
			.isInstanceOf(AiRiskDetectionClientException.class)
			.satisfies(exception -> {
				AiRiskDetectionClientException clientException =
					(AiRiskDetectionClientException)exception;
				assertThat(clientException.reason())
					.isEqualTo(AiRiskDetectionClientException.Reason.IDEMPOTENCY_CONFLICT);
				assertThat(clientException.httpStatus()).isEqualTo(409);
			});
		server.verify();
	}

	@Test
	@DisplayName("Given AI 일시 장애 When 503을 반환하면 Then HTTP 상태를 보존한다")
	void mapsHttpFailureWithoutDecidingRetryPolicy() {
		// Given
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andRespond(withStatus(SERVICE_UNAVAILABLE));

		// When/Then
		assertThatThrownBy(() -> client.detect(request, headers))
			.isInstanceOf(AiRiskDetectionClientException.class)
			.satisfies(exception -> {
				AiRiskDetectionClientException clientException =
					(AiRiskDetectionClientException)exception;
				assertThat(clientException.reason())
					.isEqualTo(AiRiskDetectionClientException.Reason.HTTP_ERROR);
				assertThat(clientException.httpStatus()).isEqualTo(503);
			});
		server.verify();
	}

	@Test
	@DisplayName("Given 연결 실패 When AI를 호출하면 Then 네트워크 오류로 분류한다")
	void mapsNetworkFailure() {
		// Given
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andRespond(withException(new IOException("connection refused")));

		// When/Then
		assertThatThrownBy(() -> client.detect(request, headers))
			.isInstanceOf(AiRiskDetectionClientException.class)
			.satisfies(exception -> {
				AiRiskDetectionClientException clientException =
					(AiRiskDetectionClientException)exception;
				assertThat(clientException.reason())
					.isEqualTo(AiRiskDetectionClientException.Reason.NETWORK_ERROR);
				assertThat(clientException.httpStatus()).isNull();
			});
		server.verify();
	}

	@Test
	@DisplayName("Given read timeout보다 느린 AI 응답 When 호출하면 Then 네트워크 오류로 분류한다")
	void timesOutSlowAiResponse() throws Exception {
		// Given
		HttpServer slowServer = HttpServer.create(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		var executor = Executors.newSingleThreadExecutor();
		slowServer.setExecutor(executor);
		slowServer.createContext("/v1/detect", exchange -> {
			try (exchange) {
				Thread.sleep(300);
				exchange.sendResponseHeaders(200, -1);
			}
			catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
			}
		});
		slowServer.start();

		try {
			AiRiskDetectionHttpProperties properties = new AiRiskDetectionHttpProperties(
				true,
				"http://127.0.0.1:" + slowServer.getAddress().getPort(),
				"/v1/detect",
				Duration.ofSeconds(1),
				Duration.ofMillis(50)
			);
			AiRiskDetectionClient timeoutClient =
				new AiRiskDetectionHttpConfiguration().aiRiskDetectionClient(properties);

			// When/Then
			assertThatThrownBy(() -> timeoutClient.detect(request, headers))
				.isInstanceOf(AiRiskDetectionClientException.class)
				.satisfies(exception -> assertThat(
					((AiRiskDetectionClientException)exception).reason()
				).isEqualTo(AiRiskDetectionClientException.Reason.NETWORK_ERROR));
		}
		finally {
			slowServer.stop(0);
			executor.shutdownNow();
		}
	}

	@Test
	@DisplayName("Given body 없는 성공 응답 When AI를 호출하면 Then 빈 응답 오류로 분류한다")
	void rejectsEmptySuccessfulResponse() {
		// Given
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andRespond(withSuccess());

		// When/Then
		assertThatThrownBy(() -> client.detect(request, headers))
			.isInstanceOf(AiRiskDetectionClientException.class)
			.satisfies(exception -> assertThat(
				((AiRiskDetectionClientException)exception).reason()
			).isEqualTo(AiRiskDetectionClientException.Reason.EMPTY_RESPONSE));
		server.verify();
	}

	private String readFixture(String path) throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream(path)) {
			assertThat(input).as("fixture %s", path).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
