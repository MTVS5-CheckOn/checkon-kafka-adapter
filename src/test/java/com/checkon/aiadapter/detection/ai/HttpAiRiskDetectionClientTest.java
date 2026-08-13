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
import java.time.LocalDate;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

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
		client = new HttpAiRiskDetectionClient(
			builder.build(), "/v1/detect", objectMapper
		);

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
		assertThat(response.data().stats().r1ThresholdPp()).isEqualByComparingTo("12.50");
		assertThat(response.data().stats().r1ThresholdSource()).isEqualTo("pooled");
		assertThat(response.data().stats().r1PoolN()).isEqualTo(42);
		assertThat(response.meta().executionId()).isEqualTo("ai-execution-1");
		server.verify();
	}

	@Test
	@DisplayName("Given 진단 확장 필드가 없는 기존 AI 응답 When 역직렬화하면 Then nullable stats로 호환한다")
	void readsLegacyStatsWithoutR1Diagnostics() {
		// Given
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andRespond(withSuccess("""
				{
				  "data":{"signals":[],"stats":{"students_evaluated":1,
				    "signals_raised":0,"excluded_under_2w":0,"capped_out":0,
				    "rules_skipped":[]}},
				  "error":null,
				  "meta":{"execution_id":"legacy-execution","versions":{"contract":"0.2"}}
				}
				""", APPLICATION_JSON));

		// When
		AiDetectionResponse response = client.detect(request, headers);

		// Then
		assertThat(response.data().stats().r1ThresholdPp()).isNull();
		assertThat(response.data().stats().r1ThresholdSource()).isNull();
		assertThat(response.data().stats().r1PoolN()).isNull();
		server.verify();
	}

	@Test
	@DisplayName("Given structured signal and evidence When AI response is read Then every field is retained")
	void retainsStructuredSignalAndEvidenceFields() {
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andRespond(withSuccess("""
				{
				  "data":{"signals":[{
				    "signal_id":"signal-1","student_ref":"st_0123456789abcdef0123456789abcdef",
				    "class_ref":"cl_0123456789abcdef0123456789abcdef","rule_id":"R1",
				    "signal_type":"acc_drop","display_label":"정답률 하락",
				    "metric":"accuracy","observed":0.5,"baseline":0.8,"sample_size":20,
				    "score":1.0,"rank":1,"advisory":false,"lifecycle":"new",
				    "brief":{"text":"확인이 필요합니다","gate_passed":true,"fallback_used":false},
				    "evidence":[{"source_table":"learning_event","record_id":"le_1",
				      "summary":"근거","role":"trigger","observed":0.5,
				      "sample_size":20,"occurred_on":"2026-08-10"}]
				  }],"stats":{"students_evaluated":1,"signals_raised":1,
				    "excluded_under_2w":0,"capped_out":0,"rules_skipped":[]}},
				  "error":null,"meta":{"execution_id":"structured","versions":{"contract":"0.2"}}
				}
				""", APPLICATION_JSON));

		AiDetectionResponse response = client.detect(request, headers);

		assertThat(response.data().signals()).singleElement().satisfies(signal -> {
			assertThat(signal.metric()).isEqualTo("accuracy");
			assertThat(signal.observed()).isEqualByComparingTo("0.5");
			assertThat(signal.baseline()).isEqualByComparingTo("0.8");
			assertThat(signal.sampleSize()).isEqualTo(20);
			assertThat(signal.evidence()).singleElement().satisfies(evidence -> {
				assertThat(evidence.role()).isEqualTo("trigger");
				assertThat(evidence.observed()).isEqualByComparingTo("0.5");
				assertThat(evidence.sampleSize()).isEqualTo(20);
				assertThat(evidence.occurredOn()).isEqualTo(LocalDate.of(2026, 8, 10));
			});
		});
		server.verify();
	}

	@Test
	@DisplayName("Given Kafka 원본 payload When AI HTTP API를 호출하면 Then JSON 문자열을 변경하지 않는다")
	void sendsRawKafkaPayloadWithoutDtoReserialization() throws Exception {
		// Given
		String rawEvent = readFixture("contracts/risk-detection-requested-v0.2.json");
		String requestBody = objectMapper.writeValueAsString(
			objectMapper.readTree(rawEvent).get("payload")
		);
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andExpect(method(POST))
			.andExpect(content().string(requestBody))
			.andRespond(withSuccess(
				readFixture("ai/detect-success-response.json"), APPLICATION_JSON));

		// When
		AiDetectionResponse response = client.detectRaw(requestBody, headers);

		// Then
		assertThat(response.error()).isNull();
		server.verify();
	}

	@Test
	@DisplayName("Given 실제 HTTP 서버 When 원본 payload를 보내면 Then wire body가 JSON과 동일하다")
	void sendsExactJsonOverRealHttpConnection() throws Exception {
		// Given
		String requestBody = objectMapper.writeValueAsString(request);
		AtomicReference<byte[]> received = new AtomicReference<>();
		AtomicReference<String> contentType = new AtomicReference<>();
		AtomicReference<String> contentLength = new AtomicReference<>();
		HttpServer captureServer = HttpServer.create(
			new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		captureServer.createContext("/v1/detect", exchange -> {
			try (exchange) {
				received.set(exchange.getRequestBody().readAllBytes());
				contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
				contentLength.set(exchange.getRequestHeaders().getFirst("Content-Length"));
				byte[] response = readFixture("ai/detect-success-response.json")
					.getBytes(StandardCharsets.UTF_8);
				exchange.getResponseHeaders().set("Content-Type", "application/json");
				exchange.sendResponseHeaders(200, response.length);
				exchange.getResponseBody().write(response);
			}
		});
		captureServer.start();
		try {
			HttpAiRiskDetectionClient wireClient = new HttpAiRiskDetectionClient(
				RestClient.builder().baseUrl("http://127.0.0.1:"
					+ captureServer.getAddress().getPort()).build(),
				"/v1/detect", objectMapper);

			// When
			wireClient.detectRaw(requestBody, headers);

			// Then
			assertThat(new String(received.get(), StandardCharsets.UTF_8))
				.isEqualTo(requestBody);
			assertThat(contentType.get()).startsWith("application/json");
			assertThat(contentLength.get()).isEqualTo(String.valueOf(
				requestBody.getBytes(StandardCharsets.UTF_8).length));
		}
		finally {
			captureServer.stop(0);
		}
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
		String errorBody = "{\"error\":{\"code\":\"INVALID_SCHEMA\","
			+ "\"message\":\"invalid\",\"detail\":{\"field\":\"students.0\"}}}";
		server.expect(once(), requestTo("http://ai.example.test/v1/detect"))
			.andRespond(withStatus(SERVICE_UNAVAILABLE)
				.contentType(APPLICATION_JSON).body(errorBody));

		// When/Then
		assertThatThrownBy(() -> client.detect(request, headers))
			.isInstanceOf(AiRiskDetectionClientException.class)
			.satisfies(exception -> {
				AiRiskDetectionClientException clientException =
					(AiRiskDetectionClientException)exception;
				assertThat(clientException.reason())
					.isEqualTo(AiRiskDetectionClientException.Reason.HTTP_ERROR);
				assertThat(clientException.httpStatus()).isEqualTo(503);
				assertThat(clientException.responseBody()).isEqualTo(errorBody);
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
				new AiRiskDetectionHttpConfiguration().aiRiskDetectionClient(
					properties, objectMapper
				);

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
