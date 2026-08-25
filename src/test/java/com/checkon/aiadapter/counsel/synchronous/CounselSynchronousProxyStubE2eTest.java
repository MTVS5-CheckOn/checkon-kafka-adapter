package com.checkon.aiadapter.counsel.synchronous;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

@SpringBootTest(
	webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
	properties = "spring.autoconfigure.exclude="
		+ "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
		+ "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
)
@DisplayName("Backend 역할 HTTP 호출에서 Adapter와 AI Stub까지의 동기 왕복")
class CounselSynchronousProxyStubE2eTest {

	private static final String TENANT = "tn_0123456789abcdef0123456789abcdef";
	private static final LinkedBlockingQueue<CapturedRequest> CAPTURED = new LinkedBlockingQueue<>();
	private static final HttpServer AI_STUB = startAiStub();

	@LocalServerPort
	private int adapterPort;

	@DynamicPropertySource
	static void aiProperties(DynamicPropertyRegistry registry) {
		registry.add("checkon.ai.classify.base-url",
			() -> "http://127.0.0.1:" + AI_STUB.getAddress().getPort());
		registry.add("checkon.ai.classify.connect-timeout", () -> "1s");
		registry.add("checkon.ai.classify.read-timeout", () -> "2s");
		registry.add("checkon.ai.labels.base-url",
			() -> "http://127.0.0.1:" + AI_STUB.getAddress().getPort());
		registry.add("checkon.ai.labels.connect-timeout", () -> "1s");
		registry.add("checkon.ai.labels.read-timeout", () -> "2s");
	}

	@AfterAll
	static void stopAiStub() {
		AI_STUB.stop(0);
	}

	@Test
	@DisplayName("Given Backend 분류 요청 When Adapter를 호출하면 Then AI Stub까지 헤더와 원문 본문이 보존된다")
	void roundTripsClassificationOverRealHttp() throws Exception {
		String request = "{\"inquiry_ref\":\"iq_204\",\"body_text\":\"여름방학 특강 시간표가 궁금합니다\"}";

		var response = backendClient().post()
			.uri("/v1/classify")
			.header("X-Tenant-Id", TENANT)
			.header("X-Request-Id", "req-classify-e2e-4999")
			.contentType(MediaType.APPLICATION_JSON)
			.body(request)
			.retrieve()
			.toEntity(String.class);
		CapturedRequest captured = CAPTURED.poll(2, TimeUnit.SECONDS);

		assertThat(response.getStatusCode().value()).isEqualTo(200);
		assertThat(captured).isNotNull();
		assertThat(captured.path()).isEqualTo("/v1/classify");
		assertThat(captured.tenant()).isEqualTo(TENANT);
		assertThat(captured.requestId()).isEqualTo("req-classify-e2e-4999");
		assertThat(captured.idempotencyKey()).isNull();
		assertThat(captured.body()).isEqualTo(request);
	}

	@Test
	@DisplayName("Given Backend 실제 라벨 fixture When Adapter를 호출하면 Then AI Stub까지 헤더와 본문이 보존된다")
	void roundTripsLabelSuggestionOverRealHttp() throws Exception {
		String request = fixture("post_labels_suggest.request.json");

		var response = backendClient().post()
			.uri("/v1/labels/suggest")
			.header("X-Tenant-Id", TENANT)
			.header("X-Request-Id", "req-label-e2e-5000")
			.contentType(MediaType.APPLICATION_JSON)
			.body(request)
			.retrieve()
			.toEntity(String.class);
		CapturedRequest captured = CAPTURED.poll(2, TimeUnit.SECONDS);

		assertThat(response.getStatusCode().value()).isEqualTo(200);
		assertThat(response.getBody()).isEqualTo(fixture("post_labels_suggest.200.json"));
		assertThat(captured).isNotNull();
		assertThat(captured.path()).isEqualTo("/v1/labels/suggest");
		assertThat(captured.tenant()).isEqualTo(TENANT);
		assertThat(captured.requestId()).isEqualTo("req-label-e2e-5000");
		assertThat(captured.idempotencyKey()).isNull();
		assertThat(captured.body()).isEqualTo(request);
	}

	@Test
	@DisplayName("Given Backend 분류 확정 When Adapter를 호출하면 Then classify AI confirmations 경로로 전달된다")
	void roundTripsClassificationConfirmationOverRealHttp() throws Exception {
		String request = """
			{"kind":"classification","suggestion_id":"iq_204","action":"corrected",
			 "corrected_value":{"topic":"counsel_request"}}
			""";

		var response = backendClient().post()
			.uri("/v1/confirmations")
			.header("X-Tenant-Id", TENANT)
			.header("X-Request-Id", "req-confirm-classify-e2e-5001")
			.contentType(MediaType.APPLICATION_JSON)
			.body(request)
			.retrieve()
			.toEntity(String.class);
		CapturedRequest captured = CAPTURED.poll(2, TimeUnit.SECONDS);

		assertThat(response.getStatusCode().value()).isEqualTo(200);
		assertThat(captured).isNotNull();
		assertThat(captured.path()).isEqualTo("/v1/confirmations");
		assertThat(captured.requestId()).isEqualTo("req-confirm-classify-e2e-5001");
		assertThat(captured.body()).isEqualTo(request);
	}

	@Test
	@DisplayName("Given Backend 라벨 확정 When Adapter를 호출하면 Then 공용 AI confirmations 경로로 한 번 전달된다")
	void roundTripsLabelConfirmationOverRealHttp() throws Exception {
		String request = "{\"kind\":\"label\",\"suggestion_id\":\"gd_11b0:comm:data\",\"action\":\"confirmed\"}";

		var response = backendClient().post()
			.uri("/v1/confirmations")
			.header("X-Tenant-Id", TENANT)
			.header("X-Request-Id", "req-confirm-e2e-5001")
			.contentType(MediaType.APPLICATION_JSON)
			.body(request)
			.retrieve()
			.toEntity(String.class);
		CapturedRequest captured = CAPTURED.poll(2, TimeUnit.SECONDS);

		assertThat(response.getStatusCode().value()).isEqualTo(200);
		assertThat(response.getBody()).isEqualTo(fixture("confirmation-response-accepted.json"));
		assertThat(captured).isNotNull();
		assertThat(captured.path()).isEqualTo("/v1/confirmations");
		assertThat(captured.tenant()).isEqualTo(TENANT);
		assertThat(captured.requestId()).isEqualTo("req-confirm-e2e-5001");
		assertThat(captured.idempotencyKey()).isNull();
		assertThat(captured.body()).isEqualTo(request);
	}

	private RestClient backendClient() {
		return RestClient.builder()
			.baseUrl("http://127.0.0.1:" + adapterPort)
			.build();
	}

	private static HttpServer startAiStub() {
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
			server.createContext("/", CounselSynchronousProxyStubE2eTest::handleAiRequest);
			server.start();
			return server;
		}
		catch (IOException exception) {
			throw new IllegalStateException("AI Stub을 시작할 수 없습니다", exception);
		}
	}

	private static void handleAiRequest(HttpExchange exchange) throws IOException {
		String path = exchange.getRequestURI().getPath();
		String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
		CAPTURED.add(new CapturedRequest(
			path,
			exchange.getRequestHeaders().getFirst("X-Tenant-Id"),
			exchange.getRequestHeaders().getFirst("X-Request-Id"),
			exchange.getRequestHeaders().getFirst("Idempotency-Key"),
			body
		));
		String fixture = switch (path) {
			case "/v1/classify" -> null;
			case "/v1/labels/suggest" -> "post_labels_suggest.200.json";
			case "/v1/confirmations" -> "confirmation-response-accepted.json";
			default -> throw new IllegalArgumentException("unexpected AI path: " + path);
		};
		String responseBody = fixture == null
			? "{\"data\":{\"classified\":true},\"error\":null,\"meta\":null}"
			: fixture(fixture);
		byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
		exchange.sendResponseHeaders(200, response.length);
		exchange.getResponseBody().write(response);
		exchange.close();
	}

	private static String fixture(String name) throws IOException {
		String path = "contracts/labels/" + name;
		try (var input = CounselSynchronousProxyStubE2eTest.class.getClassLoader().getResourceAsStream(path)) {
			if (input == null) {
				throw new IOException("fixture not found: " + path);
			}
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private record CapturedRequest(
		String path,
		String tenant,
		String requestId,
		String idempotencyKey,
		String body
	) {
	}
}
