package com.checkon.aiadapter.counsel.synchronous;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import tools.jackson.databind.ObjectMapper;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

@DisplayName("라벨 제안과 확정 프록시 HTTP 경계")
class CounselSynchronousProxyControllerTest {

	private static final String TENANT = "tn_0123456789abcdef0123456789abcdef";

	private RecordingProxy proxy;
	private MockMvc mockMvc;

	@BeforeEach
	void setUp() {
		proxy = new RecordingProxy();
		mockMvc = MockMvcBuilders.standaloneSetup(
			new CounselSynchronousProxyController(proxy, new ObjectMapper())
		).build();
	}

	@Nested
	@DisplayName("Given Backend가 POST /v1/labels/suggest를 호출할 때")
	class GivenBackendCallsLabelSuggest {

		@Test
		@DisplayName("When 필수 헤더와 실제 본문이 유효하면 Then 라벨 제안 프록시에 한 번 전달한다")
		void delegatesAValidRequestExactlyOnce() throws Exception {
			String payload = readFixture("post_labels_suggest.request.json");

			mockMvc.perform(post("/v1/labels/suggest")
					.header("X-Tenant-Id", TENANT)
					.header("X-Request-Id", "req-label-2040")
					.contentType(APPLICATION_JSON)
					.content(payload))
				.andExpect(status().isOk())
				.andExpect(content().json("{\"data\":{\"suggestions\":[]}}"));

			assertThat(proxy.labelSuggestCalls).isEqualTo(1);
			assertThat(proxy.payload).isEqualTo(payload);
			assertThat(proxy.headers).isEqualTo(new CounselSynchronousProxy.Headers(TENANT, "req-label-2040"));
		}

		@Test
		@DisplayName("When 필수 헤더가 누락되면 Then 400이고 AI를 호출하지 않는다")
		void rejectsAMissingRequiredHeader() throws Exception {
			mockMvc.perform(post("/v1/labels/suggest")
					.header("X-Tenant-Id", TENANT)
					.contentType(APPLICATION_JSON)
					.content(readFixture("post_labels_suggest.request.json")))
				.andExpect(status().isBadRequest());

			assertThat(proxy.totalCalls()).isZero();
		}

		@Test
		@DisplayName("When 본문이 비어 있으면 Then 400이고 AI를 호출하지 않는다")
		void rejectsAnEmptyBody() throws Exception {
			mockMvc.perform(post("/v1/labels/suggest")
					.header("X-Tenant-Id", TENANT)
					.header("X-Request-Id", "req-label-empty")
					.contentType(APPLICATION_JSON)
					.content(""))
				.andExpect(status().isBadRequest());

			assertThat(proxy.totalCalls()).isZero();
		}

		@Test
		@DisplayName("When tenant alias가 유효하지 않으면 Then 400이고 민감한 본문을 로그에 남기지 않는다")
		void rejectsAnInvalidTenantWithoutLoggingPayload() throws Exception {
			String sensitiveMarker = "상담-본문-로그-금지-guardian-raw-id";
			Logger logger = (Logger) LoggerFactory.getLogger(CounselSynchronousProxyController.class);
			ListAppender<ILoggingEvent> appender = new ListAppender<>();
			appender.start();
			logger.addAppender(appender);
			try {
				mockMvc.perform(post("/v1/labels/suggest")
						.header("X-Tenant-Id", "teacher-raw-id")
						.header("X-Request-Id", "req-label-2041")
						.contentType(APPLICATION_JSON)
						.content("{\"history\":[{\"text\":\"" + sensitiveMarker + "\"}]}"))
					.andExpect(status().isBadRequest())
					.andExpect(content().json("{\"error\":{\"code\":\"INVALID_REQUEST\"}}"));
			}
			finally {
				logger.detachAppender(appender);
				appender.stop();
			}

			assertThat(proxy.totalCalls()).isZero();
			assertThat(appender.list).noneMatch(event -> event.getFormattedMessage().contains(sensitiveMarker));
		}
	}

	@Nested
	@DisplayName("Given Backend가 POST /v1/confirmations를 호출할 때")
	class GivenBackendCallsConfirmations {

		@Test
		@DisplayName("When 분류 확정 본문이 유효하면 Then 공용 확정 프록시에 한 번 전달한다")
		void delegatesAClassificationConfirmation() throws Exception {
			String payload = """
				{"kind":"classification","suggestion_id":"iq_204","action":"corrected",
				 "corrected_value":{"topic":"counsel_request"}}
				""";
			assertConfirmationDelegated(payload, "req-confirm-classification");
			assertThat(proxy.classificationConfirmationCalls).isEqualTo(1);
			assertThat(proxy.labelConfirmationCalls).isZero();
			assertThat(proxy.payload).isEqualTo(payload);
		}

		@Test
		@DisplayName("When 라벨 확정 본문이 유효하면 Then 같은 공용 확정 프록시에 한 번 전달한다")
		void delegatesALabelConfirmation() throws Exception {
			String payload = """
				{"kind":"label","suggestion_id":"gd_11b0:comm:data","action":"confirmed"}
				""";
			assertConfirmationDelegated(payload, "req-confirm-label");
			assertThat(proxy.classificationConfirmationCalls).isZero();
			assertThat(proxy.labelConfirmationCalls).isEqualTo(1);
			assertThat(proxy.payload).isEqualTo(payload);
		}

		@Test
		@DisplayName("When 필수 헤더가 누락되면 Then 400이고 AI를 호출하지 않는다")
		void rejectsAMissingRequiredHeader() throws Exception {
			mockMvc.perform(post("/v1/confirmations")
					.header("X-Request-Id", "req-confirm-missing")
					.contentType(APPLICATION_JSON)
					.content("{\"kind\":\"label\"}"))
				.andExpect(status().isBadRequest());

			assertThat(proxy.totalCalls()).isZero();
		}

		@Test
		@DisplayName("When tenant alias가 유효하지 않으면 Then 400이고 AI를 호출하지 않는다")
		void rejectsAnInvalidTenantAlias() throws Exception {
			mockMvc.perform(post("/v1/confirmations")
					.header("X-Tenant-Id", "teacher-raw-id")
					.header("X-Request-Id", "req-confirm-invalid")
					.contentType(APPLICATION_JSON)
					.content("{\"kind\":\"classification\"}"))
				.andExpect(status().isBadRequest())
				.andExpect(content().json("{\"error\":{\"code\":\"INVALID_REQUEST\"}}"));

			assertThat(proxy.totalCalls()).isZero();
		}

		@Test
		@DisplayName("When kind가 누락되거나 지원하지 않으면 Then 400이고 AI를 호출하지 않는다")
		void rejectsMissingAndUnsupportedKindsWithoutCallingAi() throws Exception {
			for (String payload : new String[] {"{\"suggestion_id\":\"iq_204\"}", "{\"kind\":\"draft\"}"}) {
				mockMvc.perform(post("/v1/confirmations")
						.header("X-Tenant-Id", TENANT)
						.header("X-Request-Id", "req-invalid-kind")
						.contentType(APPLICATION_JSON)
						.content(payload))
					.andExpect(status().isBadRequest())
					.andExpect(content().json("{\"error\":{\"code\":\"INVALID_REQUEST\"}}"));
			}

			assertThat(proxy.totalCalls()).isZero();
		}

		@Test
		@DisplayName("When JSON이 잘못되면 Then 400이고 AI를 호출하지 않는다")
		void rejectsMalformedJsonWithoutCallingAi() throws Exception {
			mockMvc.perform(post("/v1/confirmations")
					.header("X-Tenant-Id", TENANT)
					.header("X-Request-Id", "req-malformed-json")
					.contentType(APPLICATION_JSON)
					.content("{\"kind\":"))
				.andExpect(status().isBadRequest());

			assertThat(proxy.totalCalls()).isZero();
		}

		private void assertConfirmationDelegated(String payload, String requestId) throws Exception {
			mockMvc.perform(post("/v1/confirmations")
					.header("X-Tenant-Id", TENANT)
					.header("X-Request-Id", requestId)
					.contentType(APPLICATION_JSON)
					.content(payload))
				.andExpect(status().isOk())
				.andExpect(content().json("{\"data\":{\"accepted\":true}}"));

			assertThat(proxy.headers).isEqualTo(new CounselSynchronousProxy.Headers(TENANT, requestId));
		}
	}

	private String readFixture(String name) throws IOException {
		String path = "contracts/labels/" + name;
		try (var input = getClass().getClassLoader().getResourceAsStream(path)) {
			assertThat(input).as("fixture %s", path).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static final class RecordingProxy implements CounselSynchronousProxy {
		private int labelSuggestCalls;
		private int classificationConfirmationCalls;
		private int labelConfirmationCalls;
		private String payload;
		private Headers headers;

		@Override
		public ResponseEntity<String> suggestLabels(String payload, Headers headers) {
			labelSuggestCalls++;
			this.payload = payload;
			this.headers = headers;
			return ResponseEntity.ok("{\"data\":{\"suggestions\":[]}}");
		}

		@Override
		public ResponseEntity<String> confirmClassification(String payload, Headers headers) {
			classificationConfirmationCalls++;
			this.payload = payload;
			this.headers = headers;
			return ResponseEntity.ok("{\"data\":{\"accepted\":true}}");
		}

		@Override
		public ResponseEntity<String> confirmLabel(String payload, Headers headers) {
			labelConfirmationCalls++;
			this.payload = payload;
			this.headers = headers;
			return ResponseEntity.ok("{\"data\":{\"accepted\":true}}");
		}

		private int totalCalls() {
			return labelSuggestCalls + classificationConfirmationCalls + labelConfirmationCalls;
		}
	}
}
