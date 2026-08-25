package com.checkon.aiadapter.counsel.classification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@DisplayName("문의 분류 프록시 HTTP 경계")
class InquiryClassificationProxyControllerTest {

	private RecordingProxy proxy;
	private MockMvc mockMvc;

	@BeforeEach
	void setUp() {
		proxy = new RecordingProxy();
		mockMvc = MockMvcBuilders.standaloneSetup(new InquiryClassificationProxyController(proxy)).build();
	}

	@Nested
	@DisplayName("Given Backend가 POST /v1/classify를 호출할 때")
	class GivenBackendCallsClassify {

		@Test
		@DisplayName("When 필수 헤더와 본문이 유효하면 Then 분류 프록시에 한 번 전달한다")
		void delegatesAValidRequest() throws Exception {
			String payload = "{\"inquiry_ref\":\"iq_204\",\"body_text\":\"시간표가 궁금합니다\"}";

			mockMvc.perform(post("/v1/classify")
					.header("X-Tenant-Id", "tn_0123456789abcdef0123456789abcdef")
					.header("X-Request-Id", "req-2040")
					.contentType(APPLICATION_JSON)
					.content(payload))
				.andExpect(status().isOk())
				.andExpect(content().json("{\"data\":{\"classified\":true}}"));

			assertThat(proxy.calls).isEqualTo(1);
			assertThat(proxy.payload).isEqualTo(payload);
			assertThat(proxy.headers.tenantAlias()).isEqualTo("tn_0123456789abcdef0123456789abcdef");
			assertThat(proxy.headers.requestId()).isEqualTo("req-2040");
		}

		@Test
		@DisplayName("When 테넌트 alias가 유효하지 않으면 Then 400이고 AI를 호출하지 않는다")
		void rejectsAnInvalidTenantAlias() throws Exception {
			mockMvc.perform(post("/v1/classify")
					.header("X-Tenant-Id", "teacher-raw-id")
					.header("X-Request-Id", "req-2040")
					.contentType(APPLICATION_JSON)
					.content("{\"inquiry_ref\":\"iq_204\",\"body_text\":\"문의\"}"))
				.andExpect(status().isBadRequest())
				.andExpect(content().json("{\"error\":{\"code\":\"INVALID_REQUEST\"}}"));

			assertThat(proxy.calls).isZero();
		}
	}

	private static final class RecordingProxy implements InquiryClassificationProxy {
		private int calls;
		private String payload;
		private Headers headers;

		@Override
		public ResponseEntity<String> classify(String payload, Headers headers) {
			calls++;
			this.payload = payload;
			this.headers = headers;
			return ResponseEntity.ok("{\"data\":{\"classified\":true}}");
		}
	}
}
