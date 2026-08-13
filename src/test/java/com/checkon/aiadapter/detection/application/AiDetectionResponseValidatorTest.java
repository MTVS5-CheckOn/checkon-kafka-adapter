package com.checkon.aiadapter.detection.application;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.checkon.aiadapter.detection.ai.AiDetectionRequest;
import com.checkon.aiadapter.detection.ai.AiDetectionResponse;
import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class AiDetectionResponseValidatorTest {

	private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
	private final AiDetectionResponseValidator validator = new AiDetectionResponseValidator();
	private AiDetectionRequest request;

	@BeforeEach
	void setUp() throws Exception {
		try (InputStream input = getClass().getClassLoader().getResourceAsStream(
			"contracts/risk-detection-requested-v0.2.json")) {
			request = objectMapper.readValue(input, RiskDetectionRequestedEvent.class).payload();
		}
	}

	@Test
	@DisplayName("Given structured evidence role When validating Then the response is accepted")
	void acceptsStructuredEvidenceRole() {
		assertThatCode(() -> validator.validate(request, responseWithRole("trigger")))
			.doesNotThrowAnyException();
	}

	@Test
	@DisplayName("Given missing evidence role When validating Then the response is rejected")
	void rejectsMissingEvidenceRole() {
		assertThatThrownBy(() -> validator.validate(request, responseWithRole(null)))
			.isInstanceOf(InvalidAiDetectionResponseException.class)
			.hasMessageContaining("evidence.role");
	}

	private AiDetectionResponse responseWithRole(String role) {
		AiDetectionResponse.Evidence evidence = new AiDetectionResponse.Evidence(
			"assignment_week_summary", "assignment-week-1", "과제 근거", role,
			BigDecimal.ONE, 2, LocalDate.of(2026, 8, 10)
		);
		AiDetectionResponse.Signal signal = new AiDetectionResponse.Signal(
			"signal-1", "st_abcdef0123456789abcdef0123456789",
			"cl_0123456789abcdef0123456789abcdef", "R2", "submit_drop",
			"제출 저하", "consecutive_missing_weeks", BigDecimal.ONE, null, 2,
			1.0, 1, false, "new",
			new AiDetectionResponse.Brief("확인이 필요합니다", true, false),
			List.of(evidence)
		);
		return new AiDetectionResponse(
			new AiDetectionResponse.Data(
				List.of(signal), new AiDetectionResponse.Stats(1, 1, 0, 0, List.of())
			),
			null,
			new AiDetectionResponse.Meta("execution-1", Map.of("contract", "0.2"))
		);
	}
}
