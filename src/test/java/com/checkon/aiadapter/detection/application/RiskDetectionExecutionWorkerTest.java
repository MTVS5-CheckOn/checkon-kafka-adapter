package com.checkon.aiadapter.detection.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.checkon.aiadapter.detection.ai.AiDetectionRequestHeaders;
import com.checkon.aiadapter.detection.ai.AiDetectionResponse;
import com.checkon.aiadapter.detection.ai.AiRiskDetectionClient;
import com.checkon.aiadapter.detection.ai.AiRiskDetectionClientException;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionInboxRepository;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionInboxRepository.ClaimedRequest;
import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class RiskDetectionExecutionWorkerTest {

	private static final Instant NOW = Instant.parse("2026-08-12T00:00:00Z");

	private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
	private final RiskDetectionInboxRepository inboxRepository = mock(
		RiskDetectionInboxRepository.class);
	private final AiRiskDetectionClient aiClient = mock(AiRiskDetectionClient.class);
	private final RiskDetectionOutcomeCoordinator outcomeCoordinator = mock(
		RiskDetectionOutcomeCoordinator.class);
	private final RiskDetectionProcessingProperties properties =
		new RiskDetectionProcessingProperties(
			true, Duration.ofSeconds(1), Duration.ofSeconds(30),
			Duration.ofSeconds(1), 3);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

	private RiskDetectionExecutionWorker worker;
	private RiskDetectionRequestedEvent event;
	private String requestBody;

	@BeforeEach
	void setUp() throws Exception {
		worker = new RiskDetectionExecutionWorker(
			inboxRepository, aiClient, outcomeCoordinator, properties, clock);
		String rawEvent = readFixture();
		event = objectMapper.readValue(rawEvent, RiskDetectionRequestedEvent.class);
		requestBody = objectMapper.writeValueAsString(
			objectMapper.readTree(rawEvent).get("payload")
		);
	}

	@Test
	@DisplayName("Given 첫 번째 503 실패 When worker가 처리하면 Then 1초 뒤 재시도를 예약한다")
	void schedulesTransientRetry() {
		// Given
		when(inboxRepository.claimNext(NOW, properties.lockTimeout()))
			.thenReturn(Optional.of(new ClaimedRequest(event, requestBody, 1, 1, false)));
		when(aiClient.detectRaw(requestBody, headers()))
			.thenThrow(AiRiskDetectionClientException.httpError(503, null));

		// When
		boolean processed = worker.processOne();

		// Then
		assertThat(processed).isTrue();
		verify(outcomeCoordinator).retry(
			event.eventId(), 1, NOW.plusSeconds(1), "AI_HTTP_503");
		verify(outcomeCoordinator, never()).fail(
			org.mockito.ArgumentMatchers.any(),
			org.mockito.ArgumentMatchers.anyLong(),
			org.mockito.ArgumentMatchers.any(),
			org.mockito.ArgumentMatchers.any(),
			org.mockito.ArgumentMatchers.any());
	}

	@Test
	@DisplayName("Given 세 번째 503 실패 When worker가 처리하면 Then failed 결과를 만든다")
	void failsAfterRetryExhaustion() {
		// Given
		when(inboxRepository.claimNext(NOW, properties.lockTimeout()))
			.thenReturn(Optional.of(new ClaimedRequest(event, requestBody, 3, 1, false)));
		when(aiClient.detectRaw(requestBody, headers()))
			.thenThrow(AiRiskDetectionClientException.httpError(503, null));

		// When
		worker.processOne();

		// Then
		verify(outcomeCoordinator).fail(
			event, 1, "AI_HTTP_503",
			"AI service remained unavailable after adapter retries", null);
		verify(outcomeCoordinator, never()).retry(
			org.mockito.ArgumentMatchers.any(),
			org.mockito.ArgumentMatchers.anyLong(),
			org.mockito.ArgumentMatchers.any(),
			org.mockito.ArgumentMatchers.any());
	}

	@Test
	@DisplayName("Given 민감한 요청 값이 포함된 AI 실패 When 로그를 남기면 Then 코드와 상태 외 식별자·payload는 노출하지 않는다")
	void doesNotExposeSensitiveRequestValuesInFailureLog() {
		// Given
		Logger logger = (Logger) LoggerFactory.getLogger(RiskDetectionExecutionWorker.class);
		ListAppender<ILoggingEvent> appender = new ListAppender<>();
		appender.start();
		logger.addAppender(appender);
		when(inboxRepository.claimNext(NOW, properties.lockTimeout()))
			.thenReturn(Optional.of(new ClaimedRequest(event, requestBody, 3, 1, false)));
		when(aiClient.detectRaw(requestBody, headers()))
			.thenThrow(AiRiskDetectionClientException.httpError(503, "sensitive-response-body", null));

		// When
		worker.processOne();

		// Then
		String output = appender.list.stream().map(ILoggingEvent::getFormattedMessage)
			.collect(java.util.stream.Collectors.joining("\n"));
		logger.detachAppender(appender);
		assertThat(output).contains("errorCode=AI_HTTP_503", "httpStatus=503")
			.doesNotContain(event.eventId().toString(), event.tenantAlias(), event.requestId(),
				"sensitive-response-body", "students", "student_ref");
	}

	@Test
	@DisplayName("Given AI 응답 계약 오류 When worker가 처리하면 Then 재시도 없이 failed 결과를 만든다")
	void failsInvalidResponseWithoutRetry() {
		// Given
		AiDetectionResponse response = mock(AiDetectionResponse.class);
		when(inboxRepository.claimNext(NOW, properties.lockTimeout()))
			.thenReturn(Optional.of(new ClaimedRequest(event, requestBody, 1, 1, false)));
		when(aiClient.detectRaw(requestBody, headers())).thenReturn(response);
		doThrow(new InvalidAiDetectionResponseException("invalid"))
			.when(outcomeCoordinator).complete(event, 1, response);

		// When
		worker.processOne();

		// Then
		verify(outcomeCoordinator).fail(
			event, 1, "AI_RESPONSE_INVALID", "AI response contract is invalid", null);
	}

	private AiDetectionRequestHeaders headers() {
		return new AiDetectionRequestHeaders(
			event.tenantAlias(), event.requestId(), event.idempotencyKey());
	}

	private String readFixture() throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream(
			"contracts/risk-detection-requested-v0.2.json")) {
			if (input == null) {
				throw new IOException("fixture not found");
			}
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
