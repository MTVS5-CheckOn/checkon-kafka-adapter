package com.checkon.aiadapter.counsel.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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

import com.checkon.aiadapter.counsel.ai.AiCounselDraftClient;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftClientException;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftRequestHeaders;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftResponse;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository.ClaimedRequest;
import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class CounselDraftExecutionWorkerTest {

	private static final Instant NOW = Instant.parse("2026-08-20T00:00:00Z");

	private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
	private final CounselDraftInboxRepository inboxRepository = mock(CounselDraftInboxRepository.class);
	private final AiCounselDraftClient aiClient = mock(AiCounselDraftClient.class);
	private final CounselDraftOutcomeCoordinator outcomeCoordinator = mock(CounselDraftOutcomeCoordinator.class);
	private final CounselDraftProcessingProperties properties =
		new CounselDraftProcessingProperties(true, Duration.ofSeconds(1), Duration.ofMinutes(10), Duration.ofSeconds(1), 3);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

	private CounselDraftExecutionWorker worker;
	private CounselDraftRequestedEvent event;
	private String requestBody;

	@BeforeEach
	void setUp() throws Exception {
		worker = new CounselDraftExecutionWorker(inboxRepository, aiClient, outcomeCoordinator, properties, clock);
		String rawEvent = readFixture();
		event = objectMapper.readValue(rawEvent, CounselDraftRequestedEvent.class);
		requestBody = objectMapper.writeValueAsString(objectMapper.readTree(rawEvent).get("payload"));
	}

	@Test
	@DisplayName("Given 첫 번째 503 실패 When worker가 처리하면 Then 1초 뒤 재시도를 예약한다")
	void schedulesTransientRetry() {
		when(inboxRepository.claimNext(NOW, properties.lockTimeout()))
			.thenReturn(Optional.of(new ClaimedRequest(event, requestBody, 1, 1, false)));
		when(aiClient.createDraftRaw(requestBody, headers()))
			.thenThrow(AiCounselDraftClientException.httpError(503, null));

		boolean processed = worker.processOne();

		assertThat(processed).isTrue();
		verify(outcomeCoordinator).retry(event.eventId(), 1, NOW.plusSeconds(1), "AI_HTTP_503");
		verify(outcomeCoordinator, never()).fail(any(), org.mockito.ArgumentMatchers.anyLong(), any(), any(), any());
	}

	@Test
	@DisplayName("Given 세 번째 503 실패 When worker가 처리하면 Then failed 결과를 만든다")
	void failsAfterRetryExhaustion() {
		when(inboxRepository.claimNext(NOW, properties.lockTimeout()))
			.thenReturn(Optional.of(new ClaimedRequest(event, requestBody, 3, 1, false)));
		when(aiClient.createDraftRaw(requestBody, headers()))
			.thenThrow(AiCounselDraftClientException.httpError(503, null));

		worker.processOne();

		verify(outcomeCoordinator).fail(
			event, 1, "AI_HTTP_503", "AI service remained unavailable after adapter retries", null);
		verify(outcomeCoordinator, never()).retry(any(), org.mockito.ArgumentMatchers.anyLong(), any(), any());
	}

	@Test
	@DisplayName("Given AI 응답 계약 오류 When worker가 처리하면 Then 재시도 없이 failed 결과를 만든다")
	void failsInvalidResponseWithoutRetry() {
		AiCounselDraftResponse response = mock(AiCounselDraftResponse.class);
		when(inboxRepository.claimNext(NOW, properties.lockTimeout()))
			.thenReturn(Optional.of(new ClaimedRequest(event, requestBody, 1, 1, false)));
		when(aiClient.createDraftRaw(requestBody, headers())).thenReturn(response);
		doThrow(new InvalidAiCounselDraftResponseException("invalid"))
			.when(outcomeCoordinator).complete(event, 1, response);

		worker.processOne();

		verify(outcomeCoordinator).fail(event, 1, "AI_RESPONSE_INVALID", "AI response contract is invalid", null);
	}

	@Test
	@DisplayName("Given 클레임할 잡이 없을 때 When worker가 처리하면 Then AI를 호출하지 않는다")
	void doesNothingWhenNoRequestIsClaimed() {
		when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.empty());

		boolean processed = worker.processOne();

		assertThat(processed).isFalse();
		org.mockito.Mockito.verifyNoInteractions(aiClient, outcomeCoordinator);
	}

	private AiCounselDraftRequestHeaders headers() {
		return new AiCounselDraftRequestHeaders(event.tenantAlias(), event.requestId(), event.idempotencyKey());
	}

	private String readFixture() throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream("contracts/counsel-draft-requested-v1.json")) {
			if (input == null) throw new IOException("fixture not found");
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
