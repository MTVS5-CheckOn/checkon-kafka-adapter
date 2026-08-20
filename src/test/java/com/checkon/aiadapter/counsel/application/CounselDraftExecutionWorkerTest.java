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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.checkon.aiadapter.counsel.ai.AiCounselDraftClient;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftClient.PollResponse;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftClientException;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftMeta;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftRequestHeaders;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftResponse;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository.ClaimedRequest;
import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@DisplayName("상담 초안 SUBMIT/POLL 실행 worker")
class CounselDraftExecutionWorkerTest {

	private static final Instant NOW = Instant.parse("2026-08-20T00:00:00Z");
	private static final Instant CREATED_AT = NOW.minusSeconds(5);

	private final ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
	private final CounselDraftInboxRepository inboxRepository = mock(CounselDraftInboxRepository.class);
	private final AiCounselDraftClient aiClient = mock(AiCounselDraftClient.class);
	private final CounselDraftOutcomeCoordinator outcomeCoordinator = mock(CounselDraftOutcomeCoordinator.class);
	private final CounselDraftProcessingProperties properties = new CounselDraftProcessingProperties(
		true, Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofMinutes(10), Duration.ofSeconds(1),
		Duration.ofMinutes(2), 3);
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

	@Nested
	@DisplayName("Given phase가 SUBMIT일 때")
	class GivenSubmitPhase {

		@Test
		@DisplayName("When POST가 이미 종단 status를 반환하면 Then 폴링 없이 바로 완료 처리한다")
		void completesImmediatelyWhenPostReturnsATerminalStatus() {
			var claimed = claimedRequest("SUBMIT", null, null, 1);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));
			AiCounselDraftResponse response = response("019846dc-7c00-7000-8000-0000000006a1", "succeeded", "ai-exec-1");
			when(aiClient.createDraftRaw(requestBody, headers())).thenReturn(response);

			boolean processed = worker.processOne();

			assertThat(processed).isTrue();
			verify(outcomeCoordinator).complete(event, 1, response, "ai-exec-1");
			verify(inboxRepository, never()).markSubmitted(any(), org.mockito.ArgumentMatchers.anyLong(), any(), any(), any(), any());
		}

		@Test
		@DisplayName("When POST가 queued를 반환하면 Then ai_job_id를 저장하고 POLL로 전이한다")
		void movesToPollWhenPostReturnsQueued() {
			var claimed = claimedRequest("SUBMIT", null, null, 1);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));
			AiCounselDraftResponse response = response("019846dc-7c00-7000-8000-0000000006a1", "queued", "ai-exec-1");
			when(aiClient.createDraftRaw(requestBody, headers())).thenReturn(response);

			worker.processOne();

			verify(inboxRepository).markSubmitted(
				event.eventId(), 1, "019846dc-7c00-7000-8000-0000000006a1", "ai-exec-1",
				NOW.plus(properties.pollInterval()), NOW);
			verify(outcomeCoordinator, never()).complete(any(), org.mockito.ArgumentMatchers.anyLong(), any(), any());
		}
	}

	@Nested
	@DisplayName("Given phase가 POLL일 때")
	class GivenPollPhase {

		@Test
		@DisplayName("When GET이 종단 status를 반환하면 Then SUBMIT 시점의 execution_id를 그대로 보존해 완료 처리한다")
		void preservesTheSubmitTimeExecutionIdOnCompletion() {
			var claimed = claimedRequest("POLL", "019846dc-7c00-7000-8000-0000000006a1", "submit-exec", 1);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));
			AiCounselDraftResponse response = response("019846dc-7c00-7000-8000-0000000006a1", "succeeded", "different-poll-exec");
			when(aiClient.getDraft("019846dc-7c00-7000-8000-0000000006a1", headers()))
				.thenReturn(new PollResponse(response, null));

			worker.processOne();

			verify(outcomeCoordinator).complete(event, 1, response, "submit-exec");
		}

		@Test
		@DisplayName("When GET이 Retry-After와 함께 비종단 status를 반환하면 Then 그 값을 그대로 다음 폴링 시각으로 쓴다")
		void honorsRetryAfterExactlyAsTheNextPollDelay() {
			var claimed = claimedRequest("POLL", "019846dc-7c00-7000-8000-0000000006a1", "submit-exec", 1);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));
			AiCounselDraftResponse response = response("019846dc-7c00-7000-8000-0000000006a1", "running", "submit-exec");
			when(aiClient.getDraft("019846dc-7c00-7000-8000-0000000006a1", headers()))
				.thenReturn(new PollResponse(response, Duration.ofSeconds(7)));

			worker.processOne();

			verify(inboxRepository).markWaiting(event.eventId(), 1, NOW.plusSeconds(7), NOW);
		}

		@Test
		@DisplayName("When GET에 Retry-After가 없으면 Then 어댑터 기본 poll interval로 재시도를 예약한다")
		void fallsBackToPollIntervalWhenRetryAfterIsMissing() {
			var claimed = claimedRequest("POLL", "019846dc-7c00-7000-8000-0000000006a1", "submit-exec", 1);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));
			AiCounselDraftResponse response = response("019846dc-7c00-7000-8000-0000000006a1", "queued", "submit-exec");
			when(aiClient.getDraft("019846dc-7c00-7000-8000-0000000006a1", headers()))
				.thenReturn(new PollResponse(response, null));

			worker.processOne();

			verify(inboxRepository).markWaiting(event.eventId(), 1, NOW.plus(properties.pollInterval()), NOW);
		}
	}

	@Nested
	@DisplayName("Given 어댑터 처리 시간 상한을 넘었을 때")
	class GivenTheAdapterTimeLimitIsExceeded {

		@Test
		@DisplayName("When worker가 claim하면 Then AI를 호출하지 않고 타임아웃으로 종결한다")
		void failsClosedWithoutCallingTheAiServer() {
			var claimed = new ClaimedRequest(event, requestBody, "POLL", "job-1", "exec-1", 1, 1,
				NOW.minus(properties.maxElapsed()).minusSeconds(1), false);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));

			worker.processOne();

			verify(outcomeCoordinator).fail(event, 1, "ADAPTER_TIME_LIMIT_EXCEEDED",
				"Counsel draft job did not reach a terminal status in time", null);
			org.mockito.Mockito.verifyNoInteractions(aiClient);
		}
	}

	@Nested
	@DisplayName("Given AI 호출이 실패할 때")
	class GivenTheAiCallFails {

		@Test
		@DisplayName("When 첫 번째 503 실패 Then 1초 뒤 재시도를 예약한다")
		void schedulesTransientRetry() {
			var claimed = claimedRequest("SUBMIT", null, null, 1);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));
			when(aiClient.createDraftRaw(requestBody, headers()))
				.thenThrow(AiCounselDraftClientException.httpError(503, null));

			boolean processed = worker.processOne();

			assertThat(processed).isTrue();
			verify(outcomeCoordinator).retry(event.eventId(), 1, NOW.plusSeconds(1), "AI_HTTP_503");
			verify(outcomeCoordinator, never()).fail(any(), org.mockito.ArgumentMatchers.anyLong(), any(), any(), any());
		}

		@Test
		@DisplayName("When 세 번째 503 실패 Then failed 결과를 만든다")
		void failsAfterRetryExhaustion() {
			var claimed = claimedRequest("SUBMIT", null, null, 3);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));
			when(aiClient.createDraftRaw(requestBody, headers()))
				.thenThrow(AiCounselDraftClientException.httpError(503, null));

			worker.processOne();

			verify(outcomeCoordinator).fail(
				event, 1, "AI_HTTP_503", "AI service remained unavailable after adapter retries", null);
			verify(outcomeCoordinator, never()).retry(any(), org.mockito.ArgumentMatchers.anyLong(), any(), any());
		}

		@Test
		@DisplayName("When AI 응답에 status가 없으면 Then 재시도 없이 계약 오류로 종결한다")
		void failsInvalidResponseWithoutRetry() {
			var claimed = claimedRequest("SUBMIT", null, null, 1);
			when(inboxRepository.claimNext(NOW, properties.lockTimeout())).thenReturn(Optional.of(claimed));
			AiCounselDraftResponse invalid = new AiCounselDraftResponse(
				new AiCounselDraftResponse.Data(null, null), null, null);
			when(aiClient.createDraftRaw(requestBody, headers())).thenReturn(invalid);

			worker.processOne();

			verify(outcomeCoordinator).fail(event, 1, "AI_RESPONSE_INVALID", "AI response contract is invalid", null);
		}
	}

	private ClaimedRequest claimedRequest(String phase, String aiJobId, String aiExecutionId, int httpAttempt) {
		return new ClaimedRequest(event, requestBody, phase, aiJobId, aiExecutionId, httpAttempt, 1, CREATED_AT, false);
	}

	private AiCounselDraftResponse response(String jobId, String status, String executionId) {
		return new AiCounselDraftResponse(
			new AiCounselDraftResponse.Data(jobId, status), null,
			new AiCounselDraftMeta(executionId, null));
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
