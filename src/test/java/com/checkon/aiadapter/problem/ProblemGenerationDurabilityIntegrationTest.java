package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.checkon.aiadapter.problem.ai.AiProblemClient;
import com.checkon.aiadapter.problem.ai.AiProblemClientException;
import com.checkon.aiadapter.problem.ai.ProblemSubmissionResponse;
import com.checkon.aiadapter.problem.ai.ProblemJobResponse;
import com.checkon.aiadapter.problem.ai.ProblemItemSetResponse;
import com.checkon.aiadapter.problem.ai.ProblemItemDetailResponse;
import com.checkon.aiadapter.problem.application.ProblemGenerationExecutionWorker;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationRequestDecoder;

import tools.jackson.databind.ObjectMapper;

@Testcontainers
@SpringBootTest(properties = {
	"checkon.ai.problem-generation.worker-enabled=true",
	"checkon.ai.problem-generation.poll-delay=1h",
	"checkon.ai.problem-generation.scheduler-enabled=false",
	"checkon.ai.problem-generation.poll-interval=1ms",
	"checkon.ai.problem-generation.base-url=http://localhost:1",
	"checkon.kafka.problem-generation.enabled=true",
	"checkon.kafka.problem-generation.outbox-poll-delay=1h",
	"spring.kafka.listener.auto-startup=false"
})
class ProblemGenerationDurabilityIntegrationTest {
	private static final String TENANT = "tn_0123456789abcdef0123456789abcdef";
	private static final UUID EVENT = UUID.fromString("0198-0000-7000-8000-000000000001".replace("0198-", "01980000-"));
	private static final UUID REQUEST = UUID.fromString("01980000-0000-7000-8000-000000000002");
	private static final UUID EXECUTION = UUID.fromString("01980000-0000-7000-8000-000000000003");

	@Container
	static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.4");

	@DynamicPropertySource
	static void database(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		registry.add("spring.datasource.username", POSTGRES::getUsername);
		registry.add("spring.datasource.password", POSTGRES::getPassword);
		registry.add("spring.flyway.url", POSTGRES::getJdbcUrl);
		registry.add("spring.flyway.user", POSTGRES::getUsername);
		registry.add("spring.flyway.password", POSTGRES::getPassword);
	}

	@Autowired ProblemGenerationStore store;
	@Autowired ProblemGenerationRequestDecoder decoder;
	@Autowired ProblemGenerationExecutionWorker worker;
	@Autowired JdbcTemplate jdbc;
	@Autowired ObjectMapper objectMapper;
	@MockitoBean AiProblemClient client;

	@BeforeEach
	void clean() {
		jdbc.update("DELETE FROM outbox_publish_attempt WHERE worker_kind='problem_generation'");
		jdbc.update("DELETE FROM problem_generation_outbox");
		jdbc.update("DELETE FROM problem_generation_attempt");
		jdbc.update("DELETE FROM problem_generation_request_inbox");
	}

	@Test
	@DisplayName("Given 동일 child 이벤트 When 두 번 등록하면 Then durable inbox 한 행으로 멱등 처리한다")
	void deduplicatesRequestInDurableInbox() {
		// Given
		var event = decoder.decode(TENANT, requestEvent());
		Instant now = Instant.now();

		// When
		var first = store.register(event, UUID.randomUUID(), requestEvent(), now);
		var duplicate = store.register(event, UUID.randomUUID(), requestEvent(), now.plusSeconds(1));

		// Then
		assertThat(first).isEqualTo(ProblemGenerationStore.Registration.NEW);
		assertThat(duplicate).isEqualTo(ProblemGenerationStore.Registration.DUPLICATE);
		assertThat(count("problem_generation_request_inbox")).isEqualTo(1);
	}

	@Test
	@DisplayName("Given POST 정본 실행 ID When 이후 GET 값이 달라져도 Then POST 값을 결과에 보존한다")
	void preservesSubmittedExecutionIdAcrossPolling() throws Exception {
		// Given
		var event = decoder.decode(TENANT, requestEvent());
		store.register(event, UUID.fromString("01980000-0000-7000-8000-000000000004"), requestEvent(), Instant.now());
		when(client.submit(any(), any())).thenReturn(read("""
			{"data":{"job_id":"job-48","status":"queued"},"error":null,"meta":{"execution_id":"ai-exec-48"}}
			""",ProblemSubmissionResponse.class));

		// When: submit identifiers are persisted before a later process instance claims POLL.
		assertThat(worker.processOne()).isTrue();
		assertThat(inboxStatus()).isEqualTo("WAITING");
		assertThat(jdbc.queryForObject(
			"SELECT ai_execution_id FROM problem_generation_request_inbox WHERE event_id=?",
			String.class, EVENT)).isEqualTo("ai-exec-48");
		jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '1 second'");
		when(client.job(any(), any())).thenReturn(new AiProblemClient.JobResponse(read("""
			{"data":{"job_id":"job-48","status":"succeeded","result":{"set_id":"set-48"}},"error":null,"meta":{"execution_id":"wrong-job-exec"}}
			""",ProblemJobResponse.class),null));
		when(client.items(any(), any())).thenReturn(read("""
			{"data":{"set_id":"set-48","status_counts":{"needs_review":1},"items":[
			 {"slot_index":0,"item_id":"item-48","status":"needs_review","current_revision_no":0}
			]},"error":null,"meta":{"execution_id":"wrong-items-meta-exec","versions":{"contract":"0.1"}}}
			""",ProblemItemSetResponse.class));
		when(client.item(any(), anyInt(), any())).thenReturn(read("""
			{"data":{"set_id":"set-48","slot_index":0,"item_id":"item-48","status":"needs_review",
			 "item":{"stem":"문제 본문","choices":[{"no":1,"text":"정답"},{"no":2,"text":"오답"}],"answer":{"correct_no":1},"rationale":"근거"}}}
			""",ProblemItemDetailResponse.class));
		assertThat(worker.processOne()).isTrue();

		// Then
		assertThat(inboxStatus()).isEqualTo("OUTCOME_PENDING");
		String payload = jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox",
			String.class);
		assertThat(payload)
			.contains("\"problem_execution_id\": \"" + EXECUTION + "\"")
			.contains("\"job_id\": \"job-48\"")
			.contains("\"execution_id\": \"ai-exec-48\"")
			.doesNotContain("wrong-job-exec", "wrong-items-exec", "wrong-items-meta-exec")
			.contains("\"set_id\": \"set-48\"")
			.contains("\"stem\": \"문제 본문\"")
			.contains("\"correct_no\": 1");
	}

	@Test
	@DisplayName("Given AI 재시작으로 기존 결과가 유실됨 When polling하면 Then 재제출 없이 명시 사유로 실패한다")
	void failsWithoutResubmittingWhenResultWasLostAfterAiRestart() throws Exception {
		// Given
		var event = decoder.decode(TENANT, requestEvent());
		store.register(event, UUID.randomUUID(), requestEvent(), Instant.now());
		when(client.submit(any(), any())).thenReturn(read("""
			{"data":{"job_id":"lost-job","status":"queued"},"error":null,
			 "meta":{"execution_id":"canonical-exec"}}
			""",ProblemSubmissionResponse.class));
		assertThat(worker.processOne()).isTrue();
		jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '1 second'");
		when(client.job(any(), any())).thenThrow(new AiProblemClientException(
			"result_unavailable_after_restart", false, null));

		// When
		assertThat(worker.processOne()).isTrue();

		// Then
		String payload = jdbc.queryForObject(
			"SELECT event_payload::text FROM problem_generation_outbox", String.class);
		assertThat(payload)
			.contains("\"event_type\": \"worker_job.failed\"")
			.contains("\"job_id\": \"lost-job\"")
			.contains("\"execution_id\": \"canonical-exec\"")
			.contains("\"error_code\": \"result_unavailable_after_restart\"");
		verify(client, times(1)).submit(any(), any());
	}

	@Test
	@DisplayName("Given Adapter 접수 후 21분이 지났을 때 When worker가 claim하면 Then AI를 호출하지 않고 timed_out으로 종결한다")
	void failsClosedAtAdapterTimeLimit() throws Exception {
		// Given
		var event=decoder.decode(TENANT,requestEvent());
		store.register(event,UUID.randomUUID(),requestEvent(),Instant.now().minus(Duration.ofMinutes(22)));

		// When
		assertThat(worker.processOne()).isTrue();

		// Then
		String payload=jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox",String.class);
		assertThat(payload).contains("\"child_status\": \"timed_out\"","\"error_code\": \"ADAPTER_TIME_LIMIT_EXCEEDED\"");
		verify(client,org.mockito.Mockito.never()).submit(any(),any());
	}

	@Test
	@DisplayName("Given 정규화 결과가 3 MiB를 넘을 때 When 성공 결과를 만들면 Then result_ref 없이 RESULT_TOO_LARGE로 실패한다")
	void rejectsOversizedNormalizedResult() throws Exception {
		// Given
		var event=decoder.decode(TENANT,requestEvent()); store.register(event,UUID.randomUUID(),requestEvent(),Instant.now());
		when(client.submit(any(),any())).thenReturn(read("{\"data\":{\"job_id\":\"large-job\"},\"meta\":{\"execution_id\":\"large-exec\"}}",ProblemSubmissionResponse.class));
		assertThat(worker.processOne()).isTrue(); jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '1 second'");
		when(client.job(any(),any())).thenReturn(new AiProblemClient.JobResponse(read(
			"{\"data\":{\"status\":\"succeeded\",\"result\":{\"set_id\":\"large-set\"}}}",ProblemJobResponse.class),null));
		when(client.items(any(),any())).thenReturn(read("{\"data\":{\"set_id\":\"large-set\",\"items\":[{\"slot_index\":0,\"item_id\":\"large-item\",\"status\":\"verified\",\"current_revision_no\":0}]}}",ProblemItemSetResponse.class));
		String huge="가".repeat(1_100_000);
		when(client.item(any(),anyInt(),any())).thenReturn(read("{\"data\":{\"set_id\":\"large-set\",\"slot_index\":0,\"item_id\":\"large-item\",\"item\":{\"stem\":\""+huge+"\",\"choices\":[{\"no\":1,\"text\":\"정답\"},{\"no\":2,\"text\":\"오답\"}],\"answer\":{\"correct_no\":1}}}}",ProblemItemDetailResponse.class));

		// When
		assertThat(worker.processOne()).isTrue();

		// Then
		String payload=jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox",String.class);
		assertThat(payload).contains("\"error_code\": \"RESULT_TOO_LARGE\"").doesNotContain("large-item",huge.substring(0,100));
	}

	@Test
	@DisplayName("Given queued 응답에 Retry-After가 있을 때 When polling하면 Then 상태는 유지하고 다음 poll 시각에 advisory를 반영한다")
	void honorsRetryAfterAsPollingAdvice() throws Exception {
		// Given
		var event=decoder.decode(TENANT,requestEvent()); store.register(event,UUID.randomUUID(),requestEvent(),Instant.now());
		when(client.submit(any(),any())).thenReturn(read("{\"data\":{\"job_id\":\"wait-job\"},\"meta\":{\"execution_id\":\"wait-exec\"}}",ProblemSubmissionResponse.class));
		assertThat(worker.processOne()).isTrue(); jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '1 second'");
		Instant before=Instant.now();
		when(client.job(any(),any())).thenReturn(new AiProblemClient.JobResponse(read("{\"data\":{\"status\":\"queued\"}}",ProblemJobResponse.class),Duration.ofSeconds(10)));

		// When
		assertThat(worker.processOne()).isTrue();

		// Then
		Instant next=jdbc.queryForObject("SELECT next_attempt_at FROM problem_generation_request_inbox",java.sql.Timestamp.class).toInstant();
		assertThat(inboxStatus()).isEqualTo("WAITING");
		assertThat(next).isAfterOrEqualTo(before.plusSeconds(9));
	}

	@Test
	@DisplayName("Given 처리 중 프로세스 종료 When lock timeout이 지나면 Then 같은 phase와 실행 ID로 재claim한다")
	void reclaimsStaleProcessingState() {
		// Given
		var event = decoder.decode(TENANT, requestEvent());
		UUID adapterExecution = UUID.randomUUID();
		Instant now = Instant.parse("2026-08-13T00:00:00Z");
		store.register(event, adapterExecution, requestEvent(), now);
		var first = store.claimNext(now, Duration.ofSeconds(30)).orElseThrow();

		// When
		var recovered = store.claimNext(now.plusSeconds(31), Duration.ofSeconds(30)).orElseThrow();

		// Then
		assertThat(recovered.adapterExecutionId()).isEqualTo(first.adapterExecutionId());
		assertThat(recovered.phase()).isEqualTo("SUBMIT");
		assertThat(recovered.httpAttempt()).isEqualTo(2);
	}

	@Test
	@DisplayName("Given AI 네트워크 일시 장애 When worker가 처리하면 Then 결과 실패 대신 재시도 시각을 영속화한다")
	void persistsTransientAiRetry() {
		// Given
		var event = decoder.decode(TENANT, requestEvent());
		store.register(event, UUID.randomUUID(), requestEvent(), Instant.now());
		when(client.submit(any(), any())).thenThrow(
			new AiProblemClientException("AI_NETWORK_ERROR", true, new RuntimeException("offline")));

		// When
		assertThat(worker.processOne()).isTrue();

		// Then
		assertThat(inboxStatus()).isEqualTo("RETRY_PENDING");
		assertThat(jdbc.queryForObject("SELECT last_error_code FROM problem_generation_request_inbox WHERE event_id=?",
			String.class, EVENT)).isEqualTo("AI_NETWORK_ERROR");
		assertThat(count("problem_generation_outbox")).isZero();
	}

	private String inboxStatus() {
		return jdbc.queryForObject("SELECT status FROM problem_generation_request_inbox WHERE event_id=?",
			String.class, EVENT);
	}

	@Test
	@DisplayName("Given 문제 출제 stale reclaim When 이전 claim이 늦게 재시도·완료하면 Then 최신 claim을 변경하지 않는다")
	void fencesSupersededProblemClaim() {
		// Given
		var event=decoder.decode(TENANT,requestEvent());Instant now=Instant.parse("2026-08-13T00:00:00Z");
		store.register(event,UUID.randomUUID(),requestEvent(),now);
		var old=store.claimNext(now,Duration.ofSeconds(30)).orElseThrow();
		var current=store.claimNext(now.plusSeconds(31),Duration.ofSeconds(30)).orElseThrow();
		// When/Then
		org.assertj.core.api.Assertions.assertThatThrownBy(()->store.markRetry(old.eventId(),old.claimVersion(),now.plusSeconds(60),"LATE",now.plusSeconds(32)))
			.isInstanceOf(IllegalStateException.class);
		org.assertj.core.api.Assertions.assertThatThrownBy(()->store.saveOutcome(old.eventId(),old.claimVersion(),UUID.randomUUID(),"topic",TENANT,"{}",now.plusSeconds(32)))
			.isInstanceOf(IllegalStateException.class);
		assertThat(current.claimVersion()).isGreaterThan(old.claimVersion());
		assertThat(inboxStatus()).isEqualTo("PROCESSING");
		assertThat(count("problem_generation_outbox")).isZero();
	}

	@Test
	@DisplayName("Given 문제 출제 Outbox stale reclaim When 이전 publisher가 늦게 ack하면 Then 최신 claim을 유지한다")
	void fencesSupersededProblemOutboxClaim() {
		// Given
		var event=decoder.decode(TENANT,requestEvent());Instant now=Instant.parse("2026-08-13T00:00:00Z");
		store.register(event,UUID.randomUUID(),requestEvent(),now);
		var request=store.claimNext(now,Duration.ofSeconds(30)).orElseThrow();
		store.saveOutcome(request.eventId(),request.claimVersion(),UUID.randomUUID(),"topic",TENANT,"{}",now);
		var old=store.claimOutbox(now,Duration.ofSeconds(30)).orElseThrow();
		var current=store.claimOutbox(now.plusSeconds(31),Duration.ofSeconds(30)).orElseThrow();
		// When/Then
		org.assertj.core.api.Assertions.assertThatThrownBy(()->store.outboxPublished(old.eventId(),old.sourceEventId(),old.claimVersion(),now.plusSeconds(32)))
			.isInstanceOf(IllegalStateException.class);
		assertThat(current.claimVersion()).isGreaterThan(old.claimVersion());
		assertThat(jdbc.queryForObject("SELECT status FROM problem_generation_outbox",String.class)).isEqualTo("PUBLISHING");
	}

	private <T> T read(String json,Class<T> type) throws Exception { return objectMapper.readValue(json,type); }

	private int count(String table) {
		return jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class);
	}

	private String requestEvent() {
		return """
			{
			 "event_id":"%s","event_type":"problem_generation.requested","occurred_at":"2026-08-13T00:00:00Z",
			 "tenant_id":"%s","schema_version":"pg-child-request-1","correlation_id":"%s",
			 "payload":{"problem_request_id":"%s","problem_execution_id":"%s","target_index":0,
			  "idempotency_key":"issue-48-child-0","request":{"target_kind":"student","target_ref":"st_0123456789abcdef0123456789abcdef","target_source":"teacher_manual","manual_targets":["language.node.infer"],"taxonomy_version":"v1","area_tag":"language","type_tags":["infer"],"item_format":"mcq","count":1}}
			}
			""".formatted(EVENT, TENANT, REQUEST, REQUEST, EXECUTION);
	}
}
