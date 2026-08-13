package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
import com.checkon.aiadapter.problem.application.ProblemGenerationExecutionWorker;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationRequestDecoder;

import tools.jackson.databind.ObjectMapper;

@Testcontainers
@SpringBootTest(properties = {
	"checkon.ai.problem-generation.worker-enabled=true",
	"checkon.ai.problem-generation.poll-delay=1h",
	"checkon.ai.problem-generation.poll-interval=1ms",
	"checkon.ai.problem-generation.base-url=http://localhost:1",
	"checkon.kafka.problem-generation.enabled=false"
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
		jdbc.update("DELETE FROM problem_generation_outbox");
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
	@DisplayName("Given AI queued 응답 When 재시작 뒤 polling이 성공하면 Then 평탄화한 결과를 Outbox에 저장한다")
	void resumesPollingAndStoresProjectableOutcome() throws Exception {
		// Given
		var event = decoder.decode(TENANT, requestEvent());
		store.register(event, UUID.fromString("01980000-0000-7000-8000-000000000004"), requestEvent(), Instant.now());
		when(client.submit(any(), any())).thenReturn(objectMapper.readTree("""
			{"data":{"job_id":"job-48","status":"queued"},"error":null,"meta":{"execution_id":"ai-exec-48"}}
			"""));

		// When: first process submits, then a later process instance can claim the durable POLL phase.
		assertThat(worker.processOne()).isTrue();
		jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '1 second'");
		when(client.job(any(), any())).thenReturn(objectMapper.readTree("""
			{"data":{"job_id":"job-48","status":"succeeded"},"error":null,"meta":{"execution_id":"ai-exec-48"}}
			"""));
		when(client.items(any(), any())).thenReturn(objectMapper.readTree("""
			{"data":{"job_id":"job-48","execution_id":"ai-exec-48","set_id":"set-48","items":[
			 {"item_id":"item-48","status":"needs_review","item":{"stem":"문제 본문","choices":[{"no":1,"text":"정답"},{"no":2,"text":"오답"}],"answer":{"correct_no":1},"rationale":"근거"}}
			]},"error":null,"meta":{"execution_id":"ai-exec-48","versions":{"contract":"0.1"}}}
			"""));
		assertThat(worker.processOne()).isTrue();

		// Then
		assertThat(inboxStatus()).isEqualTo("OUTCOME_PENDING");
		String payload = jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox",
			String.class);
		assertThat(payload)
			.contains("\"problem_execution_id\": \"" + EXECUTION + "\"")
			.contains("\"job_id\": \"job-48\"")
			.contains("\"set_id\": \"set-48\"")
			.contains("\"stem\": \"문제 본문\"")
			.contains("\"correct_answer\": 1");
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

	private int count(String table) {
		return jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class);
	}

	private String requestEvent() {
		return """
			{
			 "event_id":"%s","event_type":"problem_generation.requested","occurred_at":"2026-08-13T00:00:00Z",
			 "tenant_id":"%s","schema_version":"pg-child-request-1","correlation_id":"%s",
			 "payload":{"problem_request_id":"%s","problem_execution_id":"%s","target_index":0,
			  "idempotency_key":"issue-48-child-0","request":{"target_kind":"student","target_ref":"st_issue48","target_source":"teacher_manual","taxonomy_version":"v1","area_tag":"language","type_tags":["infer"],"item_format":"mcq","count":1}}
			}
			""".formatted(EVENT, TENANT, REQUEST, REQUEST, EXECUTION);
	}
}
