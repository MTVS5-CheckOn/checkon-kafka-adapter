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
import com.checkon.aiadapter.problem.ai.ProblemRevisionResponse;
import com.checkon.aiadapter.problem.application.ProblemGenerationExecutionWorker;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationRequestDecoder;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationRevisionRequestDecoder;

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
	@Autowired ProblemGenerationRevisionRequestDecoder revisionDecoder;
	@Autowired ProblemGenerationExecutionWorker worker;
	@Autowired JdbcTemplate jdbc;
	@Autowired ObjectMapper objectMapper;
	@MockitoBean AiProblemClient client;

	@BeforeEach
	void clean() {
		jdbc.update("DELETE FROM outbox_publish_attempt WHERE worker_kind='problem_generation'");
		jdbc.update("DELETE FROM problem_generation_outbox");
		jdbc.update("DELETE FROM problem_generation_revision_attempt");
		jdbc.update("DELETE FROM problem_generation_revision_inbox");
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
		jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '5 seconds'");
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
		String reference=jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox WHERE event_kind='terminal'",String.class);
		String slot=jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox WHERE event_kind='slot'",String.class);
		assertThat(reference)
			.contains("\"problem_execution_id\": \"" + EXECUTION + "\"")
			.contains("\"job_id\": \"job-48\"")
			.contains("\"execution_id\": \"ai-exec-48\"")
			.doesNotContain("wrong-job-exec", "wrong-items-exec", "wrong-items-meta-exec")
			.contains("\"set_id\": \"set-48\"").doesNotContain("문제 본문");
		assertThat(slot).contains("\"stem\": \"문제 본문\"","\"correct_no\": 1");
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
		jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '5 seconds'");
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
	@DisplayName("Given 오래 대기한 요청 When worker가 claim하면 Then 고정 시간 초과로 실패시키지 않고 같은 논리 요청을 계속한다")
	void keepsReconcilingPastTheFormerAdapterTimeLimit() throws Exception {
		// Given
		var event=decoder.decode(TENANT,requestEvent());
		store.register(event,UUID.randomUUID(),requestEvent(),Instant.now().minus(Duration.ofMinutes(22)));

		// When
		when(client.submit(any(),any())).thenReturn(read("{\"data\":{\"job_id\":\"old-job\",\"status\":\"queued\"},\"meta\":{\"execution_id\":\"old-exec\"}}",ProblemSubmissionResponse.class));
		assertThat(worker.processOne()).isTrue();

		// Then
		assertThat(inboxStatus()).isEqualTo("WAITING");
		assertThat(count("problem_generation_outbox")).isZero();
		verify(client,times(1)).submit(any(),any());
	}

	@Test
	@DisplayName("Given slot 상세 이벤트가 1 MiB를 넘을 때 When 결과를 만들면 Then 본문을 발행하지 않고 fail-closed한다")
	void rejectsOversizedNormalizedResult() throws Exception {
		// Given
		var event=decoder.decode(TENANT,requestEvent()); store.register(event,UUID.randomUUID(),requestEvent(),Instant.now());
		when(client.submit(any(),any())).thenReturn(read("{\"data\":{\"job_id\":\"large-job\",\"status\":\"queued\"},\"meta\":{\"execution_id\":\"large-exec\"}}",ProblemSubmissionResponse.class));
		assertThat(worker.processOne()).isTrue(); jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '5 seconds'");
		when(client.job(any(),any())).thenReturn(new AiProblemClient.JobResponse(read(
			"{\"data\":{\"status\":\"succeeded\",\"result\":{\"set_id\":\"large-set\"}}}",ProblemJobResponse.class),null));
		when(client.items(any(),any())).thenReturn(read("{\"data\":{\"set_id\":\"large-set\",\"items\":[{\"slot_index\":0,\"item_id\":\"large-item\",\"status\":\"verified\",\"current_revision_no\":0}]}}",ProblemItemSetResponse.class));
		String huge="가".repeat(1_100_000);
		when(client.item(any(),anyInt(),any())).thenReturn(read("{\"data\":{\"set_id\":\"large-set\",\"slot_index\":0,\"item_id\":\"large-item\",\"item\":{\"stem\":\""+huge+"\",\"choices\":[{\"no\":1,\"text\":\"정답\"},{\"no\":2,\"text\":\"오답\"}],\"answer\":{\"correct_no\":1}}}}",ProblemItemDetailResponse.class));

		// When
		assertThat(worker.processOne()).isTrue();

		// Then
		String payload=jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox",String.class);
		assertThat(payload).contains("\"error_code\": \"DETAIL_EVENT_TOO_LARGE\"").doesNotContain("large-item",huge.substring(0,100));
	}

	@Test
	@DisplayName("Given queued 응답에 Retry-After가 있을 때 When polling하면 Then 상태는 유지하고 다음 poll 시각에 advisory를 반영한다")
	void honorsRetryAfterAsPollingAdvice() throws Exception {
		// Given
		var event=decoder.decode(TENANT,requestEvent()); store.register(event,UUID.randomUUID(),requestEvent(),Instant.now());
		when(client.submit(any(),any())).thenReturn(read("{\"data\":{\"job_id\":\"wait-job\",\"status\":\"queued\"},\"meta\":{\"execution_id\":\"wait-exec\"}}",ProblemSubmissionResponse.class));
		assertThat(worker.processOne()).isTrue(); jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '5 seconds'");
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

	@Test
	@DisplayName("Given 수정 가능한 slot 요청 When AI 수정과 상세 재조회가 성공하면 Then revision 결과를 별도 Outbox에 저장한다")
	void storesRevisionResultAfterRefetchingSlotDetail() throws Exception {
		// Given
		prepareRevisionTarget(); String raw=revisionRequestEvent(0);
		store.registerRevision(revisionDecoder.decode(TENANT,raw),raw,Instant.now());
		when(client.revise(any(),anyInt(),any(),any())).thenReturn(read("""
			{"data":{"set_id":"set-48","slot_index":0,"current_revision_no":1},
			 "meta":{"execution_id":"revision-exec-1"}}
			""",ProblemRevisionResponse.class));
		when(client.item(any(),anyInt(),any())).thenReturn(read("""
			{"data":{"set_id":"set-48","slot_index":0,"item_id":"item-48","status":"verified",
			 "current_revision_no":1,"available_actions":["refine"],"revisions":[{"revision_no":1}],
			 "item":{"area_tag":"language","type_tag":"concept","skill_node_id":"language.node",
			 "stem":"수정된 문두","choices":[
			 {"no":1,"text":"정답","why_wrong":null,"misconception_tag":null},
			 {"no":2,"text":"오답2","why_wrong":"개념 혼동","misconception_tag":"concept_confusion"},
			 {"no":3,"text":"오답3","why_wrong":"대상 혼동","misconception_tag":"target_confusion"},
			 {"no":4,"text":"오답4","why_wrong":"범위 혼동","misconception_tag":"range_confusion"},
			 {"no":5,"text":"오답5","why_wrong":"조건 혼동","misconception_tag":"condition_confusion"}],
			 "answer":{"correct_no":1},"rationale":"수정 근거"}}}
			""",ProblemItemDetailResponse.class));

		// When
		assertThat(worker.processOne()).isTrue();

		// Then
		assertThat(jdbc.queryForObject("SELECT status FROM problem_generation_revision_inbox",String.class)).isEqualTo("OUTCOME_PENDING");
		String payload=jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox WHERE revision_source_event_id IS NOT NULL",String.class);
		assertThat(payload).contains("problem_generation.revision.succeeded","revision-exec-1","수정된 문두",
			"\"target_index\": 0","\"current_revision_no\": 1","concept_confusion");
		verify(client).revise(org.mockito.ArgumentMatchers.eq("set-48"),org.mockito.ArgumentMatchers.eq(0),
			org.mockito.ArgumentMatchers.contains("\"base_revision_no\""),any());
	}

	@Test
	@DisplayName("Given 오래된 revision 번호 When AI가 충돌을 반환하면 Then reason과 현재 번호를 Backend 결과에 보존한다")
	void preservesRevisionConflictDetails() {
		// Given
		prepareRevisionTarget(); String raw=revisionRequestEvent(0);
		store.registerRevision(revisionDecoder.decode(TENANT,raw),raw,Instant.now());
		when(client.revise(any(),anyInt(),any(),any())).thenThrow(
			new AiProblemClientException("REVISION_CONFLICT",false,"stale_base_revision",1,null));

		// When
		assertThat(worker.processOne()).isTrue();

		// Then
		String payload=jdbc.queryForObject("SELECT event_payload::text FROM problem_generation_outbox WHERE revision_source_event_id IS NOT NULL",String.class);
		assertThat(payload).contains("problem_generation.revision.failed","REVISION_CONFLICT",
			"stale_base_revision","\"target_index\": 0","\"current_revision_no\": 1");
		verify(client,org.mockito.Mockito.never()).item(any(),anyInt(),any());
	}

	private void prepareRevisionTarget(){var event=decoder.decode(TENANT,requestEvent());
		store.register(event,UUID.randomUUID(),requestEvent(),Instant.now());
		jdbc.update("UPDATE problem_generation_request_inbox SET status='OUTCOME_PUBLISHED',phase='POLL',ai_set_id='set-48',ai_job_id='job-48',ai_execution_id='exec-48'");}

	private String revisionRequestEvent(int baseRevisionNo){return """
		{"event_id":"01980000-0000-7000-8000-000000000031","event_type":"problem_generation.revision.requested",
		 "occurred_at":"2026-08-23T00:00:00Z","tenant_id":"%s","schema_version":"pg-revision-request-1",
		 "correlation_id":"%s","payload":{"request_id":"%s","problem_execution_id":"%s",
		 "revision_request_id":"01980000-0000-7000-8000-000000000032","set_id":"set-48","slot_index":0,
		 "base_revision_no":%d,"revision_kind":"ai_refine","instruction":"문두를 명확하게 수정",
		 "idempotency_key":"problem-revision:01980000-0000-7000-8000-000000000032"}}
		""".formatted(TENANT,REQUEST,REQUEST,EXECUTION,baseRevisionNo);}

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
			 "tenant_id":"%s","schema_version":"pg-child-request-2","correlation_id":"%s",
			 "payload":{"problem_request_id":"%s","problem_execution_id":"%s","target_index":0,
			  "idempotency_key":"issue-48-child-0","request":{"target_kind":"student","target_ref":"st_0123456789abcdef0123456789abcdef","target_source":"teacher_manual","manual_targets":["language.node.infer"],"taxonomy_version":"v1","area_tag":"language","type_tags":["infer"],"item_format":"mcq","count":1}}
			}
			""".formatted(EVENT, TENANT, REQUEST, REQUEST, EXECUTION);
	}
}
