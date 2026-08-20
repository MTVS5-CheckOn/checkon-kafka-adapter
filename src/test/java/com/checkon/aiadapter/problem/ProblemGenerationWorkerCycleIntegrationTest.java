package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.checkon.aiadapter.problem.ai.AiProblemClient;
import com.checkon.aiadapter.problem.ai.ProblemSubmissionResponse;
import com.checkon.aiadapter.problem.ai.ProblemJobResponse;
import com.checkon.aiadapter.problem.ai.ProblemItemSetResponse;
import com.checkon.aiadapter.problem.ai.ProblemItemDetailResponse;
import com.checkon.aiadapter.problem.application.ProblemGenerationExecutionWorker;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationOutboxPublisher;

import tools.jackson.databind.ObjectMapper;

@Testcontainers
@SpringBootTest(properties = {
	"checkon.kafka.problem-generation.enabled=true",
	"checkon.ai.problem-generation.worker-enabled=true",
	"checkon.ai.problem-generation.base-url=http://localhost:1",
	"checkon.ai.problem-generation.poll-delay=1h",
	"checkon.ai.problem-generation.scheduler-enabled=false",
	"checkon.ai.problem-generation.poll-interval=1ms",
	"checkon.kafka.problem-generation.outbox-poll-delay=1h",
	"spring.kafka.consumer.auto-offset-reset=earliest"
})
@Import(ProblemGenerationWorkerCycleIntegrationTest.Topics.class)
class ProblemGenerationWorkerCycleIntegrationTest {
	private static final String REQUEST_TOPIC = "checkon.ai.problem-generation.requests.v1";
	private static final String RESULT_TOPIC = "checkon.ai.problem-generation.results.v1";
	private static final String TENANT = "tn_0123456789abcdef0123456789abcdef";

	@Container
	@ServiceConnection
	static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2");

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

	@Autowired KafkaTemplate<String, String> kafka;
	@Autowired JdbcTemplate jdbc;
	@Autowired ObjectMapper objectMapper;
	@Autowired ProblemGenerationExecutionWorker worker;
	@Autowired ProblemGenerationOutboxPublisher outbox;
	@MockitoBean AiProblemClient client;

	@BeforeEach
	void clean() {
		jdbc.update("DELETE FROM outbox_publish_attempt WHERE worker_kind='problem_generation'");
		jdbc.update("DELETE FROM problem_generation_outbox");
		jdbc.update("DELETE FROM problem_generation_attempt");
		jdbc.update("DELETE FROM problem_generation_request_inbox");
	}

	@Test
	@DisplayName("Given Backend child 이벤트 When worker 한 사이클을 실행하면 Then 정본 ID와 문항을 Kafka 결과로 발행한다")
	void completesOneKafkaHttpKafkaCycle() throws Exception {
		// Given
		when(client.submit(any(), any())).thenReturn(objectMapper.readValue("""
			{"data":{"job_id":"cycle-job","status":"queued"},"error":null,
			 "meta":{"execution_id":"cycle-post-execution"}}
			""",ProblemSubmissionResponse.class));
		when(client.job(any(), any())).thenReturn(new AiProblemClient.JobResponse(objectMapper.readValue("""
			{"data":{"job_id":"cycle-job","status":"succeeded","result":{"set_id":"cycle-set"}},"error":null,
			 "meta":{"execution_id":"unstable-get-execution"}}
			""",ProblemJobResponse.class),Duration.ofSeconds(1)));
		when(client.items(any(), any())).thenReturn(objectMapper.readValue("""
			{"data":{"set_id":"cycle-set","status_counts":{"needs_review":1,"dropped":1},"items":[{"slot_index":0,"item_id":"cycle-item",
			 "status":"needs_review","current_revision_no":0},{"slot_index":1,"item_id":null,"status":"dropped","current_revision_no":0,"failure_reason":"generation_exhausted"}]},"error":null,
			 "meta":{"execution_id":"another-unstable-get-execution","versions":{"contract":"0.1"}}}
			""",ProblemItemSetResponse.class));
		when(client.item(any(),anyInt(),any())).thenReturn(objectMapper.readValue("""
			{"data":{"set_id":"cycle-set","slot_index":0,"item_id":"cycle-item","status":"needs_review",
			 "item":{"stem":"사이클 문제","choices":[{"no":1,"text":"정답"},{"no":2,"text":"오답"}],
			 "answer":{"correct_no":1},"rationale":"사이클 근거"}}}
			""",ProblemItemDetailResponse.class));

		try (KafkaConsumer<String, String> consumer = resultConsumer()) {
			consumer.subscribe(List.of(RESULT_TOPIC));

			// When
			kafka.send(REQUEST_TOPIC, TENANT, requestEvent()).get(10, TimeUnit.SECONDS);
			awaitInbox(Duration.ofSeconds(15));
			assertThat(worker.processOne()).isTrue();
			jdbc.update("UPDATE problem_generation_request_inbox SET next_attempt_at=now()-interval '5 seconds'");
			assertThat(worker.processOne()).isTrue();
			assertThat(outbox.publishOne()).isTrue();
			ConsumerRecord<String, String> result = pollOne(consumer, Duration.ofSeconds(15));

			// Then
			assertThat(result).isNotNull();
			assertThat(result.key()).isEqualTo(TENANT);
			assertThat(result.value())
				.contains("worker_job.succeeded", "cycle-job", "cycle-post-execution",
					"cycle-set", "cycle-item", "사이클 문제","generation_exhausted","\"item\": null")
				.doesNotContain("unstable-get-execution", "another-unstable-get-execution");
			assertThat(jdbc.queryForObject(
				"SELECT status FROM problem_generation_request_inbox", String.class))
				.isEqualTo("OUTCOME_PUBLISHED");
		}
	}

	private void awaitInbox(Duration timeout) throws InterruptedException {
		Instant deadline = Instant.now().plus(timeout);
		while (Instant.now().isBefore(deadline)) {
			Integer count = jdbc.queryForObject(
				"SELECT count(*) FROM problem_generation_request_inbox", Integer.class);
			if (count != null && count == 1) return;
			Thread.sleep(50);
		}
		throw new AssertionError("Kafka request was not stored in the Inbox");
	}

	private KafkaConsumer<String, String> resultConsumer() {
		return new KafkaConsumer<>(Map.of(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
			ConsumerConfig.GROUP_ID_CONFIG, "problem-cycle-" + UUID.randomUUID(),
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
	}

	private ConsumerRecord<String, String> pollOne(KafkaConsumer<String, String> consumer, Duration timeout) {
		Instant deadline = Instant.now().plus(timeout);
		while (Instant.now().isBefore(deadline)) {
			for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) return record;
		}
		return null;
	}

	private String requestEvent() {
		return """
			{"event_id":"01980000-0000-7000-8000-000000000021","event_type":"problem_generation.requested",
			 "occurred_at":"2026-08-13T00:00:00Z","tenant_id":"%s","schema_version":"pg-child-request-1",
			 "correlation_id":"01980000-0000-7000-8000-000000000022","payload":{
			 "problem_request_id":"01980000-0000-7000-8000-000000000022",
			 "problem_execution_id":"01980000-0000-7000-8000-000000000023","target_index":0,
			 "idempotency_key":"cycle-child-0","request":{"target_kind":"student","target_ref":"st_0123456789abcdef0123456789abcdef",
			 "target_source":"teacher_manual","manual_targets":["language.node.infer"],"taxonomy_version":"v1","area_tag":"language",
			 "type_tags":["concept"],"item_format":"mcq","count":1}}}
			""".formatted(TENANT);
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class Topics {
		@Bean NewTopic requested() { return TopicBuilder.name(REQUEST_TOPIC).partitions(1).replicas(1).build(); }
		@Bean NewTopic result() { return TopicBuilder.name(RESULT_TOPIC).partitions(1).replicas(1).build(); }
		@Bean NewTopic dlt() { return TopicBuilder.name(REQUEST_TOPIC + ".dlt").partitions(1).replicas(1).build(); }
	}
}
