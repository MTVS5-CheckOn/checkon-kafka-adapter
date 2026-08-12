package com.checkon.aiadapter.detection.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

import com.checkon.aiadapter.detection.ai.AiDetectionRequestHeaders;
import com.checkon.aiadapter.detection.ai.AiDetectionResponse;
import com.checkon.aiadapter.detection.ai.AiRiskDetectionClient;
import com.checkon.aiadapter.detection.application.RiskDetectionExecutionWorker;
import com.checkon.aiadapter.detection.kafka.RiskDetectionOutboxPublisher;
import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Testcontainers
@SpringBootTest(properties = {
	"checkon.kafka.risk-detection.enabled=true",
	"checkon.kafka.risk-detection.outbox-poll-delay=1h",
	"checkon.ai.risk-detection.worker-enabled=true",
	"checkon.ai.risk-detection.poll-delay=1h",
	"spring.kafka.consumer.auto-offset-reset=earliest"
})
@Import(RiskDetectionVerticalSliceIntegrationTest.TopicConfiguration.class)
class RiskDetectionVerticalSliceIntegrationTest {

	private static final String REQUESTED_TOPIC = "checkon.risk-detection.requested.v1";
	private static final String COMPLETED_TOPIC = "checkon.risk-detection.completed.v1";
	private static final String FAILED_TOPIC = "checkon.risk-detection.failed.v1";
	private static final String TENANT_ALIAS = "tn_0123456789abcdef0123456789abcdef";

	@Container
	static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.4");

	@Container
	static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2");

	@DynamicPropertySource
	static void connectionProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		registry.add("spring.datasource.username", POSTGRES::getUsername);
		registry.add("spring.datasource.password", POSTGRES::getPassword);
		registry.add("spring.flyway.url", POSTGRES::getJdbcUrl);
		registry.add("spring.flyway.user", POSTGRES::getUsername);
		registry.add("spring.flyway.password", POSTGRES::getPassword);
		registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
	}

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	RiskDetectionExecutionWorker executionWorker;

	@Autowired
	RiskDetectionOutboxPublisher outboxPublisher;

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	ObjectMapper objectMapper;

	@MockitoBean
	AiRiskDetectionClient aiClient;

	private RiskDetectionRequestedEvent requestEvent;
	private String rawRequest;

	@BeforeEach
	void setUp() throws Exception {
		jdbcTemplate.update("DELETE FROM risk_detection_outbox");
		jdbcTemplate.update("DELETE FROM risk_detection_request_inbox");
		rawRequest = readFixture();
		requestEvent = objectMapper.readValue(rawRequest, RiskDetectionRequestedEvent.class);
	}

	@Test
	@DisplayName("Given Backend requested 이벤트 When Adapter가 처리하면 Then completed 이벤트를 동일 추적 정보로 발행한다")
	void publishesCompletedOutcomeEndToEnd() throws Exception {
		// Given
		AiDetectionResponse response = successfulResponse();
		when(aiClient.detect(requestEvent.payload(), requestHeaders())).thenReturn(response);

		// When
		ConsumerRecord<String, String> completed;
		try (KafkaConsumer<String, String> consumer = outcomeConsumer(COMPLETED_TOPIC)) {
			consumer.subscribe(List.of(COMPLETED_TOPIC));
			kafkaTemplate.send(REQUESTED_TOPIC, TENANT_ALIAS, rawRequest)
				.get(10, TimeUnit.SECONDS);
			awaitInbox(Duration.ofSeconds(15));
			assertThat(executionWorker.processOne()).isTrue();
			assertThat(outboxPublisher.publishOne()).isTrue();
			completed = pollOne(consumer, Duration.ofSeconds(15));
		}

		// Then
		assertThat(completed).isNotNull();
		assertThat(completed.key()).isEqualTo(TENANT_ALIAS);
		JsonNode outcome = objectMapper.readTree(completed.value());
		assertThat(outcome.get("event_type").asText()).isEqualTo("risk-detection.completed");
		assertThat(outcome.get("causation_id").asText())
			.isEqualTo(requestEvent.eventId().toString());
		assertThat(outcome.get("correlation_id").asText())
			.isEqualTo(requestEvent.runId().toString());
		assertThat(outcome.get("request_id").asText()).isEqualTo(requestEvent.requestId());
		assertThat(inboxStatus()).isEqualTo("OUTCOME_PUBLISHED");
		assertThat(outboxStatus()).isEqualTo("PUBLISHED");
		verify(aiClient).detect(requestEvent.payload(), requestHeaders());
	}

	@Test
	@DisplayName("Given AI 멱등 충돌 When Adapter가 처리하면 Then failed 이벤트를 발행한다")
	void publishesFailedOutcomeForTerminalAiFailure() throws Exception {
		// Given
		when(aiClient.detect(requestEvent.payload(), requestHeaders()))
			.thenThrow(com.checkon.aiadapter.detection.ai.AiRiskDetectionClientException
				.idempotencyConflict(null));

		// When
		ConsumerRecord<String, String> failed;
		try (KafkaConsumer<String, String> consumer = outcomeConsumer(FAILED_TOPIC)) {
			consumer.subscribe(List.of(FAILED_TOPIC));
			kafkaTemplate.send(REQUESTED_TOPIC, TENANT_ALIAS, rawRequest)
				.get(10, TimeUnit.SECONDS);
			awaitInbox(Duration.ofSeconds(15));
			executionWorker.processOne();
			outboxPublisher.publishOne();
			failed = pollOne(consumer, Duration.ofSeconds(15));
		}

		// Then
		assertThat(failed).isNotNull();
		JsonNode outcome = objectMapper.readTree(failed.value());
		assertThat(outcome.get("event_type").asText()).isEqualTo("risk-detection.failed");
		assertThat(outcome.get("payload").get("code").asText())
			.isEqualTo("IDEMPOTENCY_CONFLICT");
		assertThat(outcome.get("payload").get("retryable").asBoolean()).isFalse();
		assertThat(inboxStatus()).isEqualTo("OUTCOME_PUBLISHED");
	}

	private AiDetectionRequestHeaders requestHeaders() {
		return new AiDetectionRequestHeaders(
			requestEvent.tenantAlias(), requestEvent.requestId(), requestEvent.idempotencyKey());
	}

	private AiDetectionResponse successfulResponse() {
		return new AiDetectionResponse(
			new AiDetectionResponse.Data(
				List.of(),
				new AiDetectionResponse.Stats(1, 0, 0, 0, List.of())
			),
			null,
			new AiDetectionResponse.Meta("ai-execution-1", Map.of("contract", "0.2"))
		);
	}

	private KafkaConsumer<String, String> outcomeConsumer(String topic) {
		return new KafkaConsumer<>(Map.of(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
			ConsumerConfig.GROUP_ID_CONFIG, "adapter-outcome-test-" + topic + UUID.randomUUID(),
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
		));
	}

	private void awaitInbox(Duration timeout) throws InterruptedException {
		Instant deadline = Instant.now().plus(timeout);
		while (Instant.now().isBefore(deadline)) {
			Integer count = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM risk_detection_request_inbox", Integer.class);
			if (count != null && count == 1) {
				return;
			}
			Thread.sleep(100);
		}
		throw new AssertionError("Requested event was not stored in the Inbox");
	}

	private ConsumerRecord<String, String> pollOne(
		KafkaConsumer<String, String> consumer,
		Duration timeout
	) {
		Instant deadline = Instant.now().plus(timeout);
		while (Instant.now().isBefore(deadline)) {
			for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
				return record;
			}
		}
		return null;
	}

	private String inboxStatus() {
		return jdbcTemplate.queryForObject(
			"SELECT status FROM risk_detection_request_inbox", String.class);
	}

	private String outboxStatus() {
		return jdbcTemplate.queryForObject(
			"SELECT status FROM risk_detection_outbox", String.class);
	}

	private String readFixture() throws IOException {
		try (var input = getClass().getClassLoader().getResourceAsStream(
			"contracts/risk-detection-requested-v0.2.json")) {
			assertThat(input).isNotNull();
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class TopicConfiguration {

		@Bean
		NewTopic requestedTopic() {
			return TopicBuilder.name(REQUESTED_TOPIC).partitions(1).replicas(1).build();
		}

		@Bean
		NewTopic completedTopic() {
			return TopicBuilder.name(COMPLETED_TOPIC).partitions(1).replicas(1).build();
		}

		@Bean
		NewTopic failedTopic() {
			return TopicBuilder.name(FAILED_TOPIC).partitions(1).replicas(1).build();
		}
	}
}
