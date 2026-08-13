package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import com.checkon.aiadapter.problem.application.ProblemGenerationRequestHandler;

@Testcontainers
@SpringBootTest(properties = {
	"checkon.kafka.problem-generation.enabled=true",
	"spring.kafka.consumer.auto-offset-reset=earliest",
	"spring.autoconfigure.exclude="
		+ "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
		+ "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
})
@Import(KafkaProblemGenerationRequestListenerIntegrationTest.Topics.class)
class KafkaProblemGenerationRequestListenerIntegrationTest {
	private static final String TOPIC = "checkon.ai.problem-generation.requests.v1";
	private static final String DLT = TOPIC + ".dlt";
	private static final String TENANT = "tn_0123456789abcdef0123456789abcdef";

	@Container
	@ServiceConnection
	static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2");

	@Autowired KafkaTemplate<String, String> kafka;
	@MockitoBean ProblemGenerationRequestHandler handler;

	@Test
	@DisplayName("Given 유효한 pg-child-request-1 When 수신하면 Then child 식별자를 보존해 handler에 전달한다")
	void consumesValidChildRequest() throws Exception {
		String event = event("pg-child-request-1");

		kafka.send(TOPIC, TENANT, event).get(10, TimeUnit.SECONDS);

		verify(handler, timeout(15_000)).handle(
			org.mockito.ArgumentMatchers.argThat(value -> value.problemExecutionId().toString()
				.equals("01980000-0000-7000-8000-000000000003")),
			org.mockito.ArgumentMatchers.eq(event));
	}

	@Test
	@DisplayName("Given 잘못된 child schema When 수신하면 Then 재시도 없이 DLT로 격리한다")
	void routesInvalidContractToDlt() throws Exception {
		String invalid = event("pg-child-request-2");
		ConsumerRecord<String, String> record;
		try (KafkaConsumer<String, String> consumer = consumer()) {
			consumer.subscribe(List.of(DLT));
			kafka.send(TOPIC, TENANT, invalid).get(10, TimeUnit.SECONDS);
			record = pollOne(consumer, Duration.ofSeconds(20));
		}

		assertThat(record).isNotNull();
		assertThat(record.key()).isEqualTo(TENANT);
		assertThat(record.value()).contains("pg-child-request-2");
		verifyNoInteractions(handler);
	}

	private KafkaConsumer<String, String> consumer() {
		return new KafkaConsumer<>(Map.of(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
			ConsumerConfig.GROUP_ID_CONFIG, "problem-generation-dlt-" + UUID.randomUUID(),
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

	private String event(String schema) {
		return """
			{"event_id":"01980000-0000-7000-8000-000000000001","event_type":"problem_generation.requested",
			 "occurred_at":"2026-08-13T00:00:00Z","tenant_id":"%s","schema_version":"%s",
			 "correlation_id":"01980000-0000-7000-8000-000000000002","payload":{
			  "problem_request_id":"01980000-0000-7000-8000-000000000002",
			  "problem_execution_id":"01980000-0000-7000-8000-000000000003","target_index":0,
			  "idempotency_key":"child-0","request":{"target_kind":"student"}}}
			""".formatted(TENANT, schema);
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class Topics {
		@Bean NewTopic requested() { return TopicBuilder.name(TOPIC).partitions(1).replicas(1).build(); }
		@Bean NewTopic dlt() { return TopicBuilder.name(DLT).partitions(1).replicas(1).build(); }
	}
}
