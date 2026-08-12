package com.checkon.aiadapter.detection.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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

import com.checkon.aiadapter.detection.application.RiskDetectionRequestHandler;

@Testcontainers
@SpringBootTest(properties = {
	"checkon.kafka.risk-detection.enabled=true",
	"spring.kafka.consumer.auto-offset-reset=earliest",
	"spring.autoconfigure.exclude="
		+ "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
		+ "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
})
@Import(KafkaRiskDetectionRequestListenerIntegrationTest.TopicConfiguration.class)
class KafkaRiskDetectionRequestListenerIntegrationTest {

	private static final String REQUESTED_TOPIC = "checkon.risk-detection.requested.v1";
	private static final String DLT_TOPIC = REQUESTED_TOPIC + ".dlt";
	private static final String TENANT_ALIAS = "tn_0123456789abcdef0123456789abcdef";

	@Container
	@ServiceConnection
	static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2");

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@MockitoBean
	RiskDetectionRequestHandler handler;

	@Test
	@DisplayName("Given 정상 requested 이벤트 When Kafka에서 소비하면 Then 검증 후 handler에 전달한다")
	void consumesValidRequest() throws Exception {
		// Given
		String rawEvent = readFixture();

		// When
		kafkaTemplate.send(REQUESTED_TOPIC, TENANT_ALIAS, rawEvent)
			.get(10, TimeUnit.SECONDS);

		// Then
		verify(handler, timeout(15_000)).handle(
			org.mockito.ArgumentMatchers.argThat(event ->
				event.eventId().equals(UUID.fromString("019b0000-0000-7000-8000-000000000011"))
					&& event.payload().detectionEvidence().size() == 1
			),
			org.mockito.ArgumentMatchers.eq(rawEvent)
		);
	}

	@Test
	@DisplayName("Given 잘못된 schema_version When Kafka에서 소비하면 Then handler를 호출하지 않고 DLT로 보낸다")
	void sendsContractViolationDirectlyToDlt() throws Exception {
		// Given
		String invalidEvent = readFixture().replace(
			"\"schema_version\": \"1.0\"",
			"\"schema_version\": \"2.0\""
		);

		// When
		ConsumerRecord<String, String> dltRecord;
		try (KafkaConsumer<String, String> dltConsumer = dltConsumer()) {
			dltConsumer.subscribe(List.of(DLT_TOPIC));
			kafkaTemplate.send(REQUESTED_TOPIC, TENANT_ALIAS, invalidEvent)
				.get(10, TimeUnit.SECONDS);
			dltRecord = pollOne(dltConsumer, Duration.ofSeconds(20));
		}

		// Then
		assertThat(dltRecord).isNotNull();
		assertThat(dltRecord.key()).isEqualTo(TENANT_ALIAS);
		assertThat(dltRecord.value()).contains("\"schema_version\": \"2.0\"");
		verifyNoInteractions(handler);
	}

	private KafkaConsumer<String, String> dltConsumer() {
		return new KafkaConsumer<>(Map.of(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
			ConsumerConfig.GROUP_ID_CONFIG, "adapter-contract-dlt-test-" + UUID.randomUUID(),
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
		));
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
		NewTopic requestedDltTopic() {
			return TopicBuilder.name(DLT_TOPIC).partitions(1).replicas(1).build();
		}
	}
}
