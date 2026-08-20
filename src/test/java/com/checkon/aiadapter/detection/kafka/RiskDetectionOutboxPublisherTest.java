package com.checkon.aiadapter.detection.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.checkon.aiadapter.common.kafka.RiskDetectionKafkaProperties;
import com.checkon.aiadapter.common.durability.OutboxPublicationService;
import com.checkon.aiadapter.detection.application.RiskDetectionOutboxDeliveryCoordinator;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionOutboxRepository;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionOutboxRepository.ClaimedOutboxEvent;

class RiskDetectionOutboxPublisherTest {

	private static final Instant NOW = Instant.parse("2026-08-12T00:00:00Z");
	private static final UUID OUTBOX_ID = UUID.fromString(
		"019b0000-0000-7000-8000-000000000021");
	private static final UUID SOURCE_ID = UUID.fromString(
		"019b0000-0000-7000-8000-000000000011");

	private final RiskDetectionOutboxRepository repository = mock(
		RiskDetectionOutboxRepository.class);
	private final RiskDetectionOutboxDeliveryCoordinator coordinator = mock(
		RiskDetectionOutboxDeliveryCoordinator.class);
	@SuppressWarnings("unchecked")
	private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
	private final RiskDetectionKafkaProperties properties = new RiskDetectionKafkaProperties(
		true,
		"checkon.risk-detection.requested.v1",
		"checkon.risk-detection.completed.v1",
		"checkon.risk-detection.failed.v1",
		"checkon-ai-adapter-risk-detection-v1",
		Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(5),
		Duration.ofSeconds(10), 8
	);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

	private RiskDetectionOutboxPublisher publisher;

	@BeforeEach
	void setUp() {
		publisher = new RiskDetectionOutboxPublisher(
			repository, coordinator, new OutboxPublicationService(kafkaTemplate, clock), properties, clock);
	}

	@Test
	@DisplayName("Given Kafka 발행 성공 When Outbox를 처리하면 Then PUBLISHED로 전이한다")
	void marksPublishedAfterBrokerAcknowledgement() {
		// Given
		ClaimedOutboxEvent event = event(1);
		when(repository.claimNext(NOW, properties.outboxLockTimeout()))
			.thenReturn(Optional.of(event));
		when(kafkaTemplate.send(event.topic(), event.messageKey(), event.eventPayload()))
			.thenReturn(CompletableFuture.completedFuture(null));

		// When
		boolean processed = publisher.publishOne();

		// Then
		assertThat(processed).isTrue();
		verify(coordinator).published(OUTBOX_ID, SOURCE_ID, 1, NOW);
	}

	@Test
	@DisplayName("Given 첫 Kafka 발행 실패 When Outbox를 처리하면 Then 동일 이벤트를 5초 뒤 재시도한다")
	void schedulesOutboxRetry() {
		// Given
		ClaimedOutboxEvent event = event(1);
		when(repository.claimNext(NOW, properties.outboxLockTimeout()))
			.thenReturn(Optional.of(event));
		CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
		failed.completeExceptionally(new IllegalStateException("broker unavailable"));
		when(kafkaTemplate.send(event.topic(), event.messageKey(), event.eventPayload()))
			.thenReturn(failed);

		// When
		publisher.publishOne();

		// Then
		verify(coordinator).retry(
			OUTBOX_ID, 1, NOW.plusSeconds(5), "KAFKA_PUBLISH_FAILED");
		verify(coordinator, never()).dead(
			org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
			org.mockito.ArgumentMatchers.anyLong(),
			org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
	}

	@Test
	@DisplayName("Given 여덟 번째 Kafka 발행 실패 When Outbox를 처리하면 Then DEAD로 전이한다")
	void marksDeadAfterRetryExhaustion() {
		// Given
		ClaimedOutboxEvent event = event(8);
		when(repository.claimNext(NOW, properties.outboxLockTimeout()))
			.thenReturn(Optional.of(event));
		CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
		failed.completeExceptionally(new IllegalStateException("broker unavailable"));
		when(kafkaTemplate.send(event.topic(), event.messageKey(), event.eventPayload()))
			.thenReturn(failed);

		// When
		publisher.publishOne();

		// Then
		verify(coordinator).dead(
			OUTBOX_ID, SOURCE_ID, 1, "KAFKA_PUBLISH_FAILED", NOW);
	}

	private ClaimedOutboxEvent event(int attempt) {
		return new ClaimedOutboxEvent(
			OUTBOX_ID, SOURCE_ID, "checkon.risk-detection.completed.v1",
			"tn_0123456789abcdef0123456789abcdef", "{}", attempt, 1, false);
	}
}
