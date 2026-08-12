package com.checkon.aiadapter.detection.kafka;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.common.kafka.RiskDetectionKafkaProperties;
import com.checkon.aiadapter.detection.application.RiskDetectionOutboxDeliveryCoordinator;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionOutboxRepository;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionOutboxRepository.ClaimedOutboxEvent;

@Component
@ConditionalOnExpression(
	"${checkon.kafka.risk-detection.enabled:false}"
		+ " && ${checkon.ai.risk-detection.worker-enabled:false}"
)
public class RiskDetectionOutboxPublisher {

	private final RiskDetectionOutboxRepository outboxRepository;
	private final RiskDetectionOutboxDeliveryCoordinator deliveryCoordinator;
	private final KafkaTemplate<String, String> kafkaTemplate;
	private final RiskDetectionKafkaProperties properties;
	private final Clock clock;

	public RiskDetectionOutboxPublisher(
		RiskDetectionOutboxRepository outboxRepository,
		RiskDetectionOutboxDeliveryCoordinator deliveryCoordinator,
		KafkaTemplate<String, String> kafkaTemplate,
		RiskDetectionKafkaProperties properties,
		Clock clock
	) {
		this.outboxRepository = outboxRepository;
		this.deliveryCoordinator = deliveryCoordinator;
		this.kafkaTemplate = kafkaTemplate;
		this.properties = properties;
		this.clock = clock;
	}

	@Scheduled(
		fixedDelayString = "${checkon.kafka.risk-detection.outbox-poll-delay}",
		initialDelayString = "${checkon.kafka.risk-detection.outbox-poll-delay}"
	)
	public void poll() {
		publishOne();
	}

	public boolean publishOne() {
		Instant now = Instant.now(clock);
		return outboxRepository.claimNext(now, properties.outboxLockTimeout())
			.map(this::publish)
			.orElse(false);
	}

	private boolean publish(ClaimedOutboxEvent event) {
		try {
			kafkaTemplate.send(
				event.topic(), event.messageKey(), event.eventPayload())
				.get(properties.producerSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
			deliveryCoordinator.published(
				event.eventId(), event.sourceEventId(), Instant.now(clock));
		}
		catch (Exception exception) {
			String errorCode = "KAFKA_PUBLISH_FAILED";
			if (event.publishAttempt() >= properties.outboxMaxAttempts()) {
				deliveryCoordinator.dead(
					event.eventId(), event.sourceEventId(), errorCode, Instant.now(clock));
			}
			else {
				deliveryCoordinator.retry(
					event.eventId(), Instant.now(clock).plus(properties.outboxRetryDelay()),
					errorCode);
			}
		}
		return true;
	}
}
