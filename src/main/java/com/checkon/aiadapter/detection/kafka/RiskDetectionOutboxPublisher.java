package com.checkon.aiadapter.detection.kafka;

import java.time.Clock;
import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.common.kafka.RiskDetectionKafkaProperties;
import com.checkon.aiadapter.common.durability.OutboxPublicationService;
import com.checkon.aiadapter.common.durability.OutboxPublicationService.Message;
import com.checkon.aiadapter.common.durability.OutboxPublicationService.Transitions;
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
	private final OutboxPublicationService publicationService;
	private final RiskDetectionKafkaProperties properties;
	private final Clock clock;

	public RiskDetectionOutboxPublisher(
		RiskDetectionOutboxRepository outboxRepository,
		RiskDetectionOutboxDeliveryCoordinator deliveryCoordinator,
		OutboxPublicationService publicationService,
		RiskDetectionKafkaProperties properties,
		Clock clock
	) {
		this.outboxRepository = outboxRepository;
		this.deliveryCoordinator = deliveryCoordinator;
		this.publicationService = publicationService;
		this.properties = properties;
		this.clock = clock;
	}

	@Scheduled(
		fixedDelayString = "${checkon.kafka.risk-detection.outbox-poll-delay}",
		initialDelayString = "${checkon.kafka.risk-detection.outbox-poll-delay}", scheduler = "outboxPublisherScheduler"
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
		return publicationService.publish("risk_detection",
			new Message(event.topic(), event.messageKey(), event.eventPayload(), event.publishAttempt()),
			properties.outboxMaxAttempts(), properties.outboxRetryDelay(), properties.producerSendTimeout(),
			new Transitions() {
				@Override public void published(Instant now) {
					deliveryCoordinator.published(event.eventId(), event.sourceEventId(), event.claimVersion(), now);
				}
				@Override public void retry(Instant availableAt, String errorCode, Instant now) {
					deliveryCoordinator.retry(event.eventId(), event.claimVersion(), availableAt, errorCode);
				}
				@Override public void dead(String errorCode, Instant now) {
					deliveryCoordinator.dead(event.eventId(), event.sourceEventId(), event.claimVersion(), errorCode, now);
				}
			});
	}
}
