package com.checkon.aiadapter.counsel.kafka;

import java.time.Clock;
import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.common.kafka.CounselDraftKafkaProperties;
import com.checkon.aiadapter.common.durability.OutboxPublicationService;
import com.checkon.aiadapter.common.durability.OutboxPublicationService.Message;
import com.checkon.aiadapter.common.durability.OutboxPublicationService.Transitions;
import com.checkon.aiadapter.counsel.application.CounselDraftOutboxDeliveryCoordinator;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftOutboxRepository;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftOutboxRepository.ClaimedOutboxEvent;

@Component
@ConditionalOnExpression(
	"${checkon.kafka.counsel-draft.enabled:false}"
		+ " && ${checkon.ai.counsel-draft.worker-enabled:false}"
)
public class CounselDraftOutboxPublisher {

	private final CounselDraftOutboxRepository outboxRepository;
	private final CounselDraftOutboxDeliveryCoordinator deliveryCoordinator;
	private final OutboxPublicationService publicationService;
	private final CounselDraftKafkaProperties properties;
	private final Clock clock;

	public CounselDraftOutboxPublisher(
		CounselDraftOutboxRepository outboxRepository,
		CounselDraftOutboxDeliveryCoordinator deliveryCoordinator,
		OutboxPublicationService publicationService,
		CounselDraftKafkaProperties properties,
		Clock clock
	) {
		this.outboxRepository = outboxRepository;
		this.deliveryCoordinator = deliveryCoordinator;
		this.publicationService = publicationService;
		this.properties = properties;
		this.clock = clock;
	}

	@Scheduled(
		fixedDelayString = "${checkon.kafka.counsel-draft.outbox-poll-delay}",
		initialDelayString = "${checkon.kafka.counsel-draft.outbox-poll-delay}", scheduler = "outboxPublisherScheduler"
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
		return publicationService.publish("counsel_draft",
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
