package com.checkon.aiadapter.problem.kafka;

import java.time.Clock;
import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedOutbox;
import com.checkon.aiadapter.common.durability.OutboxPublicationService;
import com.checkon.aiadapter.common.durability.OutboxPublicationService.Message;
import com.checkon.aiadapter.common.durability.OutboxPublicationService.Transitions;

@Component
@ConditionalOnExpression("${checkon.kafka.problem-generation.enabled:false} && ${checkon.ai.problem-generation.worker-enabled:false}")
public class ProblemGenerationOutboxPublisher {
	private final ProblemGenerationStore store;
	private final OutboxPublicationService publicationService;
	private final ProblemGenerationKafkaProperties properties;
	private final Clock clock;

	public ProblemGenerationOutboxPublisher(ProblemGenerationStore store,
		OutboxPublicationService publicationService, ProblemGenerationKafkaProperties properties, Clock clock) {
		this.store = store; this.publicationService = publicationService; this.properties = properties; this.clock = clock;
	}

	@Scheduled(fixedDelayString = "${checkon.kafka.problem-generation.outbox-poll-delay:1s}",
		initialDelayString = "${checkon.kafka.problem-generation.outbox-poll-delay:1s}",scheduler = "outboxPublisherScheduler")
	public void poll() { publishOne(); }

	public boolean publishOne() {
		return store.claimOutbox(Instant.now(clock), properties.outboxLockTimeout()).map(this::publish).orElse(false);
	}

	private boolean publish(ClaimedOutbox event) {
		return publicationService.publish("problem_generation",
			new Message(event.topic(), event.messageKey(), event.eventPayload(), event.publishAttempt()),
			properties.outboxMaxAttempts(), properties.outboxRetryDelay(), properties.producerSendTimeout(),
			new Transitions() {
				@Override public void published(Instant now) {
					store.outboxPublished(event.eventId(), event.sourceEventId(), event.claimVersion(), now);
				}
				@Override public void retry(Instant availableAt, String errorCode, Instant now) {
					store.outboxRetry(event.eventId(), event.claimVersion(), availableAt, errorCode, now);
				}
				@Override public void dead(String errorCode, Instant now) {
					store.outboxDead(event.eventId(), event.sourceEventId(), event.claimVersion(), errorCode, now);
				}
			});
	}
}
