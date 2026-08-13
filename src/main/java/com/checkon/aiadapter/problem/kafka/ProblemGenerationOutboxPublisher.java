package com.checkon.aiadapter.problem.kafka;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedOutbox;

@Component
@ConditionalOnExpression("${checkon.kafka.problem-generation.enabled:false} && ${checkon.ai.problem-generation.worker-enabled:false}")
public class ProblemGenerationOutboxPublisher {
	private final ProblemGenerationStore store;
	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ProblemGenerationKafkaProperties properties;
	private final Clock clock;

	public ProblemGenerationOutboxPublisher(ProblemGenerationStore store,
		KafkaTemplate<String, String> kafkaTemplate, ProblemGenerationKafkaProperties properties, Clock clock) {
		this.store = store; this.kafkaTemplate = kafkaTemplate; this.properties = properties; this.clock = clock;
	}

	@Scheduled(fixedDelayString = "${checkon.kafka.problem-generation.outbox-poll-delay:1s}",
		initialDelayString = "${checkon.kafka.problem-generation.outbox-poll-delay:1s}")
	public void poll() { publishOne(); }

	public boolean publishOne() {
		return store.claimOutbox(Instant.now(clock), properties.outboxLockTimeout()).map(this::publish).orElse(false);
	}

	private boolean publish(ClaimedOutbox event) {
		try {
			kafkaTemplate.send(event.topic(), event.messageKey(), event.eventPayload())
				.get(properties.producerSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
			store.outboxPublished(event.eventId(), event.sourceEventId(), Instant.now(clock));
		}
		catch (Exception exception) {
			if (event.publishAttempt() >= properties.outboxMaxAttempts()) {
				store.outboxDead(event.eventId(), event.sourceEventId(), "KAFKA_PUBLISH_FAILED", Instant.now(clock));
			}
			else {
				store.outboxRetry(event.eventId(), Instant.now(clock).plus(properties.outboxRetryDelay()),
					"KAFKA_PUBLISH_FAILED");
			}
		}
		return true;
	}
}
