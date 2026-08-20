package com.checkon.aiadapter.common.durability;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.common.observability.DurabilityMetrics;

@Component
public class OutboxPublicationService {
	private static final String PUBLISH_ERROR = "KAFKA_PUBLISH_FAILED";
	private final KafkaTemplate<String, String> kafkaTemplate;
	private final Clock clock;

	public OutboxPublicationService(KafkaTemplate<String, String> kafkaTemplate, Clock clock) {
		this.kafkaTemplate = kafkaTemplate;
		this.clock = clock;
	}

	public boolean publish(String feature, Message message, int maxAttempts, Duration retryDelay,
		Duration sendTimeout, Transitions transitions) {
		try {
			kafkaTemplate.send(message.topic(), message.key(), message.payload())
				.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
			transitions.published(Instant.now(clock));
			DurabilityMetrics.transition(feature, "outbox_published");
		}
		catch (Exception exception) {
			Instant now = Instant.now(clock);
			if (message.publishAttempt() >= maxAttempts) {
				transitions.dead(PUBLISH_ERROR, now);
				DurabilityMetrics.transition(feature, "outbox_dead");
			}
			else {
				transitions.retry(now.plus(retryDelay), PUBLISH_ERROR, now);
				DurabilityMetrics.transition(feature, "outbox_retry");
			}
		}
		return true;
	}

	public record Message(String topic, String key, String payload, int publishAttempt) { }

	public interface Transitions {
		void published(Instant now);
		void retry(Instant availableAt, String errorCode, Instant now);
		void dead(String errorCode, Instant now);
	}
}
