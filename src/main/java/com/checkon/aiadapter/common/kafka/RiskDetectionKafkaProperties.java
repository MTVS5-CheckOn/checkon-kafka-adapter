package com.checkon.aiadapter.common.kafka;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("checkon.kafka.risk-detection")
public record RiskDetectionKafkaProperties(
	boolean enabled,
	String requestedTopic,
	String completedTopic,
	String failedTopic,
	String consumerGroupId,
	Duration outboxPollDelay,
	Duration outboxLockTimeout,
	Duration outboxRetryDelay,
	Duration producerSendTimeout,
	int outboxMaxAttempts
) {

	public RiskDetectionKafkaProperties {
		required(requestedTopic, "requestedTopic");
		required(completedTopic, "completedTopic");
		required(failedTopic, "failedTopic");
		required(consumerGroupId, "consumerGroupId");
		outboxPollDelay = positive(outboxPollDelay, "outboxPollDelay");
		outboxLockTimeout = positive(outboxLockTimeout, "outboxLockTimeout");
		outboxRetryDelay = positive(outboxRetryDelay, "outboxRetryDelay");
		producerSendTimeout = positive(producerSendTimeout, "producerSendTimeout");
		if (outboxMaxAttempts < 1 || outboxMaxAttempts > 100) {
			throw new IllegalArgumentException("outboxMaxAttempts must be 1..100");
		}
	}

	private static void required(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
	}

	private static Duration positive(Duration value, String name) {
		if (value == null || value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return value;
	}
}
