package com.checkon.aiadapter.common.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("checkon.kafka.risk-detection")
public record RiskDetectionKafkaProperties(
	boolean enabled,
	String requestedTopic,
	String completedTopic,
	String failedTopic,
	String consumerGroupId
) {

	public RiskDetectionKafkaProperties {
		required(requestedTopic, "requestedTopic");
		required(completedTopic, "completedTopic");
		required(failedTopic, "failedTopic");
		required(consumerGroupId, "consumerGroupId");
	}

	private static void required(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
	}
}
