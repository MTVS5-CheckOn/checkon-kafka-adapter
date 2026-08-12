package com.checkon.aiadapter.detection.application;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("checkon.ai.risk-detection")
public record RiskDetectionProcessingProperties(
	boolean workerEnabled,
	Duration pollDelay,
	Duration lockTimeout,
	Duration retryInitialDelay,
	int maxAttempts
) {

	public RiskDetectionProcessingProperties {
		pollDelay = positive(pollDelay, "pollDelay");
		lockTimeout = positive(lockTimeout, "lockTimeout");
		retryInitialDelay = positive(retryInitialDelay, "retryInitialDelay");
		if (maxAttempts < 1) {
			throw new IllegalArgumentException("maxAttempts must be at least 1");
		}
	}

	public Duration retryDelayAfter(int completedAttempts) {
		long multiplier = 1L << Math.min(Math.max(completedAttempts - 1, 0), 20);
		return retryInitialDelay.multipliedBy(multiplier);
	}

	private static Duration positive(Duration value, String name) {
		if (value == null || value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return value;
	}
}
