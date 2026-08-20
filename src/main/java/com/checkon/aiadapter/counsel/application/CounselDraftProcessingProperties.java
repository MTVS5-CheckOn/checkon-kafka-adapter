package com.checkon.aiadapter.counsel.application;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("checkon.ai.counsel-draft")
public record CounselDraftProcessingProperties(
	boolean workerEnabled,
	Duration pollDelay,
	Duration lockTimeout,
	Duration retryInitialDelay,
	int maxAttempts
) {

	public CounselDraftProcessingProperties {
		pollDelay = positive(pollDelay, "pollDelay");
		lockTimeout = positive(lockTimeout, "lockTimeout");
		retryInitialDelay = positive(retryInitialDelay, "retryInitialDelay");
		if (maxAttempts < 1 || maxAttempts > 20) {
			throw new IllegalArgumentException("maxAttempts must be 1..20");
		}
	}

	public Duration retryDelayAfter(int completedAttempts) {
		return com.checkon.aiadapter.common.durability.RetryPolicy.exponential(retryInitialDelay, completedAttempts);
	}

	private static Duration positive(Duration value, String name) {
		if (value == null || value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return value;
	}
}
