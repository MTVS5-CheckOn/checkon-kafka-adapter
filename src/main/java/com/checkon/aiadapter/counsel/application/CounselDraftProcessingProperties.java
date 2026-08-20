package com.checkon.aiadapter.counsel.application;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("checkon.ai.counsel-draft")
public record CounselDraftProcessingProperties(
	boolean workerEnabled,
	Duration pollDelay,
	@DefaultValue("2s") Duration pollInterval,
	Duration lockTimeout,
	Duration retryInitialDelay,
	@DefaultValue("2m") Duration maxElapsed,
	int maxAttempts
) {

	public CounselDraftProcessingProperties {
		pollDelay = positive(pollDelay, "pollDelay");
		pollInterval = positive(pollInterval, "pollInterval");
		lockTimeout = positive(lockTimeout, "lockTimeout");
		retryInitialDelay = positive(retryInitialDelay, "retryInitialDelay");
		maxElapsed = positive(maxElapsed, "maxElapsed");
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
