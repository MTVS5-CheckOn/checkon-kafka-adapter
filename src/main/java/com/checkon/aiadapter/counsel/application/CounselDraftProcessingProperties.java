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
	// AI-A 2026-08-20 타임아웃권고: 잡당 최악 상한이 450초로 실측 확인됐다.
	// 여기서 잘라내면 정상 진행 중인 잡을 조기 포기하게 되므로 여유를 둔다.
	@DefaultValue("8m") Duration maxElapsed,
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
