package com.checkon.aiadapter.problem.application;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("checkon.ai.problem-generation")
public record ProblemGenerationProperties(
	boolean workerEnabled,
	String baseUrl,
	@DefaultValue("/v1/problems") String problemsPath,
	@DefaultValue("2s") Duration pollInterval,
	@DefaultValue("30s") Duration lockTimeout,
	@DefaultValue("1s") Duration retryInitialDelay,
	@DefaultValue("5") int maxAttempts,
	@DefaultValue("3s") Duration connectTimeout,
	@DefaultValue("30s") Duration readTimeout,
	@DefaultValue("65536") int maxReferenceEventBytes,
	@DefaultValue("1048576") int maxDetailEventBytes
) {
	public ProblemGenerationProperties {
		problemsPath = requirePath(problemsPath);
		pollInterval = positive(pollInterval, "pollInterval");
		lockTimeout = positive(lockTimeout, "lockTimeout");
		retryInitialDelay = positive(retryInitialDelay, "retryInitialDelay");
		connectTimeout = positive(connectTimeout, "connectTimeout");
		readTimeout = positive(readTimeout, "readTimeout");
		if (maxAttempts < 1 || maxAttempts > 20) throw new IllegalArgumentException("maxAttempts must be 1..20");
		if(maxReferenceEventBytes<1024||maxDetailEventBytes<1024)
			throw new IllegalArgumentException("problem generation event byte limits must be at least 1024");
	}

	public Duration retryDelayAfter(int attempts) {
		return com.checkon.aiadapter.common.durability.RetryPolicy.exponential(retryInitialDelay,attempts);
	}

	private static Duration positive(Duration value, String name) {
		if (value == null || value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return value;
	}

	private static String requirePath(String value) {
		if (value == null || !value.startsWith("/")) throw new IllegalArgumentException("problemsPath must start with '/'");
		return value;
	}
}
