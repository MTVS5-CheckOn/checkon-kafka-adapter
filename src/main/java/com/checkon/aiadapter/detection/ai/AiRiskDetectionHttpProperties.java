package com.checkon.aiadapter.detection.ai;

import java.net.URI;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * connectTimeout/readTimeout default to the values documented in
 * docs/CONTRACT_BOUNDARY.md ("read timeout은 60초보다 긴 65초를 사용한다") so a
 * deployment that enables risk detection without separately setting
 * AI_CONNECT_TIMEOUT/AI_READ_TIMEOUT still boots with a working, contract-sized
 * timeout instead of null (mirrors ProblemGenerationProperties's own
 * @DefaultValue pattern for the same fields).
 */
@ConfigurationProperties("checkon.ai.risk-detection")
public record AiRiskDetectionHttpProperties(
	boolean enabled,
	String baseUrl,
	String detectPath,
	@DefaultValue("2s") Duration connectTimeout,
	@DefaultValue("65s") Duration readTimeout
) {

	URI requiredBaseUri() {
		requiredText(baseUrl, "baseUrl");
		URI uri = URI.create(baseUrl);
		if (!"http".equalsIgnoreCase(uri.getScheme())
			&& !"https".equalsIgnoreCase(uri.getScheme())) {
			throw new IllegalArgumentException("baseUrl must use http or https");
		}
		return uri;
	}

	String requiredDetectPath() {
		requiredText(detectPath, "detectPath");
		if (!detectPath.startsWith("/")) {
			throw new IllegalArgumentException("detectPath must start with '/'");
		}
		return detectPath;
	}

	Duration requiredConnectTimeout() {
		return requiredPositive(connectTimeout, "connectTimeout");
	}

	Duration requiredReadTimeout() {
		return requiredPositive(readTimeout, "readTimeout");
	}

	private static Duration requiredPositive(Duration value, String name) {
		if (value == null || value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return value;
	}

	private static void requiredText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
	}
}
