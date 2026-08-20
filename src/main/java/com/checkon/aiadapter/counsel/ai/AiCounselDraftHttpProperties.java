package com.checkon.aiadapter.counsel.ai;

import java.net.URI;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * connectTimeout/readTimeout default to the counsel contract's own numbers
 * (POST runs a job to completion inline, K=1: worst case 5 LLM calls x 90s =
 * 450s, so the adapter -- which now makes this call directly instead of the
 * backend -- must allow at least 480s or a normal request gets cut client
 * side while the AI keeps running). Mirrors CheckOn-backend's own
 * checkon.ai.counsel.read-timeout default (500s) for consistency.
 */
@ConfigurationProperties("checkon.ai.counsel-draft")
public record AiCounselDraftHttpProperties(
	boolean enabled,
	String baseUrl,
	String draftsPath,
	@DefaultValue("2s") Duration connectTimeout,
	@DefaultValue("500s") Duration readTimeout
) {

	URI requiredBaseUri() {
		requiredText(baseUrl, "baseUrl");
		URI uri = URI.create(baseUrl);
		if (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme())) {
			throw new IllegalArgumentException("baseUrl must use http or https");
		}
		return uri;
	}

	String requiredDraftsPath() {
		requiredText(draftsPath, "draftsPath");
		if (!draftsPath.startsWith("/")) {
			throw new IllegalArgumentException("draftsPath must start with '/'");
		}
		return draftsPath;
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
