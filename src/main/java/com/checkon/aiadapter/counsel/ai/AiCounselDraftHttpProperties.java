package com.checkon.aiadapter.counsel.ai;

import java.net.URI;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * AI-A 2026-08-20 타임아웃권고 실측(배포 반영됨): POST는 이제 적재만 하고
 * 즉시 반환하며, GET도 잡을 돌리지 않고 현재 phase만 읽는다. 콜드 터널
 * 왕복 최악 0.93초에 약 10배 여유를 둔 10초로 낮춘다. Mirrors CheckOn-backend's
 * own checkon.ai.counsel.get-read-timeout default (10s) for consistency.
 */
@ConfigurationProperties("checkon.ai.counsel-draft")
public record AiCounselDraftHttpProperties(
	boolean enabled,
	String baseUrl,
	String draftsPath,
	@DefaultValue("2s") Duration connectTimeout,
	@DefaultValue("10s") Duration readTimeout
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
