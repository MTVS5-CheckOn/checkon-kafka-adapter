package com.checkon.aiadapter.detection.ai;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/**
 * Unlike {@code ProblemGenerationProperties}, this record used to have no
 * {@code @DefaultValue} on connect/read timeout, and {@code application.yaml}
 * only supplied a blank placeholder fallback ({@code ${AI_CONNECT_TIMEOUT:}}).
 * Any deployment that enabled risk detection without separately setting those
 * two env vars got null timeouts, caught only at startup by
 * {@link com.checkon.aiadapter.common.execution.RuntimeConfigurationValidator}
 * with a generic "timeouts are required" error instead of just working like
 * problem-generation does.
 */
@DisplayName("위험 탐지 AI HTTP 타임아웃 기본값")
class AiRiskDetectionHttpPropertiesTest {

	@Test
	@DisplayName("Given connect-timeout/read-timeout를 아무 값도 주지 않았을 때 When 바인딩하면 Then null이 아닌 기본값으로 채워진다")
	void fallsBackToDefaultTimeoutsWhenUnset() {
		var source = new MapConfigurationPropertySource(Map.of(
			"checkon.ai.risk-detection.enabled", "true",
			"checkon.ai.risk-detection.base-url", "http://localhost:8000",
			"checkon.ai.risk-detection.detect-path", "/v1/detect"
		));
		var binder = new Binder(source);

		var properties = binder.bind("checkon.ai.risk-detection", Bindable.of(AiRiskDetectionHttpProperties.class)).get();

		assertThat(properties.connectTimeout()).isNotNull();
		assertThat(properties.readTimeout()).isNotNull();
		assertThat(properties.requiredConnectTimeout()).isEqualTo(Duration.ofSeconds(2));
		assertThat(properties.requiredReadTimeout()).isEqualTo(Duration.ofSeconds(65));
	}
}
