package com.checkon.aiadapter.detection.ai;

import java.net.http.HttpClient;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import tools.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
	prefix = "checkon.ai.risk-detection",
	name = "enabled",
	havingValue = "true"
)
public class AiRiskDetectionHttpConfiguration {

	@Bean
	AiRiskDetectionClient aiRiskDetectionClient(
		AiRiskDetectionHttpProperties properties,
		ObjectMapper objectMapper
	) {
		HttpClient httpClient = HttpClient.newBuilder()
			.connectTimeout(properties.requiredConnectTimeout())
			.version(HttpClient.Version.HTTP_1_1)
			.build();
		JdkClientHttpRequestFactory requestFactory =
			new JdkClientHttpRequestFactory(httpClient);
		requestFactory.setReadTimeout(properties.requiredReadTimeout());

		return new HttpAiRiskDetectionClient(
			RestClient.builder()
				.baseUrl(properties.requiredBaseUri().toString())
				.requestFactory(requestFactory)
				.build(),
			properties.requiredDetectPath(),
			objectMapper
		);
	}
}
