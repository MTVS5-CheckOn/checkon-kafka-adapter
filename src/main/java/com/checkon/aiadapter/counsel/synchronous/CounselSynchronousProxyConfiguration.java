package com.checkon.aiadapter.counsel.synchronous;

import java.net.http.HttpClient;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
class CounselSynchronousProxyConfiguration {

	@Bean
	CounselSynchronousProxy counselSynchronousProxy(
		@Value("${checkon.ai.classify.base-url:http://localhost:8000}") String classifyBaseUrl,
		@Value("${checkon.ai.classify.confirmations-path:/v1/confirmations}") String classifyConfirmationsPath,
		@Value("${checkon.ai.classify.connect-timeout:2s}") Duration classifyConnectTimeout,
		@Value("${checkon.ai.classify.read-timeout:30s}") Duration classifyReadTimeout,
		@Value("${checkon.ai.labels.base-url:http://localhost:8000}") String labelsBaseUrl,
		@Value("${checkon.ai.labels.suggest-path:/v1/labels/suggest}") String labelSuggestPath,
		@Value("${checkon.ai.labels.confirmations-path:/v1/confirmations}") String labelConfirmationsPath,
		@Value("${checkon.ai.labels.connect-timeout:2s}") Duration labelsConnectTimeout,
		@Value("${checkon.ai.labels.read-timeout:30s}") Duration labelsReadTimeout
	) {
		return new RestCounselSynchronousProxy(
			restClient(classifyBaseUrl, classifyConnectTimeout, classifyReadTimeout),
			classifyConfirmationsPath,
			restClient(labelsBaseUrl, labelsConnectTimeout, labelsReadTimeout),
			labelSuggestPath,
			labelConfirmationsPath
		);
	}

	private static RestClient restClient(String baseUrl, Duration connectTimeout, Duration readTimeout) {
		HttpClient httpClient = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
		factory.setReadTimeout(readTimeout);
		return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
	}
}
