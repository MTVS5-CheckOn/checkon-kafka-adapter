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
		@Value("${checkon.ai.label-confirmation.base-url:http://localhost:8000}") String baseUrl,
		@Value("${checkon.ai.label-confirmation.label-suggest-path:/v1/labels/suggest}") String labelSuggestPath,
		@Value("${checkon.ai.label-confirmation.confirmations-path:/v1/confirmations}") String confirmationsPath,
		@Value("${checkon.ai.label-confirmation.connect-timeout:2s}") Duration connectTimeout,
		@Value("${checkon.ai.label-confirmation.read-timeout:20s}") Duration readTimeout
	) {
		HttpClient httpClient = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
		factory.setReadTimeout(readTimeout);
		RestClient restClient = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
		return new RestCounselSynchronousProxy(restClient, labelSuggestPath, confirmationsPath);
	}
}
