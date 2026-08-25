package com.checkon.aiadapter.counsel.classification;

import java.net.http.HttpClient;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
class InquiryClassificationProxyConfiguration {

	@Bean
	InquiryClassificationProxy inquiryClassificationProxy(
		@Value("${checkon.ai.classify.base-url:http://localhost:8000}") String baseUrl,
		@Value("${checkon.ai.classify.path:/v1/classify}") String classifyPath,
		@Value("${checkon.ai.classify.connect-timeout:2s}") Duration connectTimeout,
		@Value("${checkon.ai.classify.read-timeout:10s}") Duration readTimeout
	) {
		HttpClient httpClient = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
		factory.setReadTimeout(readTimeout);
		RestClient restClient = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
		return new RestInquiryClassificationProxy(restClient, classifyPath);
	}
}
