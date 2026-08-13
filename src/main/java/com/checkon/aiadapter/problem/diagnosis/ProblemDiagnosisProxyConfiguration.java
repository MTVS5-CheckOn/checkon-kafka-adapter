package com.checkon.aiadapter.problem.diagnosis;

import java.net.http.HttpClient;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
class ProblemDiagnosisProxyConfiguration {
	@Bean
	ProblemDiagnosisProxy problemDiagnosisProxy(
		@Value("${checkon.ai.problem-generation.base-url:http://localhost:8000}") String baseUrl,
		@Value("${checkon.ai.problem-generation.diagnosis-path:/v1/diagnosis}") String path,
		@Value("${checkon.ai.problem-generation.connect-timeout:3s}") Duration connectTimeout,
		@Value("${checkon.ai.problem-generation.read-timeout:30s}") Duration readTimeout) {
		HttpClient http = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
		factory.setReadTimeout(readTimeout);
		return new RestProblemDiagnosisProxy(
			RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build(), path);
	}
}
