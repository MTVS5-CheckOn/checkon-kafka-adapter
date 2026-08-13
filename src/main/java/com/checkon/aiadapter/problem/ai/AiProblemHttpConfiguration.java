package com.checkon.aiadapter.problem.ai;

import java.net.URI;
import java.net.http.HttpClient;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import com.checkon.aiadapter.problem.application.ProblemGenerationProperties;

import tools.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "checkon.ai.problem-generation", name = "worker-enabled", havingValue = "true")
public class AiProblemHttpConfiguration {
	@Bean
	AiProblemClient aiProblemClient(ProblemGenerationProperties properties, ObjectMapper objectMapper) {
		URI baseUri = URI.create(properties.baseUrl());
		if (!baseUri.isAbsolute()) throw new IllegalArgumentException("problem generation baseUrl must be absolute");
		HttpClient httpClient = HttpClient.newBuilder().connectTimeout(properties.connectTimeout())
			.version(HttpClient.Version.HTTP_1_1).build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
		factory.setReadTimeout(properties.readTimeout());
		return new HttpAiProblemClient(RestClient.builder().baseUrl(baseUri.toString()).requestFactory(factory).build(),
			properties.problemsPath(), objectMapper);
	}
}
