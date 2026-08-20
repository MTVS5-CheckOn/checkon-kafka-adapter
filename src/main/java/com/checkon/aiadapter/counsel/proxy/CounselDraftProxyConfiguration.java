package com.checkon.aiadapter.counsel.proxy;

import java.net.http.HttpClient;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
class CounselDraftProxyConfiguration {

	@Bean
	CounselDraftProxy counselDraftProxy(
		@Value("${checkon.ai.counsel-draft.base-url:http://localhost:8000}") String baseUrl,
		@Value("${checkon.ai.counsel-draft.drafts-path:/v1/counsel/drafts}") String draftsPath,
		@Value("${checkon.ai.counsel-draft.connect-timeout:2s}") Duration connectTimeout,
		// AI-A 2026-08-20 실측(배포 반영됨): GET은 잡을 돌리지 않고 현재 phase만
		// 읽으므로 create/poll 워커와 같은 10초 예산을 그대로 재사용한다.
		@Value("${checkon.ai.counsel-draft.read-timeout:10s}") Duration getReadTimeout,
		// refine은 여전히 동기 LLM 호출이다 -- 04 §1-1의 480초를 유지한다.
		@Value("${checkon.ai.counsel-draft.refine-read-timeout:480s}") Duration refineReadTimeout
	) {
		return new RestCounselDraftProxy(
			restClient(baseUrl, connectTimeout, getReadTimeout),
			restClient(baseUrl, connectTimeout, refineReadTimeout),
			draftsPath
		);
	}

	private static RestClient restClient(String baseUrl, Duration connectTimeout, Duration readTimeout) {
		HttpClient httpClient = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
		factory.setReadTimeout(readTimeout);
		return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
	}
}
