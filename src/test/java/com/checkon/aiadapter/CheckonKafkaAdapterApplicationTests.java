package com.checkon.aiadapter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.checkon.aiadapter.common.kafka.RiskDetectionKafkaProperties;

@SpringBootTest
class CheckonKafkaAdapterApplicationTests {

	@Autowired
	RiskDetectionKafkaProperties kafkaProperties;

	@Test
	@DisplayName("Given 기본 설정 When 애플리케이션 컨텍스트를 로드하면 Then 정상적으로 시작된다")
	void contextLoads() {
		// Given/When: Spring Boot가 application.yaml을 사용해 컨텍스트를 시작한다.

		// Then
		assertThat(kafkaProperties.consumerGroupId())
			.isEqualTo("checkon-ai-adapter-risk-detection-v1");
	}
}
