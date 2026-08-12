package com.checkon.aiadapter.detection.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
	prefix = "checkon.kafka.risk-detection",
	name = "enabled",
	havingValue = "true"
)
@EnableKafka
@EnableKafkaRetryTopic
public class RiskDetectionKafkaConfiguration {

	@Bean
	TaskScheduler riskDetectionRetryTaskScheduler() {
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(1);
		scheduler.setThreadNamePrefix("risk-detection-retry-");
		return scheduler;
	}
}
