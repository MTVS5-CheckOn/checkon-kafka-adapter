package com.checkon.aiadapter.problem.kafka;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("checkon.kafka.problem-generation")
public record ProblemGenerationKafkaProperties(
	boolean enabled,
	@DefaultValue("checkon.ai.problem-generation.requests.v1") String requestTopic,
	@DefaultValue("checkon.ai.problem-generation.results.v1") String resultTopic,
	@DefaultValue("checkon-ai-adapter-problem-generation-v1") String consumerGroupId,
	@DefaultValue("1s") Duration outboxPollDelay,
	@DefaultValue("30s") Duration outboxLockTimeout,
	@DefaultValue("5s") Duration outboxRetryDelay,
	@DefaultValue("10s") Duration producerSendTimeout,
	@DefaultValue("8") int outboxMaxAttempts
) {
	public ProblemGenerationKafkaProperties {
		if(requestTopic==null||requestTopic.isBlank()||resultTopic==null||resultTopic.isBlank()||consumerGroupId==null||consumerGroupId.isBlank())
			throw new IllegalArgumentException("problem generation Kafka names must not be blank");
		for(var value:new Duration[]{outboxPollDelay,outboxLockTimeout,outboxRetryDelay,producerSendTimeout})
			if(value==null||value.isZero()||value.isNegative()) throw new IllegalArgumentException("problem generation Kafka durations must be positive");
		if(outboxMaxAttempts<1||outboxMaxAttempts>100) throw new IllegalArgumentException("outboxMaxAttempts must be 1..100");
	}
}
