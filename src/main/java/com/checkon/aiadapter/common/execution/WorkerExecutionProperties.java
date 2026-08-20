package com.checkon.aiadapter.common.execution;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("checkon.execution")
public record WorkerExecutionProperties(
	@DefaultValue("1") int riskDetectionPoolSize,
	@DefaultValue("1") int problemGenerationPoolSize,
	@DefaultValue("1") int outboxPoolSize
) {
	public WorkerExecutionProperties {
		riskDetectionPoolSize=bounded(riskDetectionPoolSize,"riskDetectionPoolSize");
		problemGenerationPoolSize=bounded(problemGenerationPoolSize,"problemGenerationPoolSize");
		outboxPoolSize=bounded(outboxPoolSize,"outboxPoolSize");
	}
	private static int bounded(int value,String name) {
		if(value<1||value>16) throw new IllegalArgumentException(name+" must be 1..16");
		return value;
	}
}
