package com.checkon.aiadapter.common.execution;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class WorkerExecutionConfiguration {
	@Bean("riskDetectionScheduler")
	ThreadPoolTaskScheduler riskDetectionScheduler(WorkerExecutionProperties properties) {
		return scheduler(properties.riskDetectionPoolSize(),"risk-detection-worker-");
	}
	@Bean("problemGenerationScheduler")
	ThreadPoolTaskScheduler problemGenerationScheduler(WorkerExecutionProperties properties) {
		return scheduler(properties.problemGenerationPoolSize(),"problem-generation-worker-");
	}
	@Bean("outboxPublisherScheduler")
	ThreadPoolTaskScheduler outboxPublisherScheduler(WorkerExecutionProperties properties) {
		return scheduler(properties.outboxPoolSize(),"outbox-publisher-");
	}
	private static ThreadPoolTaskScheduler scheduler(int poolSize,String prefix) {
		ThreadPoolTaskScheduler scheduler=new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(poolSize);
		scheduler.setThreadNamePrefix(prefix);
		scheduler.setWaitForTasksToCompleteOnShutdown(true);
		scheduler.setAwaitTerminationSeconds(30);
		scheduler.setRemoveOnCancelPolicy(true);
		return scheduler;
	}
}
