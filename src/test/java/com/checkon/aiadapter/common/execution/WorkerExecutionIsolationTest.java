package com.checkon.aiadapter.common.execution;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

class WorkerExecutionIsolationTest {
	@Test
	@DisplayName("Given 장시간 문제 출제 작업 When 위험 탐지와 Outbox를 실행하면 Then 별도 bounded scheduler에서 계속 동작한다")
	void isolatesLongProblemGenerationFromRiskAndOutbox() throws Exception {
		// Given
		var configuration=new WorkerExecutionConfiguration();
		var properties=new WorkerExecutionProperties(1,1,1,1);
		ThreadPoolTaskScheduler risk=configuration.riskDetectionScheduler(properties);
		ThreadPoolTaskScheduler problem=configuration.problemGenerationScheduler(properties);
		ThreadPoolTaskScheduler outbox=configuration.outboxPublisherScheduler(properties);
		risk.initialize();problem.initialize();outbox.initialize();
		CountDownLatch problemStarted=new CountDownLatch(1),releaseProblem=new CountDownLatch(1),others=new CountDownLatch(2);
		try {
			problem.execute(()->{problemStarted.countDown();try{releaseProblem.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}});
			assertThat(problemStarted.await(1,TimeUnit.SECONDS)).isTrue();
			// When
			risk.execute(others::countDown);outbox.execute(others::countDown);
			// Then
			assertThat(others.await(1,TimeUnit.SECONDS)).isTrue();
			assertThat(risk.getPoolSize()).isEqualTo(1);
			assertThat(problem.getPoolSize()).isEqualTo(1);
			assertThat(outbox.getPoolSize()).isEqualTo(1);
		}
		finally {releaseProblem.countDown();risk.shutdown();problem.shutdown();outbox.shutdown();}
	}
}
