package com.checkon.aiadapter.problem.application;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix="checkon.ai.problem-generation",name="worker-enabled",havingValue="true")
@ConditionalOnProperty(prefix="checkon.ai.problem-generation",name="scheduler-enabled",havingValue="true",matchIfMissing=true)
public class ProblemGenerationWorkerScheduler {
	private final ProblemGenerationExecutionWorker worker;
	public ProblemGenerationWorkerScheduler(ProblemGenerationExecutionWorker worker){this.worker=worker;}
	@Scheduled(fixedDelayString="${checkon.ai.problem-generation.poll-delay:1s}",initialDelayString="${checkon.ai.problem-generation.poll-delay:1s}",scheduler="problemGenerationScheduler")
	public void poll(){worker.processOne();}
}
