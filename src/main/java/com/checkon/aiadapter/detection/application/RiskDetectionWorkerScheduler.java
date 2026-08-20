package com.checkon.aiadapter.detection.application;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix="checkon.ai.risk-detection",name="worker-enabled",havingValue="true")
@ConditionalOnProperty(prefix="checkon.ai.risk-detection",name="scheduler-enabled",havingValue="true",matchIfMissing=true)
public class RiskDetectionWorkerScheduler {
	private final RiskDetectionExecutionWorker worker;
	public RiskDetectionWorkerScheduler(RiskDetectionExecutionWorker worker){this.worker=worker;}
	@Scheduled(fixedDelayString="${checkon.ai.risk-detection.poll-delay}",initialDelayString="${checkon.ai.risk-detection.poll-delay}",scheduler="riskDetectionScheduler")
	public void poll(){worker.processOne();}
}
