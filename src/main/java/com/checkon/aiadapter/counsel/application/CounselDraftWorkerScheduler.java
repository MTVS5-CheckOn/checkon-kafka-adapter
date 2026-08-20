package com.checkon.aiadapter.counsel.application;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "checkon.ai.counsel-draft", name = "worker-enabled", havingValue = "true")
@ConditionalOnProperty(prefix = "checkon.ai.counsel-draft", name = "scheduler-enabled", havingValue = "true", matchIfMissing = true)
public class CounselDraftWorkerScheduler {
	private final CounselDraftExecutionWorker worker;

	public CounselDraftWorkerScheduler(CounselDraftExecutionWorker worker) {
		this.worker = worker;
	}

	@Scheduled(
		fixedDelayString = "${checkon.ai.counsel-draft.poll-delay}",
		initialDelayString = "${checkon.ai.counsel-draft.poll-delay}",
		scheduler = "counselDraftScheduler"
	)
	public void poll() {
		worker.processOne();
	}
}
