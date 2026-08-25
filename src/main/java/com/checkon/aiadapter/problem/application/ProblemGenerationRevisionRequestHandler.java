package com.checkon.aiadapter.problem.application;

import java.time.Clock;
import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationRevisionRequestedEvent;

@Service
@ConditionalOnProperty(prefix="checkon.ai.problem-generation",name="worker-enabled",havingValue="true")
public class ProblemGenerationRevisionRequestHandler {
	private final ProblemGenerationStore store; private final Clock clock;
	public ProblemGenerationRevisionRequestHandler(ProblemGenerationStore store,Clock clock){this.store=store;this.clock=clock;}
	public void handle(ProblemGenerationRevisionRequestedEvent event,String rawPayload){
		var registration=store.registerRevision(event,rawPayload,Instant.now(clock));
		if(registration==ProblemGenerationStore.Registration.CONFLICT)
			throw new ProblemGenerationRequestConflictException("revision event_id was reused with another payload");
	}
}
