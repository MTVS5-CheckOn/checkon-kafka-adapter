package com.checkon.aiadapter.problem.application;

import java.time.Clock;
import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.checkon.aiadapter.common.kafka.UuidV7Generator;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationRequestedEvent;

@Service
@ConditionalOnProperty(prefix = "checkon.ai.problem-generation", name = "worker-enabled", havingValue = "true")
public class ProblemGenerationRequestHandler {
	private final ProblemGenerationStore store;
	private final UuidV7Generator ids;
	private final Clock clock;

	public ProblemGenerationRequestHandler(ProblemGenerationStore store, UuidV7Generator ids, Clock clock) {
		this.store = store;
		this.ids = ids;
		this.clock = clock;
	}

	public void handle(ProblemGenerationRequestedEvent event, String rawPayload) {
		var registration = store.register(event, ids.next(), rawPayload, Instant.now(clock));
		if (registration == ProblemGenerationStore.Registration.CONFLICT) {
			throw new ProblemGenerationRequestConflictException("event_id was reused with another payload");
		}
	}
}
