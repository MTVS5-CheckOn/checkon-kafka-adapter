package com.checkon.aiadapter.detection.application;

import java.time.Clock;
import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionInboxRepository;
import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

@Service
@ConditionalOnProperty(
	prefix = "checkon.ai.risk-detection",
	name = "worker-enabled",
	havingValue = "true"
)
public class DurableRiskDetectionRequestHandler implements RiskDetectionRequestHandler {

	private final RiskDetectionInboxRepository inboxRepository;
	private final Clock clock;

	public DurableRiskDetectionRequestHandler(
		RiskDetectionInboxRepository inboxRepository,
		Clock clock
	) {
		this.inboxRepository = inboxRepository;
		this.clock = clock;
	}

	@Override
	@Transactional
	public void handle(RiskDetectionRequestedEvent event) {
		RiskDetectionInboxRepository.Registration registration =
			inboxRepository.register(event, Instant.now(clock));
		if (registration == RiskDetectionInboxRepository.Registration.CONFLICT) {
			throw new RiskDetectionRequestConflictException(event.eventId());
		}
	}
}
