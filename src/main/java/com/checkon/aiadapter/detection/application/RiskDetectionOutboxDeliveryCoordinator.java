package com.checkon.aiadapter.detection.application;

import java.time.Instant;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionInboxRepository;
import com.checkon.aiadapter.detection.infrastructure.persistence.RiskDetectionOutboxRepository;

@Service
@ConditionalOnProperty(
	prefix = "checkon.ai.risk-detection",
	name = "worker-enabled",
	havingValue = "true"
)
public class RiskDetectionOutboxDeliveryCoordinator {

	private final RiskDetectionInboxRepository inboxRepository;
	private final RiskDetectionOutboxRepository outboxRepository;

	public RiskDetectionOutboxDeliveryCoordinator(
		RiskDetectionInboxRepository inboxRepository,
		RiskDetectionOutboxRepository outboxRepository
	) {
		this.inboxRepository = inboxRepository;
		this.outboxRepository = outboxRepository;
	}

	@Transactional
	public void published(UUID outboxEventId, UUID sourceEventId, Instant now) {
		outboxRepository.markPublished(outboxEventId, now);
		inboxRepository.markOutcomePublished(sourceEventId, now);
	}

	@Transactional
	public void retry(UUID outboxEventId, Instant availableAt, String errorCode) {
		outboxRepository.markRetry(outboxEventId, availableAt, errorCode);
	}

	@Transactional
	public void dead(
		UUID outboxEventId,
		UUID sourceEventId,
		String errorCode,
		Instant now
	) {
		outboxRepository.markDead(outboxEventId, errorCode);
		inboxRepository.markOutcomeDead(sourceEventId, errorCode, now);
	}
}
