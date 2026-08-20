package com.checkon.aiadapter.counsel.application;

import java.time.Instant;
import java.util.UUID;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository;
import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftOutboxRepository;

@Service
@ConditionalOnProperty(
	prefix = "checkon.ai.counsel-draft",
	name = "worker-enabled",
	havingValue = "true"
)
public class CounselDraftOutboxDeliveryCoordinator {

	private final CounselDraftInboxRepository inboxRepository;
	private final CounselDraftOutboxRepository outboxRepository;

	public CounselDraftOutboxDeliveryCoordinator(
		CounselDraftInboxRepository inboxRepository,
		CounselDraftOutboxRepository outboxRepository
	) {
		this.inboxRepository = inboxRepository;
		this.outboxRepository = outboxRepository;
	}

	@Transactional
	public void published(UUID outboxEventId, UUID sourceEventId, long claimVersion, Instant now) {
		outboxRepository.markPublished(outboxEventId, claimVersion, now);
		inboxRepository.markOutcomePublished(sourceEventId, now);
	}

	@Transactional
	public void retry(UUID outboxEventId, long claimVersion, Instant availableAt, String errorCode) {
		outboxRepository.markRetry(outboxEventId, claimVersion, availableAt, errorCode);
	}

	@Transactional
	public void dead(
		UUID outboxEventId,
		UUID sourceEventId,
		long claimVersion,
		String errorCode,
		Instant now
	) {
		outboxRepository.markDead(outboxEventId, claimVersion, errorCode);
		inboxRepository.markOutcomeDead(sourceEventId, errorCode, now);
	}
}
