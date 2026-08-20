package com.checkon.aiadapter.counsel.application;

import java.time.Clock;
import java.time.Instant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.checkon.aiadapter.counsel.infrastructure.persistence.CounselDraftInboxRepository;
import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

@Service
@ConditionalOnProperty(
	prefix = "checkon.ai.counsel-draft",
	name = "worker-enabled",
	havingValue = "true"
)
public class DurableCounselDraftRequestHandler implements CounselDraftRequestHandler {

	private final CounselDraftInboxRepository inboxRepository;
	private final Clock clock;

	public DurableCounselDraftRequestHandler(
		CounselDraftInboxRepository inboxRepository,
		Clock clock
	) {
		this.inboxRepository = inboxRepository;
		this.clock = clock;
	}

	@Override
	@Transactional
	public void handle(CounselDraftRequestedEvent event, String rawEvent) {
		CounselDraftInboxRepository.Registration registration =
			inboxRepository.register(event, rawEvent, Instant.now(clock));
		if (registration == CounselDraftInboxRepository.Registration.CONFLICT) {
			throw new CounselDraftRequestConflictException(event.eventId());
		}
	}
}
