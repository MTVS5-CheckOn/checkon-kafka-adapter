package com.checkon.aiadapter.counsel.application;

import java.util.UUID;

public class CounselDraftRequestConflictException extends RuntimeException {

	public CounselDraftRequestConflictException(UUID eventId) {
		super("A different payload already exists for event_id " + eventId);
	}
}
