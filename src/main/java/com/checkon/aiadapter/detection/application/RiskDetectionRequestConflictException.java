package com.checkon.aiadapter.detection.application;

import java.util.UUID;

public class RiskDetectionRequestConflictException extends RuntimeException {

	public RiskDetectionRequestConflictException(UUID eventId) {
		super("A different payload already exists for event_id " + eventId);
	}
}
