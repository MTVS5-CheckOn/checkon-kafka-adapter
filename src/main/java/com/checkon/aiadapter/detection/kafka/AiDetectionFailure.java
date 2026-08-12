package com.checkon.aiadapter.detection.kafka;

public record AiDetectionFailure(
	String code,
	String message,
	Object detail,
	boolean retryable
) {

	public AiDetectionFailure {
		if (code == null || code.isBlank() || code.length() > 60) {
			throw new IllegalArgumentException("code must contain 1 to 60 characters");
		}
		if (message == null || message.isBlank()) {
			throw new IllegalArgumentException("message must not be blank");
		}
	}
}
