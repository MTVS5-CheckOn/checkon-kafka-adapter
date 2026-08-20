package com.checkon.aiadapter.counsel.kafka;

/** Must match {@code CounselDraftKafkaFailure} in CheckOn-backend field for field. */
public record AiCounselDraftFailure(
	String code,
	String message,
	Object detail,
	boolean retryable
) {

	public AiCounselDraftFailure {
		if (code == null || code.isBlank() || code.length() > 60) {
			throw new IllegalArgumentException("code must contain 1 to 60 characters");
		}
		if (message == null || message.isBlank()) {
			throw new IllegalArgumentException("message must not be blank");
		}
	}
}
