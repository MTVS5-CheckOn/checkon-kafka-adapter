package com.checkon.aiadapter.detection.ai;

public class AiRiskDetectionClientException extends RuntimeException {

	private final Reason reason;
	private final Integer httpStatus;

	private AiRiskDetectionClientException(
		Reason reason,
		Integer httpStatus,
		Throwable cause
	) {
		super(reason.name(), cause);
		this.reason = reason;
		this.httpStatus = httpStatus;
	}

	public static AiRiskDetectionClientException idempotencyConflict(Throwable cause) {
		return new AiRiskDetectionClientException(Reason.IDEMPOTENCY_CONFLICT, 409, cause);
	}

	public static AiRiskDetectionClientException httpError(int status, Throwable cause) {
		return new AiRiskDetectionClientException(Reason.HTTP_ERROR, status, cause);
	}

	public static AiRiskDetectionClientException emptyResponse() {
		return new AiRiskDetectionClientException(Reason.EMPTY_RESPONSE, null, null);
	}

	public static AiRiskDetectionClientException networkError(Throwable cause) {
		return new AiRiskDetectionClientException(Reason.NETWORK_ERROR, null, cause);
	}

	public Reason reason() {
		return reason;
	}

	public Integer httpStatus() {
		return httpStatus;
	}

	public enum Reason {
		IDEMPOTENCY_CONFLICT,
		HTTP_ERROR,
		EMPTY_RESPONSE,
		NETWORK_ERROR
	}
}
