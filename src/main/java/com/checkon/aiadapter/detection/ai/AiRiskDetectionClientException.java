package com.checkon.aiadapter.detection.ai;

public class AiRiskDetectionClientException extends RuntimeException {

	private final Reason reason;
	private final Integer httpStatus;
	private final String responseBody;

	private AiRiskDetectionClientException(
		Reason reason,
		Integer httpStatus,
		String responseBody,
		Throwable cause
	) {
		super(reason.name(), cause);
		this.reason = reason;
		this.httpStatus = httpStatus;
		this.responseBody = responseBody;
	}

	public static AiRiskDetectionClientException idempotencyConflict(Throwable cause) {
		return idempotencyConflict(null, cause);
	}

	public static AiRiskDetectionClientException idempotencyConflict(
		String responseBody,
		Throwable cause
	) {
		return new AiRiskDetectionClientException(
			Reason.IDEMPOTENCY_CONFLICT, 409, responseBody, cause);
	}

	public static AiRiskDetectionClientException httpError(int status, Throwable cause) {
		return httpError(status, null, cause);
	}

	public static AiRiskDetectionClientException httpError(
		int status,
		String responseBody,
		Throwable cause
	) {
		return new AiRiskDetectionClientException(
			Reason.HTTP_ERROR, status, responseBody, cause);
	}

	public static AiRiskDetectionClientException emptyResponse() {
		return new AiRiskDetectionClientException(Reason.EMPTY_RESPONSE, null, null, null);
	}

	public static AiRiskDetectionClientException networkError(Throwable cause) {
		return new AiRiskDetectionClientException(Reason.NETWORK_ERROR, null, null, cause);
	}

	public Reason reason() {
		return reason;
	}

	public Integer httpStatus() {
		return httpStatus;
	}

	public String responseBody() {
		return responseBody;
	}

	public boolean isTransientFailure() {
		if (reason == Reason.NETWORK_ERROR) {
			return true;
		}
		return reason == Reason.HTTP_ERROR
			&& httpStatus != null
			&& (httpStatus == 408 || httpStatus == 429 || httpStatus >= 500);
	}

	public enum Reason {
		IDEMPOTENCY_CONFLICT,
		HTTP_ERROR,
		EMPTY_RESPONSE,
		NETWORK_ERROR
	}
}
