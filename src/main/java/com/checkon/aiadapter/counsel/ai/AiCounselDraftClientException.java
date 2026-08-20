package com.checkon.aiadapter.counsel.ai;

public class AiCounselDraftClientException extends RuntimeException {

	private final Reason reason;
	private final Integer httpStatus;
	private final String responseBody;

	private AiCounselDraftClientException(
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

	public static AiCounselDraftClientException idempotencyConflict(Throwable cause) {
		return idempotencyConflict(null, cause);
	}

	public static AiCounselDraftClientException idempotencyConflict(
		String responseBody,
		Throwable cause
	) {
		return new AiCounselDraftClientException(
			Reason.IDEMPOTENCY_CONFLICT, 409, responseBody, cause);
	}

	public static AiCounselDraftClientException httpError(int status, Throwable cause) {
		return httpError(status, null, cause);
	}

	public static AiCounselDraftClientException httpError(
		int status,
		String responseBody,
		Throwable cause
	) {
		return new AiCounselDraftClientException(
			Reason.HTTP_ERROR, status, responseBody, cause);
	}

	public static AiCounselDraftClientException emptyResponse() {
		return new AiCounselDraftClientException(Reason.EMPTY_RESPONSE, null, null, null);
	}

	public static AiCounselDraftClientException networkError(Throwable cause) {
		return new AiCounselDraftClientException(Reason.NETWORK_ERROR, null, null, cause);
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
