package com.checkon.aiadapter.problem.ai;

public class AiProblemClientException extends RuntimeException {
	private final boolean transientFailure;
	private final String code;

	public AiProblemClientException(String code, boolean transientFailure, Throwable cause) {
		super(code, cause);
		this.code = code;
		this.transientFailure = transientFailure;
	}

	public boolean isTransientFailure() { return transientFailure; }
	public String code() { return code; }
}
