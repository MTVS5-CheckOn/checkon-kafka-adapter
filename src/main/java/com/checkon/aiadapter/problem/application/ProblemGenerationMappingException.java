package com.checkon.aiadapter.problem.application;

public class ProblemGenerationMappingException extends RuntimeException {
	private final String code;

	public ProblemGenerationMappingException(String code, String message) {
		super(message);
		this.code = code;
	}

	public String code() {
		return code;
	}
}
