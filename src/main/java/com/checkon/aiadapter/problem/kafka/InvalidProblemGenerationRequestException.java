package com.checkon.aiadapter.problem.kafka;

public class InvalidProblemGenerationRequestException extends RuntimeException {
	public InvalidProblemGenerationRequestException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidProblemGenerationRequestException(String message) {
		super(message);
	}
}
