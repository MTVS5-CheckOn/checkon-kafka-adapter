package com.checkon.aiadapter.problem.ai;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ProblemSubmissionResponse(Data data, Meta meta) {
	public record Data(@JsonProperty("job_id") String jobId, String status,
		@JsonProperty("execution_id") String executionId) { }
	public record Meta(@JsonProperty("execution_id") String executionId) { }

	public String requiredJobId() { return required(data == null ? null : data.jobId(), "job_id"); }
	public String canonicalExecutionId() {
		String value = data != null && data.executionId() != null ? data.executionId() : meta == null ? null : meta.executionId();
		return required(value, "execution_id");
	}
	private static String required(String value,String name) {
		if(value==null||value.isBlank()) throw new IllegalArgumentException(name+" must not be blank");
		return value;
	}
}
