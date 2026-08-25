package com.checkon.aiadapter.problem.ai;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record ProblemJobResponse(Data data, Meta meta) {
	public record Data(@JsonProperty("job_id") String jobId, JobStatus status, Result result) { }
	public record Result(@JsonProperty("set_id") String setId, String status,
		@JsonProperty("requested_count") Integer requestedCount,
		@JsonProperty("processed_count") Integer processedCount,
		@JsonProperty("unstarted_count") Integer unstartedCount,
		@JsonProperty("status_counts") java.util.Map<String,Integer> statusCounts,
		@JsonProperty("stop_reason") String stopReason,
		@JsonProperty("public_failure_reason") String publicFailureReason) { }
	public record Meta(@JsonProperty("execution_id") String executionId) { }
	public enum JobStatus {
		QUEUED, LEASED, RUNNING, PAUSED, SUCCEEDED, FAILED, CANCELLED;
		@JsonCreator
		public static JobStatus parse(String value) {
			if(value==null||value.isBlank()) throw new IllegalArgumentException("job status is required");
			try{return valueOf(value.toUpperCase(java.util.Locale.ROOT));}
			catch(IllegalArgumentException exception){throw new IllegalArgumentException("unknown AI job status: "+value);}
		}
		public boolean terminal(){return this==SUCCEEDED||this==FAILED||this==CANCELLED;}
	}
	public JobStatus requiredStatus(){if(data==null||data.status()==null)throw new IllegalArgumentException("job status is required");return data.status();}
	public String requiredSetId(){String value=data==null||data.result()==null?null:data.result().setId();if(value==null||value.isBlank())throw new IllegalArgumentException("set_id must not be blank");return value;}
}
