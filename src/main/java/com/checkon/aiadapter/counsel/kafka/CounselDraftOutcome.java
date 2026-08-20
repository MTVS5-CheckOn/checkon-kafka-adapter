package com.checkon.aiadapter.counsel.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Payload of the {@code counsel-draft.completed} event -- an ID reference
 * only, per the counsel contract's own appendix note: the draft body is
 * never carried here, the backend fetches it afterward via its own
 * {@code GET /v1/counsel/drafts/{ai_job_id}} call. Must match
 * {@code CounselDraftKafkaOutcome} in CheckOn-backend field for field.
 */
public record CounselDraftOutcome(
	@JsonProperty("job_id") String jobId,
	String status,
	@JsonProperty("execution_id") String executionId
) {

	public CounselDraftOutcome {
		if (jobId == null || jobId.isBlank()) {
			throw new IllegalArgumentException("job_id must not be blank");
		}
		if (status == null || status.isBlank()) {
			throw new IllegalArgumentException("status must not be blank");
		}
	}
}
