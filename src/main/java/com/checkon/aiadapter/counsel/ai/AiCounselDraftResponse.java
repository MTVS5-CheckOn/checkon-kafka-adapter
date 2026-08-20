package com.checkon.aiadapter.counsel.ai;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 202 response for {@code POST /v1/counsel/drafts}. {@code data.status} is
 * intentionally a plain string (not a typed enum) — the adapter only relays
 * whatever phase the AI reports, it never interprets it (contract appendix A
 * is the backend's concern, not the adapter's).
 */
public record AiCounselDraftResponse(
	Data data,
	AiCounselDraftError error,
	AiCounselDraftMeta meta
) {

	public record Data(
		@JsonProperty("job_id") String jobId,
		String status
	) {
	}
}
