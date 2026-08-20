package com.checkon.aiadapter.counsel.ai;

import java.time.Duration;

public interface AiCounselDraftClient {

	AiCounselDraftResponse createDraft(
		AiCounselDraftRequest request,
		AiCounselDraftRequestHeaders headers
	);

	AiCounselDraftResponse createDraftRaw(
		String requestBody,
		AiCounselDraftRequestHeaders headers
	);

	/**
	 * {@code GET /v1/counsel/drafts/{jobId}} -- never runs the job, only reads
	 * current phase (2026-08-20 POST-계약변경: "GET status ∈ {queued, leased,
	 * running, paused} → Retry-After 가 붙습니다"). No Idempotency-Key: GET is
	 * not a mutating call.
	 */
	PollResponse getDraft(String jobId, AiCounselDraftRequestHeaders headers);

	/** {@code retryAfter} is null when the response carried no {@code Retry-After} header (terminal statuses never carry one). */
	record PollResponse(AiCounselDraftResponse body, Duration retryAfter) {
	}
}
