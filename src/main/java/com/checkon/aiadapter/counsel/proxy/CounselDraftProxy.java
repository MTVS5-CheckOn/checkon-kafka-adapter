package com.checkon.aiadapter.counsel.proxy;

import org.springframework.http.ResponseEntity;

/**
 * Backend-facing passthrough for the two counsel draft calls that stayed
 * synchronous after the 2026-08-20 POST-계약변경 (draft creation itself moved
 * to the Kafka SUBMIT/POLL worker in {@code counsel.application}). GET and
 * refine are relayed to the real AI server byte-for-byte, mirroring
 * {@code problem.diagnosis.ProblemDiagnosisProxy}.
 */
public interface CounselDraftProxy {

	/** {@code GET /v1/counsel/drafts/{jobId}} -- read-only, no Idempotency-Key. */
	ResponseEntity<String> getDraft(String jobId, Headers headers);

	/** {@code POST /v1/counsel/drafts/{jobId}/refine} -- Idempotency-Key is mandatory. */
	ResponseEntity<String> refine(String jobId, String payload, Headers headers);

	record Headers(String tenantAlias, String requestId, String idempotencyKey) {
	}
}
