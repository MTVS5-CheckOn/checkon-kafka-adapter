package com.checkon.aiadapter.counsel.application;

import org.springframework.stereotype.Component;

import com.checkon.aiadapter.counsel.ai.AiCounselDraftResponse;

/**
 * Unlike {@code AiDetectionResponseValidator}, this has no request-vs-response
 * content to cross-check -- the completed event only ever relays
 * {@code job_id}/{@code status}/{@code execution_id} (an ID reference, per
 * contract appendix note), so this only checks the envelope itself is
 * well-formed.
 */
@Component
public class AiCounselDraftResponseValidator {

	public void validate(AiCounselDraftResponse response) {
		if (response == null || response.data() == null || response.meta() == null) {
			throw invalid("data and meta are required");
		}
		if (response.error() != null) {
			throw invalid("a successful response must have error=null");
		}
		requireText(response.data().jobId(), "data.job_id");
		requireText(response.data().status(), "data.status");
		requireText(response.meta().executionId(), "meta.execution_id");
	}

	private static void requireText(String value, String field) {
		if (value == null || value.isBlank()) {
			throw invalid(field + " must not be blank");
		}
	}

	private static InvalidAiCounselDraftResponseException invalid(String message) {
		return new InvalidAiCounselDraftResponseException(message);
	}
}
