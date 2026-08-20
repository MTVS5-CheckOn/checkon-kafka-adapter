package com.checkon.aiadapter.counsel.ai;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@code versions} is left untyped — the adapter only needs
 * {@code execution_id} to fill the completed-event payload, it does not
 * validate the AI's internal version pinning.
 */
public record AiCounselDraftMeta(
	@JsonProperty("execution_id") String executionId,
	Object versions
) {
}
