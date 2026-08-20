package com.checkon.aiadapter.counsel.ai;

public interface AiCounselDraftClient {

	AiCounselDraftResponse createDraft(
		AiCounselDraftRequest request,
		AiCounselDraftRequestHeaders headers
	);

	AiCounselDraftResponse createDraftRaw(
		String requestBody,
		AiCounselDraftRequestHeaders headers
	);
}
