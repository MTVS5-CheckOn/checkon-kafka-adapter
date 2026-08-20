package com.checkon.aiadapter.counsel.ai;

public record AiCounselDraftRequestHeaders(
	String tenantAlias,
	String requestId,
	String idempotencyKey
) {

	public AiCounselDraftRequestHeaders {
		required(tenantAlias, "tenantAlias");
		required(requestId, "requestId");
		required(idempotencyKey, "idempotencyKey");
	}

	private static void required(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
	}
}
