package com.checkon.aiadapter.detection.ai;

public record AiDetectionRequestHeaders(
	String tenantAlias,
	String requestId,
	String idempotencyKey
) {

	public AiDetectionRequestHeaders {
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
