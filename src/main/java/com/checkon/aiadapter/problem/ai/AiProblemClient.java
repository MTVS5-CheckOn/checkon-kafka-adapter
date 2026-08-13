package com.checkon.aiadapter.problem.ai;

import java.time.Duration;

import tools.jackson.databind.JsonNode;

public interface AiProblemClient {
	JsonNode submit(String requestBody, Headers headers);
	JobResponse job(String jobId, Headers headers);
	JsonNode items(String setId, Headers headers);
	JsonNode item(String setId, int slotIndex, Headers headers);

	record Headers(String tenantAlias, String requestId, String idempotencyKey) { }
	record JobResponse(JsonNode body, Duration retryAfter) { }
}
