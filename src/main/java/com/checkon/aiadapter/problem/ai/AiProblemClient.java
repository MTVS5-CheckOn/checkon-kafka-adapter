package com.checkon.aiadapter.problem.ai;

import tools.jackson.databind.JsonNode;

public interface AiProblemClient {
	JsonNode submit(String requestBody, Headers headers);
	JsonNode job(String jobId, Headers headers);
	JsonNode items(String jobId, Headers headers);

	record Headers(String tenantAlias, String requestId, String idempotencyKey) { }
}
