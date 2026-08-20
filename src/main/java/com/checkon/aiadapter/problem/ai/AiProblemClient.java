package com.checkon.aiadapter.problem.ai;

import java.time.Duration;

public interface AiProblemClient {
	ProblemSubmissionResponse submit(String requestBody, Headers headers);
	JobResponse job(String jobId, Headers headers);
	ProblemItemSetResponse items(String setId, Headers headers);
	ProblemItemDetailResponse item(String setId, int slotIndex, Headers headers);

	record Headers(String tenantAlias, String requestId, String idempotencyKey) { }
	record JobResponse(ProblemJobResponse body, Duration retryAfter) { }
}
