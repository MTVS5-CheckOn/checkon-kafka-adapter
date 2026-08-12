package com.checkon.aiadapter.detection.ai;

public interface AiRiskDetectionClient {

	AiDetectionResponse detect(
		AiDetectionRequest request,
		AiDetectionRequestHeaders headers
	);

	AiDetectionResponse detectRaw(
		String requestBody,
		AiDetectionRequestHeaders headers
	);
}
