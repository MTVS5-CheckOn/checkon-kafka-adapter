package com.checkon.aiadapter.detection.ai;

public interface AiRiskDetectionClient {

	AiDetectionResponse detect(
		AiDetectionRequest request,
		AiDetectionRequestHeaders headers
	);
}
