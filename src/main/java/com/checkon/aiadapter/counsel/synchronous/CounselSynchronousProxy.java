package com.checkon.aiadapter.counsel.synchronous;

import org.springframework.http.ResponseEntity;

interface CounselSynchronousProxy {

	ResponseEntity<String> suggestLabels(String payload, Headers headers);

	ResponseEntity<String> confirmClassification(String payload, Headers headers);

	ResponseEntity<String> confirmLabel(String payload, Headers headers);

	record Headers(String tenantAlias, String requestId) {
	}
}
