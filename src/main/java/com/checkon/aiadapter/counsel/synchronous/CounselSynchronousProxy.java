package com.checkon.aiadapter.counsel.synchronous;

import org.springframework.http.ResponseEntity;

interface CounselSynchronousProxy {

	ResponseEntity<String> suggestLabels(String payload, Headers headers);

	ResponseEntity<String> confirm(String payload, Headers headers);

	record Headers(String tenantAlias, String requestId) {
	}
}
