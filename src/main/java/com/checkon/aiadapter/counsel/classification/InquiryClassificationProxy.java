package com.checkon.aiadapter.counsel.classification;

import org.springframework.http.ResponseEntity;

interface InquiryClassificationProxy {

	ResponseEntity<String> classify(String payload, Headers headers);

	record Headers(String tenantAlias, String requestId) {
	}
}
