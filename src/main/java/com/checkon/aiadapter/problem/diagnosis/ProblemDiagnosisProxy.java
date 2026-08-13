package com.checkon.aiadapter.problem.diagnosis;

import org.springframework.http.ResponseEntity;

public interface ProblemDiagnosisProxy {
	ResponseEntity<String> diagnose(String payload,Headers headers);
	record Headers(String tenantAlias,String requestId,String idempotencyKey) { }
}
