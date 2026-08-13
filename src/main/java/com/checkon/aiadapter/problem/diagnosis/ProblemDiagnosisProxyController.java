package com.checkon.aiadapter.problem.diagnosis;

import java.util.regex.Pattern;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/v1/problem-diagnoses")
class ProblemDiagnosisProxyController {
	private static final Pattern TENANT = Pattern.compile("tn_[0-9a-f]{32}");
	private static final Pattern SAFE = Pattern.compile("[A-Za-z0-9._:-]{8,200}");
	private final ProblemDiagnosisProxy proxy;

	ProblemDiagnosisProxyController(ProblemDiagnosisProxy proxy) {
		this.proxy = proxy;
	}

	@PostMapping
	ResponseEntity<String> diagnose(@RequestHeader("X-Tenant-Id") String tenant,
		@RequestHeader("X-Request-Id") String requestId,
		@RequestHeader("Idempotency-Key") String idempotencyKey,
		@RequestBody String payload) {
		if (!TENANT.matcher(tenant).matches()
			|| !SAFE.matcher(requestId).matches()
			|| !SAFE.matcher(idempotencyKey).matches()
			|| payload == null
			|| payload.isBlank()) {
			return ResponseEntity.badRequest().body("{\"error\":{\"code\":\"INVALID_REQUEST\"}}");
		}
		return proxy.diagnose(payload,
			new ProblemDiagnosisProxy.Headers(tenant, requestId, idempotencyKey));
	}
}
