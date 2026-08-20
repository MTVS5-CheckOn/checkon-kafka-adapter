package com.checkon.aiadapter.counsel.proxy;

import java.util.regex.Pattern;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/counsel/drafts")
class CounselDraftProxyController {

	private static final Pattern TENANT = Pattern.compile("tn_[0-9a-f]{32}");
	private static final Pattern SAFE = Pattern.compile("[A-Za-z0-9._:-]{8,200}");

	private final CounselDraftProxy proxy;

	CounselDraftProxyController(CounselDraftProxy proxy) {
		this.proxy = proxy;
	}

	@GetMapping("/{jobId}")
	ResponseEntity<String> getDraft(
		@PathVariable String jobId,
		@RequestHeader("X-Tenant-Id") String tenant,
		@RequestHeader(value = "X-Request-Id", required = false) String requestId
	) {
		if (!TENANT.matcher(tenant).matches()
			|| !SAFE.matcher(jobId).matches()
			|| (requestId != null && !SAFE.matcher(requestId).matches())) {
			return ResponseEntity.badRequest().body("{\"error\":{\"code\":\"INVALID_REQUEST\"}}");
		}
		return proxy.getDraft(jobId, new CounselDraftProxy.Headers(tenant, requestId, null));
	}

	@PostMapping("/{jobId}/refine")
	ResponseEntity<String> refine(
		@PathVariable String jobId,
		@RequestHeader("X-Tenant-Id") String tenant,
		@RequestHeader(value = "X-Request-Id", required = false) String requestId,
		@RequestHeader("Idempotency-Key") String idempotencyKey,
		@RequestBody String payload
	) {
		if (!TENANT.matcher(tenant).matches()
			|| !SAFE.matcher(jobId).matches()
			|| !SAFE.matcher(idempotencyKey).matches()
			|| (requestId != null && !SAFE.matcher(requestId).matches())
			|| payload == null
			|| payload.isBlank()) {
			return ResponseEntity.badRequest().body("{\"error\":{\"code\":\"INVALID_REQUEST\"}}");
		}
		return proxy.refine(jobId, payload, new CounselDraftProxy.Headers(tenant, requestId, idempotencyKey));
	}
}
