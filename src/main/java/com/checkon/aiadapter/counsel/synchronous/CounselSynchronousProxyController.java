package com.checkon.aiadapter.counsel.synchronous;

import java.util.regex.Pattern;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
class CounselSynchronousProxyController {

	private static final Pattern TENANT_ALIAS = Pattern.compile("tn_[0-9a-f]{32}");
	private static final Pattern SAFE_REQUEST_ID = Pattern.compile("[A-Za-z0-9._:-]{8,200}");
	private static final String INVALID_REQUEST = "{\"error\":{\"code\":\"INVALID_REQUEST\"}}";

	private final CounselSynchronousProxy proxy;

	CounselSynchronousProxyController(CounselSynchronousProxy proxy) {
		this.proxy = proxy;
	}

	@PostMapping("/v1/labels/suggest")
	ResponseEntity<String> suggestLabels(
		@RequestHeader("X-Tenant-Id") String tenantAlias,
		@RequestHeader("X-Request-Id") String requestId,
		@RequestBody String payload
	) {
		if (!valid(tenantAlias, requestId, payload)) {
			return ResponseEntity.badRequest().body(INVALID_REQUEST);
		}
		return proxy.suggestLabels(payload, new CounselSynchronousProxy.Headers(tenantAlias, requestId));
	}

	@PostMapping("/v1/confirmations")
	ResponseEntity<String> confirm(
		@RequestHeader("X-Tenant-Id") String tenantAlias,
		@RequestHeader("X-Request-Id") String requestId,
		@RequestBody String payload
	) {
		if (!valid(tenantAlias, requestId, payload)) {
			return ResponseEntity.badRequest().body(INVALID_REQUEST);
		}
		return proxy.confirm(payload, new CounselSynchronousProxy.Headers(tenantAlias, requestId));
	}

	private static boolean valid(String tenantAlias, String requestId, String payload) {
		return TENANT_ALIAS.matcher(tenantAlias).matches()
			&& SAFE_REQUEST_ID.matcher(requestId).matches()
			&& payload != null
			&& !payload.isBlank();
	}
}
