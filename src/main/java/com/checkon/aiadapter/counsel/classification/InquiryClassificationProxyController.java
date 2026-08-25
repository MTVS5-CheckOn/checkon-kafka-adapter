package com.checkon.aiadapter.counsel.classification;

import java.util.regex.Pattern;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
class InquiryClassificationProxyController {

	private static final Pattern TENANT = Pattern.compile("tn_[0-9a-f]{32}");
	private static final Pattern SAFE_REQUEST_ID = Pattern.compile("[A-Za-z0-9._:-]{8,200}");

	private final InquiryClassificationProxy proxy;

	InquiryClassificationProxyController(InquiryClassificationProxy proxy) {
		this.proxy = proxy;
	}

	@PostMapping("/v1/classify")
	ResponseEntity<String> classify(
		@RequestHeader("X-Tenant-Id") String tenantAlias,
		@RequestHeader("X-Request-Id") String requestId,
		@RequestBody String payload
	) {
		if (!TENANT.matcher(tenantAlias).matches()
			|| !SAFE_REQUEST_ID.matcher(requestId).matches()
			|| payload == null
			|| payload.isBlank()) {
			return ResponseEntity.badRequest().body("{\"error\":{\"code\":\"INVALID_REQUEST\"}}");
		}
		return proxy.classify(payload, new InquiryClassificationProxy.Headers(tenantAlias, requestId));
	}
}
