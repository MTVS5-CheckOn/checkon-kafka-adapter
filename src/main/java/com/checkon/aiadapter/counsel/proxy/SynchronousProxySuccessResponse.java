package com.checkon.aiadapter.counsel.proxy;

import org.springframework.http.ResponseEntity;

public final class SynchronousProxySuccessResponse {

	private SynchronousProxySuccessResponse() {
	}

	public static ResponseEntity<String> from(ResponseEntity<String> upstream) {
		var response = ResponseEntity.status(upstream.getStatusCode());
		if (upstream.getHeaders().getContentType() != null) {
			response.contentType(upstream.getHeaders().getContentType());
		}
		return response.body(upstream.getBody());
	}
}
