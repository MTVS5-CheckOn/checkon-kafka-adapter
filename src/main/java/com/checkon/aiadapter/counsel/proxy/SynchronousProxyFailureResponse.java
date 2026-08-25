package com.checkon.aiadapter.counsel.proxy;

import java.net.SocketTimeoutException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;

public final class SynchronousProxyFailureResponse {

	private static final String AI_UNAVAILABLE = "{\"error\":{\"code\":\"AI_UNAVAILABLE\"}}";
	private static final String AI_TIMEOUT = "{\"error\":{\"code\":\"AI_TIMEOUT\"}}";

	private SynchronousProxyFailureResponse() {
	}

	public static ResponseEntity<String> from(RestClientException exception) {
		if (isReadTimeout(exception)) {
			return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).body(AI_TIMEOUT);
		}
		return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(AI_UNAVAILABLE);
	}

	private static boolean isReadTimeout(Throwable exception) {
		for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
			if (cause instanceof HttpConnectTimeoutException) {
				return false;
			}
			if (cause instanceof HttpTimeoutException || cause instanceof SocketTimeoutException) {
				return true;
			}
		}
		return false;
	}
}
