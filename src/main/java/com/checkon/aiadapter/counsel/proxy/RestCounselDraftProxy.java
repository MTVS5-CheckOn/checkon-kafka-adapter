package com.checkon.aiadapter.counsel.proxy;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

/**
 * GET and refine run on separate {@link RestClient}s with different read
 * timeouts (AI-A 2026-08-20 타임아웃권고 실측) -- GET never runs a job so it
 * only needs a few seconds, but refine is still a synchronous LLM call and
 * needs a much larger budget. A single {@code JdkClientHttpRequestFactory}
 * only supports one read timeout, so two are wired up rather than one.
 */
final class RestCounselDraftProxy implements CounselDraftProxy {

	private static final String TENANT_ID_HEADER = "X-Tenant-Id";
	private static final String REQUEST_ID_HEADER = "X-Request-Id";
	private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

	private final RestClient getRestClient;
	private final RestClient refineRestClient;
	private final String draftsPath;

	RestCounselDraftProxy(RestClient getRestClient, RestClient refineRestClient, String draftsPath) {
		this.getRestClient = getRestClient;
		this.refineRestClient = refineRestClient;
		if (draftsPath == null || draftsPath.isBlank() || !draftsPath.startsWith("/")) {
			throw new IllegalArgumentException("draftsPath must start with '/'");
		}
		this.draftsPath = draftsPath;
	}

	@Override
	public ResponseEntity<String> getDraft(String jobId, Headers headers) {
		try {
			RestClient.RequestHeadersSpec<?> spec = getRestClient.get()
				.uri(draftsPath + "/{jobId}", jobId)
				.header(TENANT_ID_HEADER, headers.tenantAlias());
			if (headers.requestId() != null) spec = spec.header(REQUEST_ID_HEADER, headers.requestId());
			return spec.retrieve().toEntity(String.class);
		}
		catch (RestClientResponseException exception) {
			return ResponseEntity.status(exception.getStatusCode()).body(exception.getResponseBodyAsString());
		}
		catch (RestClientException exception) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body("{\"error\":{\"code\":\"AI_UNAVAILABLE\"}}");
		}
	}

	@Override
	public ResponseEntity<String> refine(String jobId, String payload, Headers headers) {
		try {
			RestClient.RequestBodySpec spec = refineRestClient.post()
				.uri(draftsPath + "/{jobId}/refine", jobId)
				.header(TENANT_ID_HEADER, headers.tenantAlias())
				.header(IDEMPOTENCY_KEY_HEADER, headers.idempotencyKey())
				.header(HttpHeaders.CONTENT_TYPE, "application/json");
			if (headers.requestId() != null) spec = spec.header(REQUEST_ID_HEADER, headers.requestId());
			return spec.body(payload).retrieve().toEntity(String.class);
		}
		catch (RestClientResponseException exception) {
			return ResponseEntity.status(exception.getStatusCode()).body(exception.getResponseBodyAsString());
		}
		catch (RestClientException exception) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body("{\"error\":{\"code\":\"AI_UNAVAILABLE\"}}");
		}
	}
}
