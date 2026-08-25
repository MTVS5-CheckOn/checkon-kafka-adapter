package com.checkon.aiadapter.counsel.synchronous;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

final class RestCounselSynchronousProxy implements CounselSynchronousProxy {

	private static final String TENANT_ID_HEADER = "X-Tenant-Id";
	private static final String REQUEST_ID_HEADER = "X-Request-Id";

	private final RestClient restClient;
	private final String labelSuggestPath;
	private final String confirmationsPath;

	RestCounselSynchronousProxy(RestClient restClient, String labelSuggestPath, String confirmationsPath) {
		this.restClient = restClient;
		this.labelSuggestPath = requirePath(labelSuggestPath, "labelSuggestPath");
		this.confirmationsPath = requirePath(confirmationsPath, "confirmationsPath");
	}

	@Override
	public ResponseEntity<String> suggestLabels(String payload, Headers headers) {
		return post(labelSuggestPath, payload, headers);
	}

	@Override
	public ResponseEntity<String> confirm(String payload, Headers headers) {
		return post(confirmationsPath, payload, headers);
	}

	private ResponseEntity<String> post(String path, String payload, Headers headers) {
		try {
			return restClient.post()
				.uri(path)
				.header(TENANT_ID_HEADER, headers.tenantAlias())
				.header(REQUEST_ID_HEADER, headers.requestId())
				.header(HttpHeaders.CONTENT_TYPE, "application/json")
				.body(payload)
				.retrieve()
				.toEntity(String.class);
		}
		catch (RestClientResponseException exception) {
			return ResponseEntity.status(exception.getStatusCode()).body(exception.getResponseBodyAsString());
		}
		catch (RestClientException exception) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
				.body("{\"error\":{\"code\":\"AI_UNAVAILABLE\"}}");
		}
	}

	private static String requirePath(String path, String name) {
		if (path == null || path.isBlank() || !path.startsWith("/")) {
			throw new IllegalArgumentException(name + " must start with '/'");
		}
		return path;
	}
}
