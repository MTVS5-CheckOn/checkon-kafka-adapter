package com.checkon.aiadapter.counsel.synchronous;

import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import com.checkon.aiadapter.counsel.proxy.SynchronousProxyFailureResponse;

final class RestCounselSynchronousProxy implements CounselSynchronousProxy {

	private static final String TENANT_ID_HEADER = "X-Tenant-Id";
	private static final String REQUEST_ID_HEADER = "X-Request-Id";

	private final RestClient classificationRestClient;
	private final RestClient labelsRestClient;
	private final String labelSuggestPath;
	private final String classificationConfirmationsPath;
	private final String labelConfirmationsPath;

	RestCounselSynchronousProxy(
		RestClient classificationRestClient,
		String classificationConfirmationsPath,
		RestClient labelsRestClient,
		String labelSuggestPath,
		String labelConfirmationsPath
	) {
		this.classificationRestClient = classificationRestClient;
		this.labelsRestClient = labelsRestClient;
		this.labelSuggestPath = requirePath(labelSuggestPath, "labelSuggestPath");
		this.classificationConfirmationsPath = requirePath(
			classificationConfirmationsPath, "classificationConfirmationsPath");
		this.labelConfirmationsPath = requirePath(labelConfirmationsPath, "labelConfirmationsPath");
	}

	@Override
	public ResponseEntity<String> suggestLabels(String payload, Headers headers) {
		return post(labelsRestClient, labelSuggestPath, payload, headers);
	}

	@Override
	public ResponseEntity<String> confirmClassification(String payload, Headers headers) {
		return post(classificationRestClient, classificationConfirmationsPath, payload, headers);
	}

	@Override
	public ResponseEntity<String> confirmLabel(String payload, Headers headers) {
		return post(labelsRestClient, labelConfirmationsPath, payload, headers);
	}

	private ResponseEntity<String> post(RestClient restClient, String path, String payload, Headers headers) {
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
			return SynchronousProxyFailureResponse.from(exception);
		}
	}

	private static String requirePath(String path, String name) {
		if (path == null || path.isBlank() || !path.startsWith("/")) {
			throw new IllegalArgumentException(name + " must start with '/'");
		}
		return path;
	}
}
