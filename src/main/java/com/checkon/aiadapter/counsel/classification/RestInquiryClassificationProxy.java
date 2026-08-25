package com.checkon.aiadapter.counsel.classification;

import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import com.checkon.aiadapter.counsel.proxy.SynchronousProxyFailureResponse;

final class RestInquiryClassificationProxy implements InquiryClassificationProxy {

	private static final String TENANT_ID_HEADER = "X-Tenant-Id";
	private static final String REQUEST_ID_HEADER = "X-Request-Id";

	private final RestClient restClient;
	private final String classifyPath;

	RestInquiryClassificationProxy(RestClient restClient, String classifyPath) {
		this.restClient = restClient;
		if (classifyPath == null || classifyPath.isBlank() || !classifyPath.startsWith("/")) {
			throw new IllegalArgumentException("classifyPath must start with '/'");
		}
		this.classifyPath = classifyPath;
	}

	@Override
	public ResponseEntity<String> classify(String payload, Headers headers) {
		try {
			return restClient.post()
				.uri(classifyPath)
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
}
