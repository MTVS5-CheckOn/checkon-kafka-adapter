package com.checkon.aiadapter.problem.diagnosis;

import java.nio.charset.StandardCharsets;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

final class RestProblemDiagnosisProxy implements ProblemDiagnosisProxy {
	private final RestClient client;
	private final String path;

	RestProblemDiagnosisProxy(RestClient client, String path) {
		this.client = client;
		if (path == null || !path.startsWith("/")) {
			throw new IllegalArgumentException("diagnosisPath must start with '/'");
		}
		this.path = path;
	}

	@Override
	public ResponseEntity<String> diagnose(String payload, Headers headers) {
		try {
			return client.post()
				.uri(path)
				.header("X-Tenant-Id", headers.tenantAlias())
				.header("X-Request-Id", headers.requestId())
				.header("Idempotency-Key", headers.idempotencyKey())
				.header(HttpHeaders.CONTENT_TYPE, "application/json")
				.body(payload.getBytes(StandardCharsets.UTF_8))
				.retrieve()
				.toEntity(String.class);
		}
		catch (RestClientResponseException exception) {
			return ResponseEntity.status(exception.getStatusCode())
				.body(exception.getResponseBodyAsString());
		}
		catch (RestClientException exception) {
			return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
				.body("{\"error\":{\"code\":\"AI_UNAVAILABLE\"}}");
		}
	}
}
