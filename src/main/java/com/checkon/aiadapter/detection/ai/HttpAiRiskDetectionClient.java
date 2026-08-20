package com.checkon.aiadapter.detection.ai;

import java.nio.charset.StandardCharsets;

import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

public class HttpAiRiskDetectionClient implements AiRiskDetectionClient {

	static final String TENANT_ID_HEADER = "X-Tenant-Id";
	static final String REQUEST_ID_HEADER = "X-Request-Id";
	static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

	private final RestClient restClient;
	private final String detectPath;
	private final tools.jackson.databind.ObjectMapper objectMapper;

	public HttpAiRiskDetectionClient(
		RestClient restClient,
		String detectPath,
		tools.jackson.databind.ObjectMapper objectMapper
	) {
		this.restClient = restClient;
		this.objectMapper = objectMapper;
		if (detectPath == null || detectPath.isBlank() || !detectPath.startsWith("/")) {
			throw new IllegalArgumentException("detectPath must start with '/'");
		}
		this.detectPath = detectPath;
	}

	@Override
	public AiDetectionResponse detect(
		AiDetectionRequest request,
		AiDetectionRequestHeaders headers
	) {
		try {
			return detectRaw(objectMapper.writeValueAsString(request), headers);
		}
		catch (tools.jackson.core.JacksonException exception) {
			throw new IllegalArgumentException("AI request cannot be serialized", exception);
		}
	}

	@Override
	public AiDetectionResponse detectRaw(
		String requestBody,
		AiDetectionRequestHeaders headers
	) {
		Timer.Sample sample=Timer.start(Metrics.globalRegistry);
		try {
			long contentLength = requestBody.getBytes(StandardCharsets.UTF_8).length;
			AiDetectionResponse response = restClient.post()
				.uri(detectPath)
				.header(TENANT_ID_HEADER, headers.tenantAlias())
				.header(REQUEST_ID_HEADER, headers.requestId())
				.header(IDEMPOTENCY_KEY_HEADER, headers.idempotencyKey())
				.contentType(MediaType.APPLICATION_JSON)
				.contentLength(contentLength)
				.body(requestBody)
				.retrieve()
				.body(AiDetectionResponse.class);

			if (response == null) {
				throw AiRiskDetectionClientException.emptyResponse();
			}
			return response;
		}
		catch (RestClientResponseException exception) {
			int status = exception.getStatusCode().value();
			String responseBody = exception.getResponseBodyAsString();
			if (status == 409) {
				throw AiRiskDetectionClientException.idempotencyConflict(
					responseBody, exception);
			}
			throw AiRiskDetectionClientException.httpError(
				status, responseBody, exception);
		}
		catch (RestClientException exception) {
			throw AiRiskDetectionClientException.networkError(exception);
		}
		finally { sample.stop(Metrics.timer("checkon.ai.http.duration","feature","risk_detection","operation","detect")); }
	}
}
