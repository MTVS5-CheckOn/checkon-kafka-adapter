package com.checkon.aiadapter.detection.ai;

import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

public class HttpAiRiskDetectionClient implements AiRiskDetectionClient {

	static final String TENANT_ID_HEADER = "X-Tenant-Id";
	static final String REQUEST_ID_HEADER = "X-Request-Id";
	static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

	private final RestClient restClient;
	private final String detectPath;

	public HttpAiRiskDetectionClient(RestClient restClient, String detectPath) {
		this.restClient = restClient;
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
			AiDetectionResponse response = restClient.post()
				.uri(detectPath)
				.header(TENANT_ID_HEADER, headers.tenantAlias())
				.header(REQUEST_ID_HEADER, headers.requestId())
				.header(IDEMPOTENCY_KEY_HEADER, headers.idempotencyKey())
				.contentType(MediaType.APPLICATION_JSON)
				.body(request)
				.retrieve()
				.body(AiDetectionResponse.class);

			if (response == null) {
				throw AiRiskDetectionClientException.emptyResponse();
			}
			return response;
		}
		catch (RestClientResponseException exception) {
			int status = exception.getStatusCode().value();
			if (status == 409) {
				throw AiRiskDetectionClientException.idempotencyConflict(exception);
			}
			throw AiRiskDetectionClientException.httpError(status, exception);
		}
		catch (RestClientException exception) {
			throw AiRiskDetectionClientException.networkError(exception);
		}
	}
}
