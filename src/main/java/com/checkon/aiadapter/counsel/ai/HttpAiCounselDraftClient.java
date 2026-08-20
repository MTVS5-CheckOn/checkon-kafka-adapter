package com.checkon.aiadapter.counsel.ai;

import java.nio.charset.StandardCharsets;

import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

public class HttpAiCounselDraftClient implements AiCounselDraftClient {

	static final String TENANT_ID_HEADER = "X-Tenant-Id";
	static final String REQUEST_ID_HEADER = "X-Request-Id";
	static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

	private final RestClient restClient;
	private final String draftsPath;
	private final tools.jackson.databind.ObjectMapper objectMapper;

	public HttpAiCounselDraftClient(
		RestClient restClient,
		String draftsPath,
		tools.jackson.databind.ObjectMapper objectMapper
	) {
		this.restClient = restClient;
		this.objectMapper = objectMapper;
		if (draftsPath == null || draftsPath.isBlank() || !draftsPath.startsWith("/")) {
			throw new IllegalArgumentException("draftsPath must start with '/'");
		}
		this.draftsPath = draftsPath;
	}

	@Override
	public AiCounselDraftResponse createDraft(
		AiCounselDraftRequest request,
		AiCounselDraftRequestHeaders headers
	) {
		try {
			return createDraftRaw(objectMapper.writeValueAsString(request), headers);
		}
		catch (tools.jackson.core.JacksonException exception) {
			throw new IllegalArgumentException("AI request cannot be serialized", exception);
		}
	}

	@Override
	public AiCounselDraftResponse createDraftRaw(
		String requestBody,
		AiCounselDraftRequestHeaders headers
	) {
		Timer.Sample sample = Timer.start(Metrics.globalRegistry);
		try {
			long contentLength = requestBody.getBytes(StandardCharsets.UTF_8).length;
			AiCounselDraftResponse response = restClient.post()
				.uri(draftsPath)
				.header(TENANT_ID_HEADER, headers.tenantAlias())
				.header(REQUEST_ID_HEADER, headers.requestId())
				.header(IDEMPOTENCY_KEY_HEADER, headers.idempotencyKey())
				.contentType(MediaType.APPLICATION_JSON)
				.contentLength(contentLength)
				.body(requestBody)
				.retrieve()
				.body(AiCounselDraftResponse.class);

			if (response == null) {
				throw AiCounselDraftClientException.emptyResponse();
			}
			return response;
		}
		catch (RestClientResponseException exception) {
			int status = exception.getStatusCode().value();
			String responseBody = exception.getResponseBodyAsString();
			if (status == 409) {
				throw AiCounselDraftClientException.idempotencyConflict(responseBody, exception);
			}
			throw AiCounselDraftClientException.httpError(status, responseBody, exception);
		}
		catch (RestClientException exception) {
			throw AiCounselDraftClientException.networkError(exception);
		}
		finally {
			sample.stop(Metrics.timer("checkon.ai.http.duration", "feature", "counsel_draft", "operation", "create_draft"));
		}
	}
}
