package com.checkon.aiadapter.problem.ai;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public class HttpAiProblemClient implements AiProblemClient {
	private final RestClient restClient;
	private final String path;
	private final ObjectMapper objectMapper;

	public HttpAiProblemClient(RestClient restClient, String path, ObjectMapper objectMapper) {
		this.restClient = restClient;
		this.path = path;
		this.objectMapper = objectMapper;
	}

	@Override
	public JsonNode submit(String requestBody, Headers headers) {
		return exchange(() -> restClient.post().uri(path)
			.headers(http -> headers(http, headers, true))
			.contentType(MediaType.APPLICATION_JSON)
			.contentLength(requestBody.getBytes(StandardCharsets.UTF_8).length)
			.body(requestBody).retrieve().body(JsonNode.class));
	}

	@Override
	public JobResponse job(String jobId, Headers headers) {
		try {
			var response=restClient.get().uri(path + "/{jobId}", jobId)
				.headers(http -> headers(http, headers, false)).retrieve().toEntity(JsonNode.class);
			JsonNode body=validate(response.getBody());
			return new JobResponse(body,retryAfter(response.getHeaders().getFirst("Retry-After")));
		}
		catch (RestClientResponseException exception) { throw translated(exception); }
		catch (RestClientException exception) { throw new AiProblemClientException("AI_NETWORK_ERROR",true,exception); }
	}

	@Override
	public JsonNode items(String setId, Headers headers) {
		return exchange(() -> restClient.get().uri(path + "/{setId}/items", setId)
			.headers(http -> headers(http, headers, false)).retrieve().body(JsonNode.class));
	}

	@Override
	public JsonNode item(String setId,int slotIndex,Headers headers) {
		return exchange(() -> restClient.get().uri(path+"/{setId}/items/{slotIndex}",setId,slotIndex)
			.headers(http->headers(http,headers,false)).retrieve().body(JsonNode.class));
	}

	private static void headers(org.springframework.http.HttpHeaders http, Headers value, boolean idempotent) {
		http.set("X-Tenant-Id", value.tenantAlias());
		http.set("X-Request-Id", value.requestId());
		if (idempotent) http.set("Idempotency-Key", value.idempotencyKey());
	}

	private JsonNode exchange(java.util.concurrent.Callable<JsonNode> call) {
		try {
			JsonNode response = call.call();
			return validate(response);
		}
		catch (AiProblemClientException exception) { throw exception; }
		catch (RestClientResponseException exception) {
			throw translated(exception);
		}
		catch (RestClientException exception) {
			throw new AiProblemClientException("AI_NETWORK_ERROR", true, exception);
		}
		catch (Exception exception) {
			throw new AiProblemClientException("AI_CLIENT_ERROR", false, exception);
		}
	}
	private static JsonNode validate(JsonNode response) { if(response==null||!response.isObject()) throw new AiProblemClientException("AI_EMPTY_RESPONSE",true,null); return response; }
	private AiProblemClientException translated(RestClientResponseException exception) { int status=exception.getStatusCode().value();
		return new AiProblemClientException(errorCode(status,exception.getResponseBodyAsString()),status==408||status==429||status>=500,exception); }
	private static Duration retryAfter(String value) { if(value==null||value.isBlank()) return null;
		try { long seconds=Long.parseLong(value.trim()); return seconds>0?Duration.ofSeconds(seconds):null; }
		catch(NumberFormatException ignored){ return null; } }

	private String errorCode(int status, String responseBody) {
		if (status == 409) return "IDEMPOTENCY_CONFLICT";
		if (status == 404) {
			try {
				JsonNode reason = objectMapper.readTree(responseBody).path("error").path("detail").get("reason");
				if (reason != null && reason.isTextual()
					&& "result_unavailable_after_restart".equals(reason.asText())) {
					return reason.asText();
				}
			}
			catch (RuntimeException ignored) {
				// Fall through to the stable HTTP status code for malformed or non-JSON error bodies.
			}
		}
		return "AI_HTTP_" + status;
	}
}
