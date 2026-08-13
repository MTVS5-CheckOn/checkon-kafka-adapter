package com.checkon.aiadapter.problem.kafka;

import java.time.Instant;
import java.util.UUID;

import tools.jackson.databind.JsonNode;

public record ProblemGenerationRequestedEvent(
	UUID eventId,
	Instant occurredAt,
	String tenantAlias,
	UUID problemRequestId,
	UUID problemExecutionId,
	int targetIndex,
	String idempotencyKey,
	JsonNode request
) { }
