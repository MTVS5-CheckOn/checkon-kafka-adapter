package com.checkon.aiadapter.problem.kafka;

import java.time.Instant;
import java.util.UUID;

import tools.jackson.databind.JsonNode;

public record ProblemGenerationRevisionRequestedEvent(
	UUID eventId,
	Instant occurredAt,
	String tenantAlias,
	UUID problemRequestId,
	UUID problemExecutionId,
	UUID revisionRequestId,
	String setId,
	int slotIndex,
	String idempotencyKey,
	JsonNode request
) { }
