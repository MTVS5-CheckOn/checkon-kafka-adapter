package com.checkon.aiadapter.problem.kafka;

import java.time.Instant;
import java.util.UUID;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

@Component
public class ProblemGenerationRevisionRequestDecoder {
	private static final Pattern TENANT=Pattern.compile("tn_[0-9a-f]{32}");
	private static final Pattern IDEMPOTENCY=Pattern.compile("[A-Za-z0-9._:-]{8,200}");
	private final ObjectMapper objectMapper;

	public ProblemGenerationRevisionRequestDecoder(ObjectMapper objectMapper){this.objectMapper=objectMapper;}

	public ProblemGenerationRevisionRequestedEvent decode(String messageKey,String rawPayload){
		try {
			JsonNode root=objectMapper.readTree(rawPayload);
			if(!"problem_generation.revision.requested".equals(text(root,"event_type"))) throw invalid("event_type must be problem_generation.revision.requested");
			if(!"pg-revision-request-1".equals(text(root,"schema_version"))) throw invalid("schema_version must be pg-revision-request-1");
			String tenant=text(root,"tenant_id");
			if(!TENANT.matcher(tenant).matches()||!tenant.equals(messageKey)) throw invalid("Kafka key and tenant_id must match");
			JsonNode payload=object(root,"payload"); UUID requestId=uuid(payload,"request_id");
			if(!requestId.equals(uuid(root,"correlation_id"))) throw invalid("correlation_id and request_id must match");
			int slot=integer(payload,"slot_index"); int base=integer(payload,"base_revision_no");
			if(slot<0||base<0) throw invalid("slot_index and base_revision_no must be non-negative");
			if(!"ai_refine".equals(text(payload,"revision_kind"))) throw invalid("revision_kind must be ai_refine");
			String instruction=text(payload,"instruction"); if(instruction.length()>2000) throw invalid("instruction is too long");
			String idem=text(payload,"idempotency_key"); if(!IDEMPOTENCY.matcher(idem).matches()) throw invalid("idempotency_key must be safe ASCII");
			ObjectNode request=objectMapper.createObjectNode(); request.put("base_revision_no",base);
			request.put("revision_kind","ai_refine"); request.put("instruction",instruction);
			return new ProblemGenerationRevisionRequestedEvent(uuid(root,"event_id"),Instant.parse(text(root,"occurred_at")),tenant,
				requestId,uuid(payload,"problem_execution_id"),uuid(payload,"revision_request_id"),
				text(payload,"set_id"),slot,idem,request);
		}
		catch(InvalidProblemGenerationRequestException exception){throw exception;}
		catch(JacksonException|IllegalArgumentException exception){throw new InvalidProblemGenerationRequestException("Problem revision request is invalid",exception);}
	}

	private static JsonNode object(JsonNode node,String field){JsonNode value=node==null?null:node.get(field);if(value==null||!value.isObject())throw invalid(field+" must be an object");return value;}
	private static String text(JsonNode node,String field){JsonNode value=node==null?null:node.get(field);if(value==null||!value.isTextual()||value.asText().isBlank())throw invalid(field+" must not be blank");return value.asText().trim();}
	private static UUID uuid(JsonNode node,String field){return UUID.fromString(text(node,field));}
	private static int integer(JsonNode node,String field){JsonNode value=node==null?null:node.get(field);if(value==null||!value.canConvertToInt())throw invalid(field+" must be an integer");return value.asInt();}
	private static InvalidProblemGenerationRequestException invalid(String message){return new InvalidProblemGenerationRequestException(message);}
}
