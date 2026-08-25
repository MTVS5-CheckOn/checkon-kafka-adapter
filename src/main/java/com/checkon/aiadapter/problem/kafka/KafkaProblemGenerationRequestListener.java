package com.checkon.aiadapter.problem.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.problem.application.ProblemGenerationRequestConflictException;
import com.checkon.aiadapter.problem.application.ProblemGenerationRequestHandler;
import com.checkon.aiadapter.problem.application.ProblemGenerationRevisionRequestHandler;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
@ConditionalOnProperty(prefix = "checkon.kafka.problem-generation", name = "enabled", havingValue = "true")
public class KafkaProblemGenerationRequestListener {
	private final ProblemGenerationRequestDecoder decoder;
	private final ProblemGenerationRequestHandler handler;
	private final ProblemGenerationRevisionRequestDecoder revisionDecoder;
	private final ProblemGenerationRevisionRequestHandler revisionHandler;
	private final ObjectMapper objectMapper;

	public KafkaProblemGenerationRequestListener(ProblemGenerationRequestDecoder decoder,
		ProblemGenerationRequestHandler handler,ProblemGenerationRevisionRequestDecoder revisionDecoder,
		ProblemGenerationRevisionRequestHandler revisionHandler,ObjectMapper objectMapper) {
		this.decoder = decoder;
		this.handler = handler;
		this.revisionDecoder=revisionDecoder; this.revisionHandler=revisionHandler; this.objectMapper=objectMapper;
	}

	@RetryableTopic(
		attempts = "3",
		backOff = @BackOff(delay = 1_000, multiplier = 2.0),
		dltTopicSuffix = ".dlt",
		autoCreateTopics = "false",
		exclude = {InvalidProblemGenerationRequestException.class, ProblemGenerationRequestConflictException.class}
	)
	@KafkaListener(
		topics = "${checkon.kafka.problem-generation.request-topic}",
		groupId = "${checkon.kafka.problem-generation.consumer-group-id}"
	)
	public void consume(ConsumerRecord<String, String> record) {
		if("problem_generation.revision.requested".equals(eventType(record.value()))) {
			revisionHandler.handle(revisionDecoder.decode(record.key(),record.value()),record.value()); return;
		}
		ProblemGenerationRequestedEvent event=decoder.decode(record.key(),record.value()); handler.handle(event,record.value());
	}

	private String eventType(String payload){try{return objectMapper.readTree(payload).path("event_type").asText();}
		catch(JacksonException exception){throw new InvalidProblemGenerationRequestException("event payload is invalid",exception);}}
}
