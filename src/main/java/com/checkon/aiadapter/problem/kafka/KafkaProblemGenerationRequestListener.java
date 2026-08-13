package com.checkon.aiadapter.problem.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.problem.application.ProblemGenerationRequestConflictException;
import com.checkon.aiadapter.problem.application.ProblemGenerationRequestHandler;

@Component
@ConditionalOnProperty(prefix = "checkon.kafka.problem-generation", name = "enabled", havingValue = "true")
public class KafkaProblemGenerationRequestListener {
	private final ProblemGenerationRequestDecoder decoder;
	private final ProblemGenerationRequestHandler handler;

	public KafkaProblemGenerationRequestListener(ProblemGenerationRequestDecoder decoder,
		ProblemGenerationRequestHandler handler) {
		this.decoder = decoder;
		this.handler = handler;
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
		ProblemGenerationRequestedEvent event = decoder.decode(record.key(), record.value());
		handler.handle(event, record.value());
	}
}
