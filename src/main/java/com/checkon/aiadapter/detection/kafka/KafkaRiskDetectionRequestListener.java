package com.checkon.aiadapter.detection.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.detection.application.RiskDetectionRequestHandler;

@Component
@ConditionalOnProperty(
	prefix = "checkon.kafka.risk-detection",
	name = "enabled",
	havingValue = "true"
)
public class KafkaRiskDetectionRequestListener {

	private final RiskDetectionRequestedEventDecoder decoder;
	private final RiskDetectionRequestHandler handler;

	public KafkaRiskDetectionRequestListener(
		RiskDetectionRequestedEventDecoder decoder,
		RiskDetectionRequestHandler handler
	) {
		this.decoder = decoder;
		this.handler = handler;
	}

	@RetryableTopic(
		attempts = "3",
		backOff = @BackOff(delay = 1_000, multiplier = 2.0),
		dltTopicSuffix = ".dlt",
		autoCreateTopics = "false",
		exclude = InvalidRiskDetectionRequestException.class
	)
	@KafkaListener(
		topics = "${checkon.kafka.risk-detection.requested-topic}",
		groupId = "${checkon.kafka.risk-detection.consumer-group-id}"
	)
	public void consume(ConsumerRecord<String, String> record) {
		RiskDetectionRequestedEvent event = decoder.decode(record.value());
		try {
			RiskDetectionRequestValidator.validate(record.key(), event);
		}
		catch (IllegalArgumentException exception) {
			throw new InvalidRiskDetectionRequestException(
				"Risk detection request contract is invalid: " + exception.getMessage(),
				exception
			);
		}
		handler.handle(event);
	}
}
