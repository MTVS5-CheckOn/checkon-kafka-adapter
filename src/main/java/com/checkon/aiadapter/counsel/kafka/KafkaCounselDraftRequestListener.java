package com.checkon.aiadapter.counsel.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

import com.checkon.aiadapter.counsel.application.CounselDraftRequestConflictException;
import com.checkon.aiadapter.counsel.application.CounselDraftRequestHandler;

@Component
@ConditionalOnProperty(
	prefix = "checkon.kafka.counsel-draft",
	name = "enabled",
	havingValue = "true"
)
public class KafkaCounselDraftRequestListener {

	private final CounselDraftRequestedEventDecoder decoder;
	private final CounselDraftRequestHandler handler;

	public KafkaCounselDraftRequestListener(
		CounselDraftRequestedEventDecoder decoder,
		CounselDraftRequestHandler handler
	) {
		this.decoder = decoder;
		this.handler = handler;
	}

	@RetryableTopic(
		attempts = "3",
		backOff = @BackOff(delay = 1_000, multiplier = 2.0),
		dltTopicSuffix = ".dlt",
		autoCreateTopics = "false",
		exclude = {
			InvalidCounselDraftRequestException.class,
			CounselDraftRequestConflictException.class
		}
	)
	@KafkaListener(
		topics = "${checkon.kafka.counsel-draft.requested-topic}",
		groupId = "${checkon.kafka.counsel-draft.consumer-group-id}"
	)
	public void consume(ConsumerRecord<String, String> record) {
		CounselDraftRequestedEvent event = decoder.decode(record.value());
		try {
			CounselDraftRequestValidator.validate(record.key(), event);
		}
		catch (IllegalArgumentException exception) {
			throw new InvalidCounselDraftRequestException(
				"Counsel draft request contract is invalid: " + exception.getMessage(),
				exception
			);
		}
		handler.handle(event, record.value());
	}
}
