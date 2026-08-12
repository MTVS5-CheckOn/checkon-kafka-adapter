package com.checkon.aiadapter.detection.kafka;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class RiskDetectionRequestedEventDecoder {

	private final ObjectMapper objectMapper;

	public RiskDetectionRequestedEventDecoder(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public RiskDetectionRequestedEvent decode(String rawEvent) {
		if (rawEvent == null || rawEvent.isBlank()) {
			throw new InvalidRiskDetectionRequestException(
				"Risk detection request body is empty", null);
		}
		try {
			return objectMapper.readValue(rawEvent, RiskDetectionRequestedEvent.class);
		}
		catch (JacksonException exception) {
			throw new InvalidRiskDetectionRequestException(
				"Risk detection request JSON is invalid", exception);
		}
	}
}
