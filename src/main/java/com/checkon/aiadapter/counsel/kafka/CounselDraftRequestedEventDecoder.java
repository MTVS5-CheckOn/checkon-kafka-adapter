package com.checkon.aiadapter.counsel.kafka;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class CounselDraftRequestedEventDecoder {

	private final ObjectMapper objectMapper;

	public CounselDraftRequestedEventDecoder(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public CounselDraftRequestedEvent decode(String rawEvent) {
		if (rawEvent == null || rawEvent.isBlank()) {
			throw new InvalidCounselDraftRequestException(
				"Counsel draft request body is empty", null);
		}
		try {
			return objectMapper.readValue(rawEvent, CounselDraftRequestedEvent.class);
		}
		catch (JacksonException exception) {
			throw new InvalidCounselDraftRequestException(
				"Counsel draft request JSON is invalid", exception);
		}
	}
}
