package com.checkon.aiadapter.common.kafka;

import java.security.SecureRandom;
import java.time.Clock;
import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
public class UuidV7Generator {

	private final Clock clock;
	private final SecureRandom random = new SecureRandom();

	public UuidV7Generator(Clock clock) {
		this.clock = clock;
	}

	public UUID next() {
		long unixMillis = clock.millis() & 0x0000FFFFFFFFFFFFL;
		long randomA = random.nextInt(1 << 12);
		long randomB = random.nextLong() & 0x3FFFFFFFFFFFFFFFL;
		long mostSignificant = (unixMillis << 16) | 0x7000L | randomA;
		long leastSignificant = 0x8000000000000000L | randomB;
		return new UUID(mostSignificant, leastSignificant);
	}
}
