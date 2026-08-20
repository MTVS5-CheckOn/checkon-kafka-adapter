package com.checkon.aiadapter.common.durability;

import java.time.Duration;

public final class RetryPolicy {
	private RetryPolicy() { }
	public static Duration exponential(Duration initialDelay,int completedAttempts) {
		if(initialDelay==null||initialDelay.isZero()||initialDelay.isNegative()) throw new IllegalArgumentException("initial delay must be positive");
		return initialDelay.multipliedBy(1L<<Math.min(Math.max(completedAttempts-1,0),6));
	}
}
