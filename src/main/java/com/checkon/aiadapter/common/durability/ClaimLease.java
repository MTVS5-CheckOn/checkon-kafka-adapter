package com.checkon.aiadapter.common.durability;

public record ClaimLease(long version,boolean staleReclaim) {
	public ClaimLease { if(version<1) throw new IllegalArgumentException("claim version must be positive"); }
}
