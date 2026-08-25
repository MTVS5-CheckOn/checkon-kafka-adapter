package com.checkon.aiadapter.problem.ai;

public class AiProblemClientException extends RuntimeException {
	private final boolean transientFailure;
	private final String code;
	private final String detailReason;
	private final Integer currentRevisionNo;

	public AiProblemClientException(String code, boolean transientFailure, Throwable cause) {
		this(code,transientFailure,null,null,cause);
	}

	public AiProblemClientException(String code,boolean transientFailure,String detailReason,
		Integer currentRevisionNo,Throwable cause) {
		super(code, cause);
		this.code = code;
		this.transientFailure = transientFailure;
		this.detailReason=detailReason;
		this.currentRevisionNo=currentRevisionNo;
	}

	public boolean isTransientFailure() { return transientFailure; }
	public String code() { return code; }
	public String detailReason(){return detailReason;}
	public Integer currentRevisionNo(){return currentRevisionNo;}
}
