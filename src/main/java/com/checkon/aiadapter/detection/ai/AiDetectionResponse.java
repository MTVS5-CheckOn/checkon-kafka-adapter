package com.checkon.aiadapter.detection.ai;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AiDetectionResponse(
	Data data,
	ApiError error,
	Meta meta
) {

	public record Data(List<Signal> signals, Stats stats) {
	}

	public record Signal(
		@JsonProperty("signal_id") String signalId,
		@JsonProperty("student_ref") String studentRef,
		@JsonProperty("class_ref") String classRef,
		@JsonProperty("rule_id") String ruleId,
		@JsonProperty("signal_type") String signalType,
		@JsonProperty("display_label") String displayLabel,
		double score,
		int rank,
		Boolean advisory,
		String lifecycle,
		Brief brief,
		List<Evidence> evidence
	) {
	}

	public record Brief(
		String text,
		@JsonProperty("gate_passed") boolean gatePassed,
		@JsonProperty("fallback_used") boolean fallbackUsed
	) {
	}

	public record Evidence(
		@JsonProperty("source_table") String sourceTable,
		@JsonProperty("record_id") String recordId,
		String summary
	) {
	}

	public record Stats(
		@JsonProperty("students_evaluated") int studentsEvaluated,
		@JsonProperty("signals_raised") int signalsRaised,
		@JsonProperty("excluded_under_2w") int excludedUnderTwoWeeks,
		@JsonProperty("capped_out") int cappedOut,
		@JsonProperty("rules_skipped") List<SkippedRule> rulesSkipped,
		@JsonProperty("r1_threshold_pp") BigDecimal r1ThresholdPp,
		@JsonProperty("r1_threshold_source") String r1ThresholdSource,
		@JsonProperty("r1_pool_n") Integer r1PoolN
	) {
		public Stats(
			int studentsEvaluated,
			int signalsRaised,
			int excludedUnderTwoWeeks,
			int cappedOut,
			List<SkippedRule> rulesSkipped
		) {
			this(studentsEvaluated, signalsRaised, excludedUnderTwoWeeks, cappedOut,
				rulesSkipped, null, null, null);
		}
	}

	public record SkippedRule(
		@JsonProperty("rule_id") String ruleId,
		String reason,
		int students
	) {
	}

	public record ApiError(
		String code,
		String message,
		Map<String, Object> detail
	) {
	}

	public record Meta(
		@JsonProperty("execution_id") String executionId,
		Map<String, String> versions
	) {
	}
}
