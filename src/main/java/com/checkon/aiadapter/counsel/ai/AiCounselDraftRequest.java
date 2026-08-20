package com.checkon.aiadapter.counsel.ai;

import java.time.OffsetDateTime;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for {@code POST /v1/counsel/drafts}. Field shape is a
 * wire-compatible mirror of {@code CounselDraftCreateRequest} in
 * CheckOn-backend — the same JSON travels backend -> Kafka -> adapter -> AI
 * without reshaping.
 */
public record AiCounselDraftRequest(
	Inquiry inquiry,
	@JsonProperty("student_ref") String studentRef,
	@JsonProperty("parent_ref") String parentRef,
	@JsonProperty("class_ref") String classRef,
	List<String> labels,
	@JsonProperty("dismissed_suggestions") List<DismissedSuggestion> dismissedSuggestions,
	Context context
) {

	public record Inquiry(
		@JsonProperty("inquiry_ref") String inquiryRef,
		String topic,
		String urgency,
		@JsonProperty("received_at") OffsetDateTime receivedAt,
		@JsonProperty("text_masked") String textMasked
	) {
	}

	public record DismissedSuggestion(String axis, String value) {
	}

	public record Context(
		@JsonProperty("snapshot_hash") String snapshotHash,
		@JsonProperty("period_label") String periodLabel,
		List<Fact> facts
	) {
	}

	public record Fact(
		@JsonProperty("record_id") String recordId,
		String summary
	) {
	}
}
