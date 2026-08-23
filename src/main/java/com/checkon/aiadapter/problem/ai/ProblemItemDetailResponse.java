package com.checkon.aiadapter.problem.ai;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.JsonNode;

public record ProblemItemDetailResponse(Data data) {
	public record Data(@JsonProperty("set_id") String setId,@JsonProperty("slot_index") Integer slotIndex,
		@JsonProperty("item_id") String itemId,String status,JsonNode item,JsonNode verification,
		@JsonProperty("current_revision_no") Integer currentRevisionNo,
		@JsonProperty("available_actions") JsonNode availableActions,
		JsonNode revisions,@JsonProperty("review_reason") String reviewReason,
		@JsonProperty("failure_reason") String failureReason,
		@JsonProperty("failure_detail") JsonNode failureDetail) { }
}
