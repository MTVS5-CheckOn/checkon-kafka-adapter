package com.checkon.aiadapter.problem.ai;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.JsonNode;

public record ProblemItemDetailResponse(Data data) {
	public record Data(@JsonProperty("set_id") String setId,@JsonProperty("slot_index") Integer slotIndex,
		@JsonProperty("item_id") String itemId,String status,JsonNode item,JsonNode verification,
		@JsonProperty("available_actions") JsonNode availableActions) { }
}
