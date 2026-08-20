package com.checkon.aiadapter.problem.ai;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.JsonNode;

public record ProblemItemSetResponse(Data data, Meta meta) {
	public record Data(@JsonProperty("job_id") String jobId,@JsonProperty("set_id") String setId,
		List<ItemSummary> items,@JsonProperty("status_counts") Map<String,Integer> statusCounts) { }
	public record ItemSummary(@JsonProperty("slot_index") Integer slotIndex,@JsonProperty("item_id") String itemId,
		String status,@JsonProperty("failure_reason") String failureReason,@JsonProperty("failure_detail") JsonNode failureDetail) { }
	public record Meta(Map<String,Object> versions) { }
}
