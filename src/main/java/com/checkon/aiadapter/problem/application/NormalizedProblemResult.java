package com.checkon.aiadapter.problem.application;

import java.util.List;
import java.util.Map;
import tools.jackson.databind.JsonNode;

public record NormalizedProblemResult(String setId,List<Slot> items,Map<String,Integer> statusCounts,
	int requestedCount,int processedCount,Map<String,Object> versions) {
	public record Slot(int slotIndex,String itemId,String status,String failureReason,JsonNode failureDetail,
		JsonNode item,JsonNode verification,JsonNode availableActions) { }
}
