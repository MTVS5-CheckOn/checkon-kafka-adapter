package com.checkon.aiadapter.problem.ai;

import com.fasterxml.jackson.annotation.JsonProperty;

import tools.jackson.databind.JsonNode;

public record ProblemRevisionResponse(Data data,Meta meta) {
	public record Data(@JsonProperty("set_id") String setId,@JsonProperty("slot_index") Integer slotIndex,
		JsonNode revision,@JsonProperty("current_revision_no") Integer currentRevisionNo,JsonNode verification) { }
	public record Meta(@JsonProperty("execution_id") String executionId) { }
	public String executionId(){return meta==null?null:meta.executionId();}
}
