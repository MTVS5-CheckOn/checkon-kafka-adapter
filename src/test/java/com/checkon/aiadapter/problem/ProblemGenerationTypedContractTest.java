package com.checkon.aiadapter.problem;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.checkon.aiadapter.problem.ai.*;
import com.checkon.aiadapter.problem.application.ProblemGenerationOutcomeFactory;
import com.checkon.aiadapter.problem.infrastructure.ProblemGenerationStore.ClaimedRequest;
import tools.jackson.databind.json.JsonMapper;

class ProblemGenerationTypedContractTest {
	@Test
	@DisplayName("Given 알 수 없는 AI Job 상태 When 타입 계약으로 읽으면 Then polling하지 않고 즉시 거절한다")
	void rejectsUnknownJobStatus() {
		// Given/When/Then
		assertThatThrownBy(()->JsonMapper.builder().build().readValue(
			"{\"data\":{\"status\":\"mysterious\"}}",ProblemJobResponse.class))
			.hasRootCauseInstanceOf(IllegalArgumentException.class)
			.hasStackTraceContaining("unknown AI job status");
	}

	@Test
	@DisplayName("Given summary에 중복 slot When 정규화하면 Then 누락 숫자를 0으로 보정하지 않고 계약 오류로 거절한다")
	void rejectsDuplicateSlots() {
		// Given
		var factory=new ProblemGenerationOutcomeFactory(JsonMapper.builder().findAndAddModules().build());
		var summary=new ProblemItemSetResponse(new ProblemItemSetResponse.Data("job","set",List.of(
			new ProblemItemSetResponse.ItemSummary(0,null,"dropped","x",null),
			new ProblemItemSetResponse.ItemSummary(0,null,"dropped","x",null)),Map.of("dropped",2)),null);
		var job=new ProblemJobResponse(new ProblemJobResponse.Data("job",ProblemJobResponse.JobStatus.SUCCEEDED,
			new ProblemJobResponse.Result("set")),null);
		// When/Then
		assertThatThrownBy(()->factory.succeeded(UUID.randomUUID(),request(),"job",job,summary,List.of(),Instant.now()))
			.isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate slot_index");
	}

	private ClaimedRequest request(){UUID id=UUID.randomUUID();return new ClaimedRequest(id,UUID.randomUUID(),
		"tn_0123456789abcdef0123456789abcdef",UUID.randomUUID(),UUID.randomUUID(),0,id.toString(),"idem","{}",
		"POLL","job","exec",1,Instant.now(),1,false);}
}
