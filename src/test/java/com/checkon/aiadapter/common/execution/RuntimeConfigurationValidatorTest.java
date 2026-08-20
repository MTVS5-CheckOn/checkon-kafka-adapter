package com.checkon.aiadapter.common.execution;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import com.checkon.aiadapter.common.kafka.CounselDraftKafkaProperties;
import com.checkon.aiadapter.common.kafka.RiskDetectionKafkaProperties;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftHttpProperties;
import com.checkon.aiadapter.counsel.application.CounselDraftProcessingProperties;
import com.checkon.aiadapter.detection.ai.AiRiskDetectionHttpProperties;
import com.checkon.aiadapter.detection.application.RiskDetectionProcessingProperties;
import com.checkon.aiadapter.problem.application.ProblemGenerationProperties;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationKafkaProperties;

class RuntimeConfigurationValidatorTest {
	@Test
	@DisplayName("Given Kafka만 활성화된 위험 탐지 When 시작 설정을 검증하면 Then 부분 활성화를 거절한다")
	void rejectsPartialRiskActivation() {
		// Given
		var validator=validator(false,false,true,Duration.ofSeconds(75),Duration.ofSeconds(65));
		// When/Then
		assertThatThrownBy(()->validator.run(new DefaultApplicationArguments(new String[0])))
			.isInstanceOf(IllegalStateException.class).hasMessageContaining("together");
	}

	@Test
	@DisplayName("Given HTTP 최대 블로킹 시간보다 짧은 lease When 시작 설정을 검증하면 Then 명확히 실패한다")
	void rejectsLeaseShorterThanHttpBlockingTime() {
		// Given
		var validator=validator(true,true,true,Duration.ofSeconds(60),Duration.ofSeconds(65));
		// When/Then
		assertThatThrownBy(()->validator.run(new DefaultApplicationArguments(new String[0])))
			.isInstanceOf(IllegalStateException.class).hasMessageContaining("lease timeout");
	}

	private RuntimeConfigurationValidator validator(boolean http,boolean worker,boolean kafka,
		Duration lease,Duration read) {
		var riskHttp=new AiRiskDetectionHttpProperties(http,"http://localhost:8000","/v1/detect",Duration.ofSeconds(3),read);
		var riskWorker=new RiskDetectionProcessingProperties(worker,Duration.ofSeconds(1),lease,Duration.ofSeconds(1),3);
		var riskKafka=new RiskDetectionKafkaProperties(kafka,"requested","completed","failed","group",
			Duration.ofSeconds(1),Duration.ofSeconds(30),Duration.ofSeconds(1),Duration.ofSeconds(10),8);
		var problem=new ProblemGenerationProperties(false,"","/v1/problems",Duration.ofSeconds(2),Duration.ofMinutes(6),
			Duration.ofSeconds(1),5,Duration.ofSeconds(3),Duration.ofSeconds(300),Duration.ofMinutes(21),3145728);
		var problemKafka=new ProblemGenerationKafkaProperties(false,"requests","results","group",Duration.ofSeconds(1),
			Duration.ofSeconds(30),Duration.ofSeconds(5),Duration.ofSeconds(10),8);
		var counselHttp=new AiCounselDraftHttpProperties(false,"","/v1/counsel/drafts",Duration.ofSeconds(2),Duration.ofSeconds(500));
		var counselWorker=new CounselDraftProcessingProperties(false,Duration.ofSeconds(1),Duration.ofMinutes(10),
			Duration.ofSeconds(1),3);
		var counselKafka=new CounselDraftKafkaProperties(false,"requested","completed","failed","group",
			Duration.ofSeconds(1),Duration.ofSeconds(30),Duration.ofSeconds(1),Duration.ofSeconds(10),8);
		return new RuntimeConfigurationValidator(riskHttp,riskWorker,riskKafka,problem,problemKafka,
			counselHttp,counselWorker,counselKafka);
	}
}
