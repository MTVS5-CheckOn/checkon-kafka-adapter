package com.checkon.aiadapter.common.execution;

import java.net.URI;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jdbc.core.JdbcTemplate;
import com.checkon.aiadapter.common.kafka.CounselDraftKafkaProperties;
import com.checkon.aiadapter.common.kafka.RiskDetectionKafkaProperties;
import com.checkon.aiadapter.counsel.ai.AiCounselDraftHttpProperties;
import com.checkon.aiadapter.counsel.application.CounselDraftProcessingProperties;
import com.checkon.aiadapter.detection.ai.AiRiskDetectionHttpProperties;
import com.checkon.aiadapter.detection.application.RiskDetectionProcessingProperties;
import com.checkon.aiadapter.problem.application.ProblemGenerationProperties;
import com.checkon.aiadapter.problem.kafka.ProblemGenerationKafkaProperties;

@Component
@ConditionalOnBean(JdbcTemplate.class)
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RuntimeConfigurationValidator implements ApplicationRunner {
	private final AiRiskDetectionHttpProperties riskHttp;
	private final RiskDetectionProcessingProperties riskWorker;
	private final RiskDetectionKafkaProperties riskKafka;
	private final ProblemGenerationProperties problemWorker;
	private final ProblemGenerationKafkaProperties problemKafka;
	private final AiCounselDraftHttpProperties counselHttp;
	private final CounselDraftProcessingProperties counselWorker;
	private final CounselDraftKafkaProperties counselKafka;
	public RuntimeConfigurationValidator(AiRiskDetectionHttpProperties riskHttp,
		RiskDetectionProcessingProperties riskWorker,RiskDetectionKafkaProperties riskKafka,
		ProblemGenerationProperties problemWorker,ProblemGenerationKafkaProperties problemKafka,
		AiCounselDraftHttpProperties counselHttp,CounselDraftProcessingProperties counselWorker,
		CounselDraftKafkaProperties counselKafka) {
		this.riskHttp=riskHttp;this.riskWorker=riskWorker;this.riskKafka=riskKafka;
		this.problemWorker=problemWorker;this.problemKafka=problemKafka;
		this.counselHttp=counselHttp;this.counselWorker=counselWorker;this.counselKafka=counselKafka;
	}
	@Override public void run(ApplicationArguments args) {
		boolean anyRisk=riskHttp.enabled()||riskWorker.workerEnabled()||riskKafka.enabled();
		if(anyRisk&&!(riskHttp.enabled()&&riskWorker.workerEnabled()&&riskKafka.enabled()))
			throw invalid("risk detection requires Kafka consumer, AI HTTP, and durable worker together");
		if(anyRisk) {
			requireHttpUrl(riskHttp.baseUrl(),"risk detection base-url");
			if(riskHttp.connectTimeout()==null||riskHttp.readTimeout()==null) throw invalid("risk detection timeouts are required");
			if(riskWorker.lockTimeout().compareTo(riskHttp.connectTimeout().plus(riskHttp.readTimeout()))<=0)
				throw invalid("risk detection lease timeout must exceed connect-timeout + read-timeout");
		}
		boolean anyProblem=problemWorker.workerEnabled()||problemKafka.enabled();
		if(anyProblem&&!(problemWorker.workerEnabled()&&problemKafka.enabled()))
			throw invalid("problem generation Kafka consumer, durable worker, and publisher must be enabled together");
		if(anyProblem) {
			requireHttpUrl(problemWorker.baseUrl(),"problem generation base-url");
			if(problemWorker.lockTimeout().compareTo(problemWorker.connectTimeout().plus(problemWorker.readTimeout()))<=0)
				throw invalid("problem generation lease timeout must exceed connect-timeout + read-timeout");
		}
		boolean anyCounsel=counselHttp.enabled()||counselWorker.workerEnabled()||counselKafka.enabled();
		if(anyCounsel&&!(counselHttp.enabled()&&counselWorker.workerEnabled()&&counselKafka.enabled()))
			throw invalid("counsel draft requires Kafka consumer, AI HTTP, and durable worker together");
		if(anyCounsel) {
			requireHttpUrl(counselHttp.baseUrl(),"counsel draft base-url");
			if(counselHttp.connectTimeout()==null||counselHttp.readTimeout()==null) throw invalid("counsel draft timeouts are required");
			if(counselWorker.lockTimeout().compareTo(counselHttp.connectTimeout().plus(counselHttp.readTimeout()))<=0)
				throw invalid("counsel draft lease timeout must exceed connect-timeout + read-timeout");
		}
	}
	private static void requireHttpUrl(String value,String name) {
		if(value==null||value.isBlank()) throw invalid(name+" is required when worker is enabled");
		URI uri=URI.create(value);
		if(!"http".equalsIgnoreCase(uri.getScheme())&&!"https".equalsIgnoreCase(uri.getScheme())) throw invalid(name+" must use http or https");
	}
	private static IllegalStateException invalid(String message){return new IllegalStateException("Invalid adapter configuration: "+message);}
}
