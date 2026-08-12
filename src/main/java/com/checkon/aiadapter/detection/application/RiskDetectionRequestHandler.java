package com.checkon.aiadapter.detection.application;

import com.checkon.aiadapter.detection.kafka.RiskDetectionRequestedEvent;

/**
 * Kafka transport와 위험 탐지 실행을 분리하는 application 경계다.
 * 다음 단계에서 내구성 있는 처리 상태와 AI HTTP 호출을 이 경계 뒤에 연결한다.
 */
@FunctionalInterface
public interface RiskDetectionRequestHandler {

	void handle(RiskDetectionRequestedEvent event, String rawEvent);
}
