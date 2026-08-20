package com.checkon.aiadapter.counsel.application;

import com.checkon.aiadapter.counsel.kafka.CounselDraftRequestedEvent;

/** Kafka transport와 상담 초안 생성 실행을 분리하는 application 경계다. */
@FunctionalInterface
public interface CounselDraftRequestHandler {

	void handle(CounselDraftRequestedEvent event, String rawEvent);
}
