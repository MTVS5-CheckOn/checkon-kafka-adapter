package com.checkon.aiadapter.common.observability;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import io.micrometer.core.instrument.MeterRegistry;

@Component
@ConditionalOnBean(JdbcTemplate.class)
public class DurableQueueGauges {
	public DurableQueueGauges(MeterRegistry registry,JdbcTemplate jdbc,Clock clock) {
		register(registry,jdbc,clock,"risk_detection","risk_detection_request_inbox","next_attempt_at");
		register(registry,jdbc,clock,"problem_generation","problem_generation_request_inbox","next_attempt_at");
		outbox(registry,jdbc,"risk_detection","risk_detection_outbox");
		outbox(registry,jdbc,"problem_generation","problem_generation_outbox");
	}
	private static void register(MeterRegistry registry,JdbcTemplate jdbc,Clock clock,String feature,String table,String readyAt) {
		registry.gauge("checkon.inbox.pending",java.util.List.of(io.micrometer.core.instrument.Tag.of("feature",feature)),jdbc,
			value->number(value,"SELECT count(*) FROM "+table+" WHERE status IN ('RECEIVED','WAITING','RETRY_PENDING')"));
		registry.gauge("checkon.inbox.oldest.wait.seconds",java.util.List.of(io.micrometer.core.instrument.Tag.of("feature",feature)),jdbc,
			value->age(value,clock,"SELECT min(created_at) FROM "+table+" WHERE status IN ('RECEIVED','WAITING','RETRY_PENDING')"));
	}
	private static void outbox(MeterRegistry registry,JdbcTemplate jdbc,String feature,String table) {
		for(String status:new String[]{"PENDING","DEAD"}) registry.gauge("checkon.outbox.count",
			java.util.List.of(io.micrometer.core.instrument.Tag.of("feature",feature),io.micrometer.core.instrument.Tag.of("status",status)),jdbc,
			value->number(value,"SELECT count(*) FROM "+table+" WHERE status='"+status+"'"));
	}
	private static double number(JdbcTemplate jdbc,String sql){try{return jdbc.queryForObject(sql,Long.class);}catch(RuntimeException ignored){return Double.NaN;}}
	private static double age(JdbcTemplate jdbc,Clock clock,String sql){try{Instant value=jdbc.queryForObject(sql,Instant.class);return value==null?0:Math.max(0,Duration.between(value,Instant.now(clock)).toSeconds());}catch(RuntimeException ignored){return Double.NaN;}}
}
