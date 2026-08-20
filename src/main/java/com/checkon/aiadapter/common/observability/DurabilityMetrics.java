package com.checkon.aiadapter.common.observability;

import java.time.Duration;
import io.micrometer.core.instrument.Metrics;

public final class DurabilityMetrics {
	private DurabilityMetrics() { }
	public static void transition(String feature,String result){Metrics.counter("checkon.worker.transitions","feature",feature,"result",result).increment();}
	public static void staleReclaim(String feature,String phase){Metrics.counter("checkon.worker.stale.reclaims","feature",feature,"phase",phase).increment();}
	public static void fencingRejected(String feature,String phase){Metrics.counter("checkon.worker.fencing.rejections","feature",feature,"phase",phase).increment();}
	public static <T> T timeHttp(String feature,String operation,java.util.concurrent.Callable<T> call) throws Exception {
		return Metrics.timer("checkon.ai.http.duration","feature",feature,"operation",operation).recordCallable(call);
	}
}
