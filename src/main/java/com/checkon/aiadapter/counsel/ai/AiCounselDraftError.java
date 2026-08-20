package com.checkon.aiadapter.counsel.ai;

import java.util.Map;

/** Shared {@code error} shape across the counsel envelope responses. */
public record AiCounselDraftError(
	String code,
	String message,
	Map<String, Object> detail
) {
}
