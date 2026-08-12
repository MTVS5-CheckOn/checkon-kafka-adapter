package com.checkon.aiadapter.detection.application;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.checkon.aiadapter.detection.ai.AiDetectionRequest;
import com.checkon.aiadapter.detection.ai.AiDetectionResponse;

@Component
public class AiDetectionResponseValidator {

	public void validate(AiDetectionRequest request, AiDetectionResponse response) {
		Objects.requireNonNull(request, "request must not be null");
		if (response == null || response.data() == null || response.meta() == null) {
			throw invalid("data and meta are required");
		}
		if (response.error() != null) {
			throw invalid("a successful response must have error=null");
		}
		requireText(response.meta().executionId(), "meta.execution_id");
		if (response.meta().versions() == null) {
			throw invalid("meta.versions is required");
		}
		List<AiDetectionResponse.Signal> signals = requireList(
			response.data().signals(), "data.signals");
		Objects.requireNonNull(response.data().stats(), "data.stats must not be null");

		Set<String> students = new HashSet<>();
		Set<String> classes = new HashSet<>();
		for (AiDetectionRequest.StudentSnapshot student : request.students()) {
			students.add(student.studentRef());
			classes.add(student.classRef());
		}
		Set<String> learningRecordIds = new HashSet<>();
		for (AiDetectionRequest.LearningEventSnapshot event : request.learningEvents()) {
			learningRecordIds.add(event.recordId());
		}
		Set<EvidenceKey> detectionEvidence = new HashSet<>();
		for (AiDetectionRequest.DetectionEvidence evidence : request.detectionEvidence()) {
			detectionEvidence.add(new EvidenceKey(evidence.sourceTable(), evidence.recordId()));
		}

		Set<String> signalIds = new HashSet<>();
		for (AiDetectionResponse.Signal signal : signals) {
			requireText(signal.signalId(), "signal.signal_id");
			if (!signalIds.add(signal.signalId())) {
				throw invalid("signal_id must be unique");
			}
			if (!students.contains(signal.studentRef())) {
				throw invalid("signal.student_ref is outside the request snapshot");
			}
			if (!classes.contains(signal.classRef())) {
				throw invalid("signal.class_ref is outside the request snapshot");
			}
			if (!Double.isFinite(signal.score())) {
				throw invalid("signal.score must be finite");
			}
			if (signal.rank() < 1) {
				throw invalid("signal.rank must be at least 1");
			}
			if (signal.brief() == null) {
				throw invalid("signal.brief is required");
			}
			requireText(signal.brief().text(), "signal.brief.text");
			for (AiDetectionResponse.Evidence evidence : requireList(
				signal.evidence(), "signal.evidence")) {
				requireText(evidence.sourceTable(), "evidence.source_table");
				requireText(evidence.recordId(), "evidence.record_id");
				EvidenceKey key = new EvidenceKey(evidence.sourceTable(), evidence.recordId());
				if (!detectionEvidence.contains(key)
					&& !learningRecordIds.contains(evidence.recordId())) {
					throw invalid("evidence does not belong to the request snapshot");
				}
			}
		}
	}

	private static <T> List<T> requireList(List<T> values, String field) {
		if (values == null) {
			throw invalid(field + " is required");
		}
		return values;
	}

	private static void requireText(String value, String field) {
		if (value == null || value.isBlank()) {
			throw invalid(field + " must not be blank");
		}
	}

	private static InvalidAiDetectionResponseException invalid(String message) {
		return new InvalidAiDetectionResponseException(message);
	}

	private record EvidenceKey(String sourceTable, String recordId) {
	}
}
