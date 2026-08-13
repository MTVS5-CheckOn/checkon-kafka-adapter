package com.checkon.aiadapter.problem.application;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("checkon.ai.problem-generation.nodes")
public record ProblemGenerationNodeProperties(
	@DefaultValue("language.grammar.phonological_change") String languageConcept,
	@DefaultValue("language.grammar.phonological_change") String languageInfer
) {
	public ProblemGenerationNodeProperties {
		languageConcept = requireNode(languageConcept, "languageConcept");
		languageInfer = requireNode(languageInfer, "languageInfer");
	}

	private static String requireNode(String value, String name) {
		if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
		return value;
	}
}
