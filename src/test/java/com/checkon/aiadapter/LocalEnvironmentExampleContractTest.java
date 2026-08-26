package com.checkon.aiadapter;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("로컬 환경변수 예제 계약")
class LocalEnvironmentExampleContractTest {

	private static final Path ENV_EXAMPLE = Path.of(".env.example");
	private static final Path APPLICATION_CONFIGURATION = Path.of("src/main/resources/application.yaml");

	@Test
	@DisplayName("Given 상담 동기 프록시 설정 When 예제 환경변수를 확인하면 Then 실제 설정 키만 제공한다")
	void exposesCanonicalSynchronousCounselProxyEnvironmentKeys() throws IOException {
		// Given
		Set<String> canonicalKeys = Set.of(
			"AI_CLASSIFY_BASE_URL",
			"AI_CLASSIFY_PATH",
			"AI_CLASSIFY_CONFIRMATIONS_PATH",
			"AI_CLASSIFY_CONNECT_TIMEOUT",
			"AI_CLASSIFY_READ_TIMEOUT",
			"AI_LABELS_BASE_URL",
			"AI_LABELS_SUGGEST_PATH",
			"AI_LABELS_CONFIRMATIONS_PATH",
			"AI_LABELS_CONNECT_TIMEOUT",
			"AI_LABELS_READ_TIMEOUT"
		);
		Set<String> keys = Files.readAllLines(ENV_EXAMPLE).stream()
			.map(String::trim)
			.filter(line -> !line.isBlank() && !line.startsWith("#") && line.contains("="))
			.map(line -> line.substring(0, line.indexOf('=')))
			.collect(Collectors.toSet());
		String applicationConfiguration = Files.readString(APPLICATION_CONFIGURATION);

		// When / Then
		assertThat(keys).containsAll(canonicalKeys).doesNotContain(
			"AI_LABEL_CONFIRMATION_BASE_URL",
			"AI_LABEL_SUGGEST_PATH",
			"AI_CONFIRMATIONS_PATH",
			"AI_LABEL_CONFIRMATION_CONNECT_TIMEOUT",
			"AI_LABEL_CONFIRMATION_READ_TIMEOUT"
		);
		assertThat(canonicalKeys).allSatisfy(key ->
			assertThat(applicationConfiguration).contains("${" + key + ":")
		);
	}
}
