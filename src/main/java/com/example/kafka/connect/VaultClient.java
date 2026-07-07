package com.example.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VaultClient {

    private static final Logger log = LoggerFactory.getLogger(VaultClient.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI endpoint;
    private final String token;
    private final String requestValueField;
    private final String responseValueField;
    private final Duration timeout;

    public VaultClient(
            String vaultAddr,
            String vaultPath,
            String token,
            String requestValueField,
            String responseValueField,
            int timeoutMs
    ) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .build();
        this.objectMapper = new ObjectMapper();
        this.endpoint = buildEndpoint(vaultAddr, vaultPath);
        this.token = token;
        this.requestValueField = requestValueField;
        this.responseValueField = responseValueField;
        this.timeout = Duration.ofMillis(timeoutMs);
    }

    public String encrypt(String plainText, String topic, String fieldName) {
        String base64 = Base64.getEncoder().encodeToString(plainText.getBytes(StandardCharsets.UTF_8));

        Map<String, Object> body = new HashMap<>();
        body.put(requestValueField, base64);

        final String requestBody;
        try {
            requestBody = objectMapper.writeValueAsString(body);
        } catch (IOException e) {
            throw new DataException("Failed to serialize Vault request body", e);
        }

        HttpRequest request = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("X-Vault-Token", token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8))
                .build();

        final HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new DataException("Vault request failed for topic=" + topic + ", field=" + fieldName, e);
        }

        int status = response.statusCode();
        log.info("Vault request completed: topic={}, field={}, httpStatus={}", topic, fieldName, status);

        if (status < 200 || status >= 300) {
            throw new DataException("Vault returned non-2xx response for topic=" + topic
                    + ", field=" + fieldName + ", httpStatus=" + status);
        }

        return extractResponseField(response.body(), responseValueField, topic, fieldName, status);
    }

    private static URI buildEndpoint(String vaultAddr, String vaultPath) {
        String sanitizedAddr = trimSlashes(vaultAddr);
        String sanitizedPath = trimSlashes(vaultPath);
        try {
            return new URI(sanitizedAddr + "/v1/" + sanitizedPath);
        } catch (URISyntaxException e) {
            throw new DataException("Invalid Vault endpoint address/path", e);
        }
    }

    private static String trimSlashes(String value) {
        String result = value == null ? "" : value.trim();
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        while (result.startsWith("/")) {
            result = result.substring(1);
        }
        return result;
    }

    private String extractResponseField(String responseBody, String jsonPath, String topic, String fieldName, int status) {
        final JsonNode root;
        try {
            root = objectMapper.readTree(responseBody);
        } catch (IOException e) {
            throw new DataException("Failed to parse Vault response for topic=" + topic
                    + ", field=" + fieldName + ", httpStatus=" + status, e);
        }

        JsonNode current = root;
        for (String segment : jsonPath.split("\\.")) {
            if (segment.isEmpty()) {
                continue;
            }
            current = current.path(segment);
            if (current.isMissingNode() || current.isNull()) {
                throw new DataException("Vault response field not found: path=" + jsonPath
                        + ", topic=" + topic + ", field=" + fieldName + ", httpStatus=" + status);
            }
        }

        if (!current.isValueNode()) {
            throw new DataException("Vault response field is not a scalar value: path=" + jsonPath
                    + ", topic=" + topic + ", field=" + fieldName + ", httpStatus=" + status);
        }

        return current.asText();
    }
}
