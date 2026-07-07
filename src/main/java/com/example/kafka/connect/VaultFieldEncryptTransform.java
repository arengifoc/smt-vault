package com.example.kafka.connect;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class VaultFieldEncryptTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_NAME_CONFIG = "field.name";
    public static final String VAULT_ADDR_CONFIG = "vault.addr";
    public static final String VAULT_PATH_CONFIG = "vault.path";
    public static final String VAULT_TOKEN_ENV_CONFIG = "vault.token.env";
    public static final String VAULT_REQUEST_VALUE_FIELD_CONFIG = "vault.request.value.field";
    public static final String VAULT_RESPONSE_VALUE_FIELD_CONFIG = "vault.response.value.field";
    public static final String VAULT_TIMEOUT_MS_CONFIG = "vault.timeout.ms";

    private static final String DEFAULT_TOKEN_ENV = "VAULT_TOKEN";
    private static final String DEFAULT_REQUEST_VALUE_FIELD = "plaintext";
    private static final String DEFAULT_RESPONSE_VALUE_FIELD = "data.ciphertext";
    private static final int DEFAULT_TIMEOUT_MS = 2000;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Name of the value field to encrypt")
            .define(VAULT_ADDR_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Vault base address, for example http://vault:8200")
            .define(VAULT_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Vault endpoint path without /v1, for example transit/encrypt/cards")
            .define(VAULT_TOKEN_ENV_CONFIG, ConfigDef.Type.STRING, DEFAULT_TOKEN_ENV, ConfigDef.Importance.MEDIUM,
                    "Environment variable name that stores the Vault token")
            .define(VAULT_REQUEST_VALUE_FIELD_CONFIG, ConfigDef.Type.STRING, DEFAULT_REQUEST_VALUE_FIELD,
                    ConfigDef.Importance.MEDIUM,
                    "JSON attribute used in the Vault request body")
            .define(VAULT_RESPONSE_VALUE_FIELD_CONFIG, ConfigDef.Type.STRING, DEFAULT_RESPONSE_VALUE_FIELD,
                    ConfigDef.Importance.MEDIUM,
                    "Simple JSON path containing the transformed value in Vault response")
            .define(VAULT_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, DEFAULT_TIMEOUT_MS, ConfigDef.Importance.MEDIUM,
                    "HTTP timeout in milliseconds");

    private String fieldName;
    private VaultClient vaultClient;

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (value == null) {
            return record;
        }

        Schema valueSchema = record.valueSchema();
        Object updatedValue;

        if (valueSchema != null && value instanceof Struct struct) {
            updatedValue = transformStruct(record, valueSchema, struct);
        } else if (valueSchema == null && value instanceof Map<?, ?> map) {
            updatedValue = transformMap(record, map);
        } else {
            throw new ConnectException("Unsupported record value type: " + value.getClass().getName());
        }

        if (updatedValue == value) {
            return record;
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                updatedValue,
                record.timestamp(),
                record.headers()
        );
    }

    private Object transformStruct(R record, Schema schema, Struct originalStruct) {
        Field targetField = schema.field(fieldName);
        if (targetField == null) {
            return originalStruct;
        }

        Object originalValue = originalStruct.get(fieldName);
        if (originalValue == null) {
            return originalStruct;
        }

        String encrypted = vaultClient.encrypt(String.valueOf(originalValue), record.topic(), fieldName);

        // Rebuild the struct so all original fields are preserved, changing only the configured field.
        Struct newStruct = new Struct(schema);
        for (Field field : schema.fields()) {
            if (field.name().equals(fieldName)) {
                newStruct.put(field.name(), encrypted);
            } else {
                newStruct.put(field.name(), originalStruct.get(field));
            }
        }

        return newStruct;
    }

    private Object transformMap(R record, Map<?, ?> map) {
        if (!map.containsKey(fieldName)) {
            return map;
        }

        Object originalValue = map.get(fieldName);
        if (originalValue == null) {
            return map;
        }

        String encrypted = vaultClient.encrypt(String.valueOf(originalValue), record.topic(), fieldName);

        // Copy the map to avoid mutating the source record value in-place.
        Map<String, Object> newMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            newMap.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        newMap.put(fieldName, encrypted);

        return newMap;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // Nothing to close for this MVP.
    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);

        this.fieldName = config.getString(FIELD_NAME_CONFIG);
        String vaultAddr = config.getString(VAULT_ADDR_CONFIG);
        String vaultPath = config.getString(VAULT_PATH_CONFIG);
        String tokenEnv = config.getString(VAULT_TOKEN_ENV_CONFIG);
        String requestValueField = config.getString(VAULT_REQUEST_VALUE_FIELD_CONFIG);
        String responseValueField = config.getString(VAULT_RESPONSE_VALUE_FIELD_CONFIG);
        int timeoutMs = config.getInt(VAULT_TIMEOUT_MS_CONFIG);

        String token = System.getenv(tokenEnv);
        if (token == null || token.isBlank()) {
            throw new ConnectException("Vault token environment variable is missing or empty: " + tokenEnv);
        }

        this.vaultClient = new VaultClient(
                vaultAddr,
                vaultPath,
                token,
                requestValueField,
                responseValueField,
                timeoutMs
        );
    }
}
