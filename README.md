# VaultFieldEncryptTransform (Kafka Connect SMT)

MVP funcional de un Single Message Transform (SMT) para Kafka Connect que:

- Lee un campo configurado del value del record.
- Envía el valor a HashiCorp Vault (Transit encrypt) por HTTP.
- Reemplaza el valor original con el valor devuelto por Vault.

Implementado con Java 17, Maven y API de Kafka Connect, sin frameworks pesados.

## 1. Compilar

Requisitos:

- Java 17
- Maven 3.8+

Compilación:

```bash
mvn clean package
```

El artefacto se genera en:

```text
target/vault-field-encrypt-transform-0.1.0-SNAPSHOT.jar
```

## 2. Instalar en plugin.path de Kafka Connect

1. Crea una carpeta para el plugin, por ejemplo:

```bash
mkdir -p /opt/kafka/plugins/vault-field-encrypt
```

2. Copia el JAR construido a esa carpeta:

```bash
cp target/vault-field-encrypt-transform-0.1.0-SNAPSHOT.jar /opt/kafka/plugins/vault-field-encrypt/
```

3. Asegura que el worker tenga ese path en `plugin.path` (worker config):

```properties
plugin.path=/opt/kafka/plugins
```

4. Reinicia el worker de Kafka Connect.

## 3. Variable de entorno para token de Vault

El SMT lee el token desde una variable de entorno.

Por defecto usa `VAULT_TOKEN`:

```bash
export VAULT_TOKEN="hvs.xxxxx"
```

También se puede cambiar con `vault.token.env`.

## 4. Configuración del SMT

Propiedades soportadas:

- `field.name` (requerido): campo a proteger, por ejemplo `pan`.
- `vault.addr` (requerido): URL base de Vault, por ejemplo `http://vault:8200`.
- `vault.path` (requerido): endpoint sin `/v1`, por ejemplo `transit/encrypt/cards`.
- `vault.token.env` (opcional, default `VAULT_TOKEN`): nombre de variable de entorno con el token.
- `vault.request.value.field` (opcional, default `plaintext`): atributo JSON enviado a Vault.
- `vault.response.value.field` (opcional, default `data.ciphertext`): path simple en JSON de respuesta.
- `vault.timeout.ms` (opcional, default `2000`): timeout HTTP en milisegundos.

## 5. Ejemplo con Debezium Connector

Ejemplo de configuración JSON (incluyendo transform):

```json
{
	"name": "debezium-postgres-cards",
	"config": {
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"tasks.max": "1",
		"database.hostname": "postgres",
		"database.port": "5432",
		"database.user": "debezium",
		"database.password": "dbz",
		"database.dbname": "cards",
		"database.server.name": "cards",
		"topic.prefix": "cards",
		"table.include.list": "public.card_transactions",

		"transforms": "vaultEncrypt",
		"transforms.vaultEncrypt.type": "com.example.kafka.connect.VaultFieldEncryptTransform",
		"transforms.vaultEncrypt.field.name": "pan",
		"transforms.vaultEncrypt.vault.addr": "http://vault:8200",
		"transforms.vaultEncrypt.vault.path": "transit/encrypt/cards",
		"transforms.vaultEncrypt.vault.token.env": "VAULT_TOKEN",
		"transforms.vaultEncrypt.vault.request.value.field": "plaintext",
		"transforms.vaultEncrypt.vault.response.value.field": "data.ciphertext",
		"transforms.vaultEncrypt.vault.timeout.ms": "2000"
	}
}
```

## 6. Flujo de Vault Transit usado por el SMT

Request enviado por el transform:

- URL: `{vault.addr}/v1/{vault.path}`
- Método: `POST`
- Headers:
	- `X-Vault-Token: <token desde variable de entorno>`
	- `Content-Type: application/json`
- Body (ejemplo para Transit):

```json
{
	"plaintext": "PHZhbG9yX2VuX2Jhc2U2ND4="
}
```

Respuesta esperada:

```json
{
	"data": {
		"ciphertext": "vault:v1:..."
	}
}
```

El valor de `data.ciphertext` reemplaza el contenido original del campo configurado.

## 7. Comportamiento de errores y casos borde

- Si `record.value()` es `null`: devuelve el record sin cambios.
- Si el campo no existe: devuelve el record sin cambios.
- Si el campo existe pero su valor es `null`: devuelve el record sin cambios.
- Si falta la variable de entorno del token: falla en `configure()` con `ConnectException`.
- Si Vault responde no 2xx: lanza `DataException`.
- Si hay timeout o error de red: lanza `DataException`.
- Nunca se loguea el valor original del campo.