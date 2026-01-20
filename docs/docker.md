# Infraestrutura Docker

Configurações Docker para o projeto data-gen, incluindo um deployment completo do Confluent Platform.

> **Docs relacionados**: [Ingestão de Dados](./data-ingestion.md) | [Catálogo de Dados](./data-catalog.md) | [Índice da Documentação](./README.md)

## Quick Start

```bash
# Iniciar todos os serviços
docker-compose up -d

# Verificar status
docker-compose ps

# Parar serviços
docker-compose down

# Parar e remover volumes (começar do zero)
docker-compose down -v
```

## Serviços

| Serviço | Container | Porta | Descrição |
|---------|-----------|-------|-----------|
| Zookeeper | datagen-zookeeper | 2181 | Serviço de coordenação do Kafka |
| Kafka Broker | datagen-broker | 9092, 29092 | Message broker |
| Schema Registry | datagen-schema-registry | 8081 | Gerenciamento de schemas Avro/JSON/Protobuf |
| Kafka Connect | datagen-connect | 8083 | Framework de integração de dados |
| Control Center | datagen-control-center | 9021 | Interface web para monitoramento |
| ksqlDB | datagen-ksqldb-server | 8088 | Engine de SQL streaming |
| REST Proxy | datagen-rest-proxy | 8082 | Interface HTTP para Kafka |
| PostgreSQL | datagen-postgres | 5432 | Banco de dados para conectores JDBC |

## URLs de Acesso

- **Control Center**: http://localhost:9021
- **Schema Registry API**: http://localhost:8081
- **Kafka Connect API**: http://localhost:8083
- **ksqlDB API**: http://localhost:8088
- **REST Proxy API**: http://localhost:8082

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         datagen-network                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐           │
│  │  Zookeeper   │◄────►│    Broker    │◄────►│   Schema     │           │
│  │    :2181     │      │  :9092/29092 │      │  Registry    │           │
│  └──────────────┘      └──────┬───────┘      │    :8081     │           │
│                               │              └──────┬───────┘           │
│                               │                     │                    │
│         ┌─────────────────────┼─────────────────────┤                   │
│         │                     │                     │                    │
│         ▼                     ▼                     ▼                    │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐           │
│  │   Connect    │      │   ksqlDB     │      │  REST Proxy  │           │
│  │    :8083     │      │    :8088     │      │    :8082     │           │
│  └──────┬───────┘      └──────────────┘      └──────────────┘           │
│         │                                                                │
│         │              ┌──────────────┐      ┌──────────────┐           │
│         └─────────────►│  PostgreSQL  │      │   Control    │           │
│                        │    :5432     │      │   Center     │           │
│                        └──────────────┘      │    :9021     │           │
│                                              └──────────────┘           │
└─────────────────────────────────────────────────────────────────────────┘
```

## Kafka Connect

### Conectores Pré-instalados

O container do Connect instala automaticamente estes conectores na inicialização:

| Conector | Versão | Caso de Uso |
|----------|--------|-------------|
| JDBC Source/Sink | 10.7.4 | Integração com banco de dados |
| Debezium PostgreSQL | 2.4.0 | CDC (Change Data Capture) |
| Elasticsearch | 14.0.12 | Indexação de busca |
| JSON Schema Converter | 7.5.0 | Suporte a JSON schema |

### Listar Plugins Instalados

```bash
curl -s http://localhost:8083/connector-plugins | jq '.[].class'
```

### Implantar um Conector

```bash
# Exemplo: JDBC Source do PostgreSQL
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-source",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
      "connection.url": "jdbc:postgresql://postgres:5432/datagen",
      "connection.user": "postgres",
      "connection.password": "postgres",
      "topic.prefix": "db-",
      "mode": "incrementing",
      "incrementing.column.name": "id",
      "poll.interval.ms": "1000"
    }
  }'
```

### Gerenciar Conectores

```bash
# Listar conectores
curl -s http://localhost:8083/connectors | jq

# Obter status do conector
curl -s http://localhost:8083/connectors/postgres-source/status | jq

# Pausar conector
curl -X PUT http://localhost:8083/connectors/postgres-source/pause

# Retomar conector
curl -X PUT http://localhost:8083/connectors/postgres-source/resume

# Deletar conector
curl -X DELETE http://localhost:8083/connectors/postgres-source
```

## Schema Registry

### Registrar um Schema

```bash
# Registrar schema Avro
curl -X POST http://localhost:8081/subjects/customers-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"
  }'
```

### Listar Schemas

```bash
# Listar todos os subjects
curl -s http://localhost:8081/subjects | jq

# Obter schema para subject
curl -s http://localhost:8081/subjects/customers-value/versions/latest | jq

# Obter schema por ID
curl -s http://localhost:8081/schemas/ids/1 | jq
```

### Compatibilidade

```bash
# Obter nível de compatibilidade global
curl -s http://localhost:8081/config | jq

# Definir compatibilidade para subject
curl -X PUT http://localhost:8081/config/customers-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

## Tópicos Kafka

### Criar Tópico

```bash
docker exec datagen-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic customers \
  --partitions 3 \
  --replication-factor 1
```

### Listar Tópicos

```bash
docker exec datagen-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

### Descrever Tópico

```bash
docker exec datagen-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic customers
```

### Produzir Mensagens

```bash
# Mensagens string
docker exec -it datagen-broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic customers

# Com chave
docker exec -it datagen-broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic customers \
  --property "parse.key=true" \
  --property "key.separator=:"
```

### Consumir Mensagens

```bash
# Desde o início
docker exec datagen-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic customers \
  --from-beginning

# Com chave
docker exec datagen-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic customers \
  --from-beginning \
  --property "print.key=true"
```

## ksqlDB

### Acessar CLI do ksqlDB

```bash
docker exec -it datagen-ksqldb-server ksql http://localhost:8088
```

### Exemplos de Queries

```sql
-- Mostrar tópicos
SHOW TOPICS;

-- Criar stream a partir de tópico
CREATE STREAM customers_stream (
  customer_id VARCHAR KEY,
  name VARCHAR,
  email VARCHAR,
  credit_score INT
) WITH (
  KAFKA_TOPIC='customers',
  VALUE_FORMAT='JSON'
);

-- Consultar stream
SELECT * FROM customers_stream EMIT CHANGES;

-- Criar tabela com agregação
CREATE TABLE customer_count AS
  SELECT credit_score,
         COUNT(*) AS total
  FROM customers_stream
  GROUP BY credit_score
  EMIT CHANGES;
```

## PostgreSQL

### Conectar ao Banco de Dados

```bash
docker exec -it datagen-postgres psql -U postgres -d datagen
```

### Detalhes de Conexão

| Parâmetro | Valor |
|-----------|-------|
| Host | localhost |
| Porta | 5432 |
| Banco de Dados | datagen |
| Usuário | postgres |
| Senha | postgres |

### URL JDBC

```
jdbc:postgresql://localhost:5432/datagen
# Dentro da rede Docker:
jdbc:postgresql://postgres:5432/datagen
```

## Troubleshooting

### Verificar Saúde dos Serviços

```bash
# Todos os serviços
docker-compose ps

# Logs de serviço específico
docker-compose logs -f broker
docker-compose logs -f connect
docker-compose logs -f schema-registry
```

### Problemas Comuns

#### Broker Não Iniciando

```bash
# Verificar se Zookeeper está saudável primeiro
docker-compose logs zookeeper

# Verificar conexão com Zookeeper
docker exec datagen-broker kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

#### Plugins do Connect Não Carregando

```bash
# Verificar logs de instalação de plugins
docker logs datagen-connect 2>&1 | grep -i "install"

# Listar plugins disponíveis
curl -s http://localhost:8083/connector-plugins | jq
```

#### Erros no Schema Registry

```bash
# Verificar saúde do Schema Registry
curl -s http://localhost:8081/subjects

# Verificar conexão com Kafka
docker logs datagen-schema-registry 2>&1 | grep -i "error"
```

#### Falta de Memória

Aumente a alocação de memória do Docker para pelo menos 8GB para a stack completa.

```bash
# Verificar uso atual de recursos
docker stats
```

### Resetar Tudo

```bash
# Parar e remover todos os containers e volumes
docker-compose down -v

# Remover rede
docker network rm datagen-network 2>/dev/null || true

# Iniciar do zero
docker-compose up -d
```

## Requisitos de Recursos

| Perfil | Memória | CPU | Caso de Uso |
|--------|---------|-----|-------------|
| Mínimo | 4GB | 2 cores | Desenvolvimento (Zookeeper, Broker, Schema Registry) |
| Padrão | 6GB | 4 cores | + Connect, ksqlDB |
| Completo | 8GB+ | 4+ cores | + Control Center, REST Proxy |

## Variáveis de Ambiente

Principais opções de configuração que podem ser customizadas:

### Broker

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `KAFKA_BROKER_ID` | 1 | Identificador único do broker |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | true | Auto-criar tópicos |
| `KAFKA_LOG_RETENTION_HOURS` | 168 | Retenção de logs (7 dias) |

### Connect

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `CONNECT_GROUP_ID` | datagen-connect-cluster | Consumer group do Connect |
| `CONNECT_KEY_CONVERTER` | StringConverter | Serialização de chave |
| `CONNECT_VALUE_CONVERTER` | AvroConverter | Serialização de valor |

### Schema Registry

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL` | BACKWARD | Compatibilidade padrão |

## Volumes

| Volume | Serviço | Propósito |
|--------|---------|-----------|
| `zookeeper-data` | Zookeeper | Dados do Zookeeper |
| `zookeeper-logs` | Zookeeper | Logs do Zookeeper |
| `broker-data` | Broker | Logs do Kafka |
| `connect-plugins` | Connect | JARs de conectores |
| `postgres-data` | PostgreSQL | Arquivos do banco de dados |
