# Infraestrutura Docker

Configurações Docker para o projeto data-gen, incluindo um deployment completo do Confluent Platform 8.1.1 em modo KRaft (sem Zookeeper).

> **Docs relacionados**: [Ingestão de Dados](./data-ingestion.md) | [Catálogo de Dados](./data-catalog.md) | [Setup no Windows](./windows-setup.md) | [Índice da Documentação](./README.md)

## Quick Start

```bash
# Iniciar todos os serviços
docker compose -f docker/docker-compose.yml up -d

# Verificar status
docker compose -f docker/docker-compose.yml ps

# Parar serviços
docker compose -f docker/docker-compose.yml down

# Parar e remover volumes (começar do zero)
docker compose -f docker/docker-compose.yml down -v
```

## Serviços

| Serviço | Container | Porta | Descrição |
|---------|-----------|-------|-----------|
| Kafka Broker | datagen-broker | 9092, 29092 | Message broker (KRaft mode, `cp-server`) |
| Schema Registry | datagen-schema-registry | 8081 | Gerenciamento de schemas Avro/JSON/Protobuf |
| Kafka Connect | datagen-connect | 8083 | Framework de integração de dados |
| Control Center | datagen-control-center | 9021 | Interface web para monitoramento (Next-Gen) |
| Prometheus | datagen-prometheus | 9090 | Backend de métricas para Control Center |
| Alertmanager | datagen-alertmanager | 9093 | Backend de alertas para Control Center |
| ksqlDB | datagen-ksqldb-server | 8088 | Engine de SQL streaming |
| REST Proxy | datagen-rest-proxy | 8082 | Interface HTTP para Kafka |
| PostgreSQL | datagen-postgres | 5432 | Banco de dados para dados mestres |

> **Nota**: O Control Center Legacy é incompatível com CP 8.1.1 (`message.timestamp.difference.max.ms` removido). Usamos o **Control Center Next-Gen 2.2.1** com Prometheus + Alertmanager como sidecars. O broker usa a imagem `cp-server` (enterprise) para o TelemetryReporter necessário pelo C3.

## URLs de Acesso

- **Control Center**: http://localhost:9021
- **Prometheus**: http://localhost:9090
- **Schema Registry API**: http://localhost:8081
- **Kafka Connect API**: http://localhost:8083
- **ksqlDB API**: http://localhost:8088
- **REST Proxy API**: http://localhost:8082

## Arquitetura

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          datagen-network                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────────┐        │
│  │    Broker    │◄────►│   Schema     │      │ Control Center   │        │
│  │  (cp-server) │      │  Registry    │      │  Next-Gen :9021  │        │
│  │ :9092/29092  │      │    :8081     │      └────────┬─────────┘        │
│  └──────┬───────┘      └──────┬───────┘               │                  │
│         │                     │                ┌───────┴───────┐          │
│         │  OTLP metrics       │                │               │          │
│         ├──────────────────►┌──┴───────────┐ ┌─┴────────────┐ │          │
│         │                   │  Prometheus  │ │ Alertmanager │ │          │
│         │                   │    :9090     │ │    :9093     │ │          │
│         │                   └──────────────┘ └──────────────┘ │          │
│         ├─────────────────────┤                                │          │
│         │                     │                                │          │
│         ▼                     ▼                                │          │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐│          │
│  │   Connect    │      │   ksqlDB     │      │  REST Proxy  ││          │
│  │    :8083     │      │    :8088     │      │    :8082     ││          │
│  └──────┬───────┘      └──────────────┘      └──────────────┘│          │
│         │                                                     │          │
│         │              ┌──────────────┐                       │          │
│         └─────────────►│  PostgreSQL  │                       │          │
│                        │    :5432     │                       │          │
│                        └──────────────┘                       │          │
└──────────────────────────────────────────────────────────────────────────┘
```

## Perfis de Inicialização

Nem sempre é necessário iniciar todos os serviços. Escolha o perfil adequado:

### Mínimo (Kafka + Postgres)

Para ingestão de dados com `load_data.py`:

```bash
docker compose -f docker/docker-compose.yml up -d broker schema-registry postgres
```

### Monitoramento

Adicione Control Center para visualizar brokers, tópicos, schemas e consumidores:

```bash
docker compose -f docker/docker-compose.yml up -d broker schema-registry postgres prometheus alertmanager control-center
```

### Completo

Todos os serviços, incluindo Connect, ksqlDB e REST Proxy:

```bash
docker compose -f docker/docker-compose.yml up -d
```

## Limpeza de Containers

### Parar serviços (manter dados)

```bash
docker compose -f docker/docker-compose.yml down
```

### Parar e remover volumes (reset total)

Remove todos os dados do Kafka, PostgreSQL e plugins do Connect:

```bash
docker compose -f docker/docker-compose.yml down -v
```

### Remover containers, volumes e imagens

Limpeza completa, incluindo imagens baixadas:

```bash
docker compose -f docker/docker-compose.yml down -v --rmi all
```

### Remover apenas dados do PostgreSQL

```bash
docker compose -f docker/docker-compose.yml stop postgres
docker volume rm docker_postgres-data
docker compose -f docker/docker-compose.yml up -d postgres
```

### Remover apenas dados do Kafka

```bash
docker compose -f docker/docker-compose.yml stop broker
docker volume rm docker_broker-data
docker compose -f docker/docker-compose.yml up -d broker
```

### Limpar tudo (containers + rede + cache)

Script de limpeza completa:

```bash
# Parar tudo, remover volumes e rede
docker compose -f docker/docker-compose.yml down -v
docker network rm datagen-network 2>/dev/null || true

# Opcional: remover imagens do Confluent Platform
docker images | grep confluentinc | awk '{print $3}' | xargs -r docker rmi

# Verificar que está limpo
docker ps -a --filter "name=datagen-" --format "{{.Names}}"
```

## Kafka Connect

### Conectores Pré-instalados

O container do Connect instala automaticamente estes conectores na inicialização:

| Conector | Versão | Caso de Uso |
|----------|--------|-------------|
| JDBC Source/Sink | 10.7.6 | Integração com banco de dados |
| Debezium PostgreSQL | 2.5.0 | CDC (Change Data Capture) |
| Elasticsearch | 14.1.0 | Indexação de busca |
| JSON Schema Converter | 7.9.5 | Suporte a JSON schema |

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
      "incrementing.column.name": "incremental_id",
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

### Listar Schemas

```bash
# Listar todos os subjects
curl -s http://localhost:8081/subjects | jq

# Obter schema para subject
curl -s http://localhost:8081/subjects/banking.transactions-value/versions/latest | jq

# Obter schema por ID
curl -s http://localhost:8081/schemas/ids/1 | jq
```

### Compatibilidade

```bash
# Obter nível de compatibilidade global
curl -s http://localhost:8081/config | jq

# Definir compatibilidade para subject
curl -X PUT http://localhost:8081/config/banking.transactions-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

## Tópicos Kafka

### Criar Tópico

```bash
docker exec datagen-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic banking.transactions \
  --partitions 3 \
  --replication-factor 1
```

### Listar Tópicos

```bash
docker exec datagen-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

### Verificar Offsets (contagem de mensagens)

```bash
docker exec datagen-broker kafka-get-offsets \
  --bootstrap-server localhost:9092 \
  --topic-partitions banking.transactions:0,banking.transactions:1,banking.transactions:2
```

### Consumir Mensagens

```bash
# Desde o início (primeiras 5 mensagens)
docker exec datagen-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic banking.transactions \
  --from-beginning \
  --max-messages 5

# Últimas mensagens de uma partição específica
docker exec datagen-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic banking.transactions \
  --partition 0 \
  --offset latest \
  --max-messages 1
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

-- Criar stream a partir de tópico de transações
CREATE STREAM transactions_stream (
  transaction_id VARCHAR,
  account_id VARCHAR KEY,
  transaction_type VARCHAR,
  amount DOUBLE,
  direction VARCHAR,
  timestamp VARCHAR,
  status VARCHAR
) WITH (
  KAFKA_TOPIC='banking.transactions',
  VALUE_FORMAT='JSON'
);

-- Consultar stream em tempo real
SELECT * FROM transactions_stream EMIT CHANGES;

-- Agregação: total por tipo de transação
CREATE TABLE transaction_totals AS
  SELECT transaction_type,
         COUNT(*) AS total_count,
         SUM(amount) AS total_amount
  FROM transactions_stream
  GROUP BY transaction_type
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

### URL de Conexão

```
# Python (psycopg)
postgresql://postgres:postgres@localhost:5432/datagen

# JDBC (Kafka Connect)
jdbc:postgresql://localhost:5432/datagen

# Dentro da rede Docker
jdbc:postgresql://postgres:5432/datagen
```

## Troubleshooting

### Verificar Saúde dos Serviços

```bash
# Todos os serviços
docker compose -f docker/docker-compose.yml ps

# Logs de serviço específico
docker compose -f docker/docker-compose.yml logs -f broker
docker compose -f docker/docker-compose.yml logs -f connect
docker compose -f docker/docker-compose.yml logs -f schema-registry
```

### Problemas Comuns

#### Broker Não Iniciando

```bash
# Verificar logs do broker (KRaft mode)
docker compose -f docker/docker-compose.yml logs broker

# Verificar conectividade
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

Aumente a alocação de memória do Docker para pelo menos 6GB para a stack completa.

```bash
# Verificar uso atual de recursos
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
```

## Requisitos de Recursos

| Perfil | Memória | CPU | Serviços |
|--------|---------|-----|----------|
| Mínimo | 3GB | 2 cores | Broker, Schema Registry, PostgreSQL |
| Monitoramento | 5GB | 4 cores | + Prometheus, Alertmanager, Control Center |
| Completo | 6GB+ | 4+ cores | + Connect, ksqlDB, REST Proxy |

## Variáveis de Ambiente

### Broker (KRaft mode)

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `KAFKA_NODE_ID` | 1 | Identificador único do nó |
| `KAFKA_PROCESS_ROLES` | broker,controller | Papéis KRaft do nó |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | true | Auto-criar tópicos |
| `KAFKA_LOG_RETENTION_HOURS` | 168 | Retenção de logs (7 dias) |
| `CLUSTER_ID` | MkU3OEVBNTcwNTJENDM2Qk | ID do cluster KRaft |

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

### PostgreSQL

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `POSTGRES_USER` | postgres | Usuário do banco |
| `POSTGRES_PASSWORD` | postgres | Senha do banco |
| `POSTGRES_DB` | datagen | Nome do banco de dados |

O PostgreSQL está configurado com flags de performance para bulk loading:

| Flag | Valor | Descrição |
|------|-------|-----------|
| `shared_buffers` | 256MB | Memória para cache de páginas |
| `work_mem` | 64MB | Memória por operação de sort |
| `maintenance_work_mem` | 128MB | Memória para VACUUM, CREATE INDEX |
| `max_wal_size` | 1GB | WAL antes de checkpoint |
| `synchronous_commit` | off | Commits assíncronos (mais rápido) |
| `fsync` | off | Sem sync em disco (mais rápido, seguro em Docker) |
| `full_page_writes` | off | Sem backup de páginas (seguro com fsync=off) |
| `checkpoint_timeout` | 1800s | 30min entre checkpoints |

> **Aviso**: `fsync=off` e `full_page_writes=off` são seguros em containers de desenvolvimento. **Nunca use em produção** — pode causar perda de dados em crash.

### Control Center Next-Gen

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `CONTROL_CENTER_PROMETHEUS_ENABLE` | true | Habilita backend Prometheus |
| `CONTROL_CENTER_PROMETHEUS_URL` | http://prometheus:9090 | URL do Prometheus |
| `CONTROL_CENTER_ALERTMANAGER_URL` | http://alertmanager:9093 | URL do Alertmanager |
| `PORT` | 9021 | Porta HTTP do Control Center |

Configurações do C3 ficam em `docker/config/`:
- `prometheus-generated.yml` — scraping, OTLP, storage
- `recording_rules-generated.yml` — pre-agregações para dashboards
- `trigger_rules-generated.yml` — regras de alerta
- `alertmanager-generated.yml` — configuração de notificações

## Volumes

| Volume | Serviço | Propósito |
|--------|---------|-----------|
| `broker-data` | Broker | Logs do Kafka |
| `connect-plugins` | Connect | JARs de conectores |
| `postgres-data` | PostgreSQL | Arquivos do banco de dados |
