# Guia de Ingestão de Dados

Este guia explica como ingerir dados bancários sintéticos no PostgreSQL e Kafka usando a plataforma `data-gen`.

> **Docs relacionados**: [Catálogo de Dados](./data-catalog.md) | [Infraestrutura Docker](./docker.md) | [Índice da Documentação](./README.md)

## Visão Geral

O gerador de dados cria dados bancários brasileiros realistas com dois destinos de saída:

- **PostgreSQL**: Dados mestres/referência (clientes, contas, empréstimos, etc.)
- **Kafka**: Streams de eventos (transações, trades, parcelas)

Todos os dados mantêm integridade referencial entre os dois sistemas através do `FinancialDataStore`, que valida todas as chaves estrangeiras em memória antes de enviar dados para qualquer destino.

## Arquitetura e Relacionamentos

### Fluxo de Dados

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Gerador de Dados                                  │
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │  Geradores   │───►│ DataStore    │───►│    Sinks     │              │
│  │              │    │ (valida      │    │              │              │
│  │ - Customer   │    │  refs FK)    │    │ - PostgreSQL │              │
│  │ - Account    │    │              │    │ - Kafka      │              │
│  │ - Transaction│    │              │    │              │              │
│  │ - Trade      │    │              │    │              │              │
│  │ - etc.       │    │              │    │              │              │
│  └──────────────┘    └──────────────┘    └──────┬───────┘              │
│                                                  │                      │
└──────────────────────────────────────────────────┼──────────────────────┘
                                                   │
                    ┌──────────────────────────────┼──────────────────────┐
                    │                              │                      │
                    ▼                              ▼                      │
         ┌──────────────────┐           ┌──────────────────┐             │
         │    PostgreSQL    │           │      Kafka       │             │
         │  (dados mestres) │           │  (event streams) │             │
         │                  │           │                  │             │
         │ - customers      │           │ - transactions   │             │
         │ - accounts       │◄─── FK ───│ - card_trans     │             │
         │ - credit_cards   │  refs     │ - trades         │             │
         │ - loans          │           │ - installments   │             │
         │ - properties     │           │                  │             │
         │ - stocks         │           │                  │             │
         └──────────────────┘           └──────────────────┘             │
                                                                          │
                    └─────────────────────────────────────────────────────┘
```

### Relacionamentos entre Kafka e PostgreSQL

Cada evento no Kafka referencia uma entidade mestra no PostgreSQL. A integridade é garantida pelo `FinancialDataStore`:

| Tópico Kafka | Campo FK | Tabela PostgreSQL | Validação |
|---|---|---|---|
| `banking.transactions` | `account_id` | `accounts` | Conta deve existir |
| `banking.card-transactions` | `card_id` | `credit_cards` | Cartão deve existir |
| `banking.trades` | `account_id` | `accounts` | Conta tipo INVESTIMENTOS |
| `banking.trades` | `stock_id` | `stocks` | Ação deve existir |
| `banking.installments` | `loan_id` | `loans` | Empréstimo deve existir |

### Cadeia de FKs no PostgreSQL

As tabelas mestres também possuem relacionamentos entre si:

```
customers
    ├── accounts       (customer_id → customers.customer_id)
    ├── credit_cards   (customer_id → customers.customer_id)
    └── loans          (customer_id → customers.customer_id)
                           └── property_id → properties.property_id (para HOUSING)
```

A inserção no PostgreSQL respeita essa ordem de FK: `customers → properties → stocks → accounts → credit_cards → loans`.

### Por Que os Dados se Relacionam

O `scripts/load_data.py` usa um **único** `FinancialDataStore` em memória como fonte de verdade:

1. **Geradores** criam entidades e as adicionam ao store
2. O store **valida FKs** em cada `add_*()` — lança `ReferentialIntegrityError` se inválido
3. **PostgreSQL** recebe os dados mestres do store
4. **Kafka** recebe os eventos do store — que referenciam as mesmas entidades mestras

Com uma **seed determinística**, executar o script múltiplas vezes com os mesmos parâmetros (`--seed 42 --customers 100`) sempre gera os mesmos IDs, garantindo alinhamento entre os sistemas.

## Quick Start

### Pré-requisitos

1. Docker rodando com Confluent Platform:
   ```bash
   docker compose -f docker/docker-compose.yml up -d
   ```

2. Dependências Python:
   ```bash
   pip install -e ".[dev]"
   ```

### Uso Básico

```bash
# Carregar 100 clientes em PostgreSQL e Kafka
.venv/bin/python scripts/load_data.py --customers 100 --seed 42 --create-topics

# Reexecutar com dados limpos (trunca PostgreSQL antes de inserir)
.venv/bin/python scripts/load_data.py --customers 100 --seed 42 --create-topics --truncate
```

## Opções de Linha de Comando

### `load_data.py` — Carga Padrão

```bash
python scripts/load_data.py [OPÇÕES]
```

| Opção | Padrão | Descrição |
|-------|--------|-----------|
| `--customers` | 100 | Número de clientes a gerar |
| `--seed` | 42 | Seed aleatório para reprodutibilidade |
| `--postgres-url` | `postgresql://postgres:postgres@localhost:5432/datagen` | String de conexão PostgreSQL |
| `--kafka-bootstrap` | `localhost:9092` | Servidores bootstrap do Kafka |
| `--schema-registry` | `http://localhost:8081` | URL do Schema Registry |
| `--skip-postgres` | false | Pular carga no PostgreSQL |
| `--skip-kafka` | false | Pular carga no Kafka |
| `--create-topics` | false | Criar tópicos Kafka antes de carregar |
| `--truncate` | false | Truncar tabelas PostgreSQL antes de inserir (permite reexecução) |
| `--no-cloudevents` | false | Desabilitar headers CloudEvents nas mensagens Kafka |
| `--fast` | false | Modo rápido: COPY + BULK + streaming (auto-habilitado para 10K+ clientes) |
| `--no-avro` | false | Desabilitar serialização Avro (usa JSON, mensagens ~3-4x maiores) |
| `--retention-hours` | 8 | Retenção dos tópicos Kafka em horas |

> **Dica**: O modo `--fast` é automaticamente habilitado para `--customers >= 10000`. Ele usa `MasterDataStore` (sem eventos em memória), `COPY` no PostgreSQL e `BULK` preset no Kafka.

### `load_data_parallel.py` — Carga Paralela (100K+ clientes)

Para volumes acima de 100K clientes, use o script paralelo que distribui a geração de eventos entre múltiplos workers:

```bash
python scripts/load_data_parallel.py [OPÇÕES]
```

| Opção | Padrão | Descrição |
|-------|--------|-----------|
| `--customers` | 10000 | Número de clientes a gerar |
| `--seed` | 42 | Seed aleatório para reprodutibilidade |
| `--workers` | 0 (= CPU count) | Número de workers paralelos |
| `--postgres-url` | `postgresql://postgres:postgres@localhost:5432/datagen` | String de conexão PostgreSQL |
| `--kafka-bootstrap` | `localhost:9092` | Servidores bootstrap do Kafka |
| `--schema-registry` | `http://localhost:8081` | URL do Schema Registry |
| `--skip-postgres` | false | Pular carga no PostgreSQL |
| `--skip-kafka` | false | Pular carga no Kafka |
| `--create-topics` | false | Criar tópicos Kafka antes de carregar |
| `--truncate` | false | Truncar tabelas PostgreSQL antes de inserir |
| `--no-cloudevents` | false | Desabilitar headers CloudEvents |
| `--no-avro` | false | Desabilitar serialização Avro (usa JSON) |
| `--retention-hours` | 8 | Retenção dos tópicos Kafka em horas |

Exemplo:
```bash
# 100K clientes com 8 workers
python scripts/load_data_parallel.py --customers 100000 --workers 8 --create-topics --truncate

# 1M clientes (usa todos os cores disponíveis)
python scripts/load_data_parallel.py --customers 1000000 --create-topics --truncate
```

### `benchmark.py` — Benchmark de Performance

Mede a taxa de geração, inserção no PostgreSQL e produção no Kafka:

```bash
python scripts/benchmark.py [OPÇÕES]
```

| Opção | Padrão | Descrição |
|-------|--------|-----------|
| `--scale` | 1000 | Número de clientes para benchmark |
| `--seed` | 42 | Seed aleatório |
| `--skip-postgres` | false | Pular benchmarks PostgreSQL |
| `--skip-kafka` | false | Pular benchmarks Kafka |
| `--postgres-url` | `postgresql://postgres:postgres@localhost:5432/datagen` | String de conexão PostgreSQL |
| `--kafka-bootstrap` | `localhost:9092` | Servidores bootstrap do Kafka |
| `--schema-registry` | `http://localhost:8081` | URL do Schema Registry |

## Estratégias de Ingestão

### Estratégia 1: Ambos de Uma Vez (Recomendado)

Carrega dados mestres no PostgreSQL e eventos no Kafka em uma única execução:

```bash
# Iniciar infraestrutura
docker compose -f docker/docker-compose.yml up -d

# Carregar 100 clientes (modo padrão)
python scripts/load_data.py --customers 100 --seed 42 --create-topics --truncate

# Carregar 10K clientes (modo rápido, auto-habilitado)
python scripts/load_data.py --customers 10000 --seed 42 --create-topics --truncate
```

### Estratégia 2: Kafka Primeiro, PostgreSQL Depois

Útil quando se quer testar consumidores Kafka antes de configurar o banco:

```bash
# Passo 1: Carregar apenas Kafka (master data gerada em memória para validação FK)
.venv/bin/python scripts/load_data.py \
  --customers 100 --seed 42 --skip-postgres --create-topics

# Passo 2: Verificar dados no Kafka
docker exec datagen-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic banking.transactions \
  --from-beginning --max-messages 3

# Passo 3: Quando pronto, carregar PostgreSQL com mesma seed
docker compose -f docker/docker-compose.yml up -d postgres

.venv/bin/python scripts/load_data.py \
  --customers 100 --seed 42 --skip-kafka --truncate
```

A mesma seed (`42`) + mesmo número de clientes (`100`) garante que os IDs no PostgreSQL coincidem com os IDs referenciados nos eventos Kafka.

### Estratégia 3: Apenas PostgreSQL

Para testes de schema, queries SQL ou conectores JDBC:

```bash
.venv/bin/python scripts/load_data.py \
  --customers 500 --seed 42 --skip-kafka --truncate
```

### Estratégia 4: Apenas Kafka

Para testes de stream processing, ksqlDB ou consumidores:

```bash
.venv/bin/python scripts/load_data.py \
  --customers 500 --seed 42 --skip-postgres --create-topics
```

## Distribuição de Dados

### PostgreSQL (Dados Mestres)

| Tabela | Descrição | Proporção |
|--------|-----------|-----------|
| `customers` | Clientes do banco | 1:1 com input |
| `accounts` | Contas bancárias | ~1.5 por cliente |
| `credit_cards` | Cartões de crédito | 70% dos clientes |
| `loans` | Contratos de empréstimo | 35% dos clientes |
| `properties` | Imóveis | 10% dos clientes |
| `stocks` | Referência B3 | 42 ações fixas |

### Kafka (Streams de Eventos)

| Tópico | Chave da Mensagem | Eventos/Entidade | Descrição |
|--------|-------------------|------------------|-----------|
| `banking.transactions` | `account_id` | ~20/cliente | Pix, TED, depósitos, saques |
| `banking.card-transactions` | `card_id` | ~57/portador | Compras no cartão |
| `banking.trades` | `account_id` | ~20/investidor | Ordens de compra/venda de ações |
| `banking.installments` | `loan_id` | ~30/empréstimo | Eventos de pagamento de parcelas |

### Volumes de Eventos Esperados

| Clientes | Transações | Trans. Cartão | Trades | Parcelas | Total Eventos |
|----------|------------|---------------|--------|----------|---------------|
| 100 | ~2.000 | ~4.000 | ~620 | ~1.200 | ~7.800 |
| 500 | ~10.000 | ~20.000 | ~3.100 | ~6.000 | ~39.000 |
| 1.000 | ~20.000 | ~40.000 | ~6.200 | ~12.000 | ~78.000 |
| 10.000 | ~200.000 | ~400.000 | ~62.000 | ~120.000 | ~780.000 |

### Performance

O modo `--fast` (ativado automaticamente para 10K+ clientes) aplica múltiplas otimizações:

| Otimização | Descrição | Ganho |
|------------|-----------|-------|
| `MasterDataStore` | Eventos não ficam em memória | 10x menos RAM |
| PostgreSQL `COPY` | Protocolo COPY ao invés de INSERT | 5-10x mais rápido |
| Carga paralela | 6 tabelas em paralelo via ThreadPoolExecutor | 2-3x mais rápido |
| Kafka `BULK` preset | Batches de 512KB, linger 100ms, compressão LZ4 | 3-5x mais rápido |
| Serialização rápida | `getattr()` ao invés de `asdict()` (sem deep copy) | 2-3x mais rápido |

Benchmarks de referência (Docker Desktop macOS, Apple Silicon):

**10K clientes** (~806K linhas):

| Componente | Tempo | Taxa |
|------------|-------|------|
| Geração de dados | ~19s | ~5K clientes/sec |
| PostgreSQL (37K master rows, COPY) | ~2s | ~175K rows/sec |
| Kafka (770K events, Avro+BULK) | ~294s | ~26K events/sec |
| **Total** | **~316s** | **~25K rows/sec** |

**100K clientes** (~8M linhas):

| Componente | Linhas | Tempo | Taxa |
|------------|--------|-------|------|
| Geração de dados | — | ~19s | ~5K clientes/sec |
| PostgreSQL (365K master rows, COPY) | 364.677 | ~2s | ~185K rows/sec |
| Kafka transactions | 2.020.866 | ~113s | ~18K/sec |
| Kafka card-transactions | 4.000.098 | ~141s | ~28K/sec |
| Kafka trades | 649.980 | ~20s | ~32K/sec |
| Kafka installments | 991.650 | ~20s | ~50K/sec |
| **Total** | **8.027.271** | **~317s (5m17s)** | **~25K rows/sec** |

> **Nota**: O bottleneck é Kafka (Avro serialization + network I/O). PostgreSQL com COPY é extremamente rápido (~185K rows/sec).

## Configuração do Kafka

### Tópicos

Os seguintes tópicos são criados (com `--create-topics`):

**`load_data.py`** (carga padrão — 3 partições):

| Tópico | Partições | Replicação | Chave |
|--------|-----------|------------|-------|
| `banking.transactions` | 3 | 1 | `account_id` |
| `banking.card-transactions` | 3 | 1 | `card_id` |
| `banking.trades` | 3 | 1 | `account_id` |
| `banking.installments` | 3 | 1 | `loan_id` |

**`load_data_parallel.py`** (carga paralela — 6 partições para tópicos de alto volume):

| Tópico | Partições | Replicação | Chave |
|--------|-----------|------------|-------|
| `banking.transactions` | 6 | 1 | `account_id` |
| `banking.card-transactions` | 6 | 1 | `card_id` |
| `banking.trades` | 3 | 1 | `account_id` |
| `banking.installments` | 3 | 1 | `loan_id` |

> **Nota**: O script paralelo usa 6 partições para transactions e card-transactions para permitir paralelismo entre os workers. Se os tópicos já existem com 3 partições, o script reutiliza-os.

### CloudEvents

Por padrão, todas as mensagens incluem headers CloudEvents (Binary Content Mode):

| Header | Descrição | Exemplo |
|--------|-----------|---------|
| `ce_specversion` | Versão da spec | `1.0` |
| `ce_type` | Tipo do evento | `banking.transaction.created` |
| `ce_source` | Sistema de origem | `data-gen/financial` |
| `ce_id` | ID único do evento | UUID v4 |
| `ce_time` | Timestamp ISO 8601 | `2024-03-15T14:23:45Z` |
| `ce_subject` | Chave da entidade | `acct-123-...` |

Desabilitar com `--no-cloudevents`.

### Serialização

Os dados são serializados usando **Avro** com schemas registrados no Schema Registry:

- `banking.transactions-value`
- `banking.card-transactions-value`
- `banking.trades-value`
- `banking.installments-value`

Se o Schema Registry não estiver disponível ou `confluent-kafka[avro]` não estiver instalado, faz fallback para serialização JSON com warning.

### Configuração do Producer

```python
from data_gen.sinks.kafka import KafkaSink, ProducerConfig

config = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
    acks="all",                         # "0", "1", "all"
    batch_size=16384,                   # bytes
    linger_ms=5,                        # ms para aguardar batching
    compression="snappy",               # none, gzip, snappy, lz4
    retries=3,
    enable_idempotence=False,           # produtor idempotente (requer acks="all")
    queue_buffering_max_messages=100000, # máximo de mensagens na fila interna
    queue_buffering_max_kbytes=1048576,  # 1GB - tamanho máximo da fila interna
)
sink = KafkaSink(config)
```

### Presets do Producer

O sistema oferece 4 presets pré-configurados para diferentes cenários:

| Preset | `acks` | `batch_size` | `linger_ms` | `compression` | Caso de Uso |
|--------|--------|-------------|-------------|---------------|-------------|
| `RELIABLE` | `all` | 16KB | 5ms | snappy | Produção: sem perda de mensagens, idempotente |
| `FAST` | `0` | 64KB | 50ms | snappy | Desenvolvimento: máxima velocidade, sem garantia |
| `BULK` | `1` | 512KB | 100ms | lz4 | Carga em massa: batches grandes, fila de 500K msgs |
| `EVENT_BY_EVENT` | `all` | 1 | 0ms | none | Debug: uma mensagem por vez |

```python
from data_gen.sinks.kafka import KafkaSink, RELIABLE, FAST, BULK

# Produção — idempotente, sem duplicatas
sink = KafkaSink(RELIABLE)

# Carga em massa — auto-usado pelo modo --fast
sink = KafkaSink(BULK)

# Desenvolvimento — máxima velocidade
sink = KafkaSink(FAST)
```

> **Nota**: O preset `BULK` é automaticamente usado pelo modo `--fast`. Ele configura `enable.idempotence=false`, `acks=1` (leader only), batches de 512KB com compressão LZ4, e fila interna de 500K mensagens / 2GB.

### Resiliência do Producer

O `KafkaSink` implementa mecanismos de resiliência para cargas de alto volume:

**BufferError handling**: Quando a fila interna do producer está cheia, o `send()` captura o `BufferError`, drena callbacks via `poll(1.0)`, e tenta novamente:

```python
try:
    producer.produce(**kwargs)
except BufferError:
    producer.poll(1.0)  # drena a fila
    producer.produce(**kwargs)  # retry
```

**Flush timeout escalável**: O `close()` escala o timeout do flush baseado no volume enviado — 30s base + 1s por 10K mensagens, com cap em 300s. Isso previne perda silenciosa de dados em cargas grandes:

```
1K mensagens  → 30.1s timeout
100K mensagens → 40s timeout
500K mensagens → 80s timeout
10M mensagens  → 300s timeout (cap)
```

**Poll batched**: Em vez de `poll(0)` após cada `produce()`, o producer agrupa polls a cada 10.000 mensagens, reduzindo overhead de 3-5x.

## Schema do PostgreSQL

### Tabelas Criadas

```sql
-- Tabelas de dados mestres (criadas automaticamente)
customers       -- Clientes do banco
accounts        -- Contas bancárias (FK → customers)
credit_cards    -- Cartões de crédito (FK → customers)
loans           -- Contratos de empréstimo (FK → customers, properties)
properties      -- Imóveis para financiamento habitacional
stocks          -- Dados de referência de ações B3
```

### Formato da String de Conexão

```
postgresql://[usuario]:[senha]@[host]:[porta]/[banco]
```

Exemplos:
```bash
# Desenvolvimento local
postgresql://postgres:postgres@localhost:5432/datagen

# Servidor remoto
postgresql://admin:secret@db.example.com:5432/banking

# Com SSL
postgresql://admin:secret@db.example.com:5432/banking?sslmode=require
```

## Verificação

### Verificar Dados no PostgreSQL

```bash
# Conectar ao banco
docker exec -it datagen-postgres psql -U postgres -d datagen

# Contar registros por tabela
SELECT 'customers' as tabela, count(*) FROM customers
UNION ALL SELECT 'accounts', count(*) FROM accounts
UNION ALL SELECT 'credit_cards', count(*) FROM credit_cards
UNION ALL SELECT 'loans', count(*) FROM loans
UNION ALL SELECT 'properties', count(*) FROM properties
UNION ALL SELECT 'stocks', count(*) FROM stocks
ORDER BY tabela;

# Verificar relacionamentos FK (cliente com suas contas e cartões)
SELECT c.customer_id, c.name,
       count(DISTINCT a.account_id) as contas,
       count(DISTINCT cc.card_id) as cartoes
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id
LEFT JOIN credit_cards cc ON cc.customer_id = c.customer_id
GROUP BY c.customer_id, c.name
LIMIT 10;
```

### Verificar Dados no Kafka

```bash
# Listar tópicos
docker exec datagen-broker kafka-topics --bootstrap-server localhost:9092 --list

# Verificar offsets (contagem de mensagens por partição)
docker exec datagen-broker kafka-get-offsets \
  --bootstrap-server localhost:9092 \
  --topic-partitions banking.transactions:0,banking.transactions:1,banking.transactions:2

# Consumir mensagens de exemplo
docker exec datagen-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic banking.transactions \
  --from-beginning \
  --max-messages 3
```

### Validação Automática

O `load_data.py` e `load_data_parallel.py` executam validação automática ao final de cada carga:

1. **PostgreSQL**: `SELECT COUNT(*)` em cada tabela, comparado com o número de registros gerados
2. **Kafka**: Compara delta de offsets (snapshot antes vs depois da produção) com o número de eventos enviados

```
Validating PostgreSQL row counts...
  [OK] customers: 1000 rows
  [OK] accounts: 1484 rows
  [OK] credit_cards: 700 rows
  [OK] loans: 350 rows
  [OK] properties: 100 rows
  [OK] stocks: 42 rows
Validating Kafka topic offsets...
  [OK] banking.transactions: 20031 events
  [OK] banking.card-transactions: 39973 events
  [OK] banking.trades: 6260 events
  [OK] banking.installments: 9582 events
Validation: ALL OK
```

A validação é delta-based — snapshots de offsets Kafka são capturados antes de produzir, portanto dados pré-existentes nos tópicos não causam falsos positivos.

> **Overhead**: ~75ms para PG (6 queries) + Kafka (12 watermark offsets). Desprezível.

### Verificação Cruzada (Kafka → PostgreSQL)

Confirmar que os IDs nos eventos Kafka existem no PostgreSQL:

```bash
# 1. Pegar um account_id de uma transação Kafka
docker exec datagen-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic banking.transactions \
  --from-beginning --max-messages 1

# 2. Verificar que esse account_id existe no PostgreSQL
docker exec datagen-postgres psql -U postgres -d datagen \
  -c "SELECT account_id, customer_id, account_type, status
      FROM accounts
      WHERE account_id = '<account_id_do_passo_1>';"
```

### Verificar Schema Registry

```bash
# Listar schemas registrados
curl -s http://localhost:8081/subjects | jq

# Obter schema para transactions
curl -s http://localhost:8081/subjects/banking.transactions-value/versions/latest | jq
```

## Troubleshooting

### Problemas Comuns

#### 1. Conexão Recusada (PostgreSQL)

```
psycopg.OperationalError: connection refused
```

**Solução**: Verifique se o PostgreSQL está rodando:
```bash
docker compose -f docker/docker-compose.yml ps postgres
docker compose -f docker/docker-compose.yml logs postgres
```

#### 2. Conexão Recusada (Kafka)

```
KafkaException: Failed to create producer
```

**Solução**: Verifique se o Kafka está rodando e saudável:
```bash
docker compose -f docker/docker-compose.yml ps broker
docker exec datagen-broker kafka-broker-api-versions --bootstrap-server localhost:9092
```

#### 3. Schema Registry Não Disponível

```
WARNING: confluent-kafka[avro] not installed. Using JSON serialization.
```

**Solução**: Instale suporte a Avro ou verifique se o Schema Registry está rodando:
```bash
pip install 'confluent-kafka[avro]'
curl http://localhost:8081/subjects
```

#### 4. Violação de Chave Estrangeira no PostgreSQL

```
psycopg.errors.ForeignKeyViolation: insert or update violates foreign key constraint
```

**Solução**: Use `--truncate` para limpar tabelas antes de reinserir. O script `load_data.py` garante a ordem correta de inserção automaticamente.

#### 5. Dados Duplicados no Kafka

Cada execução do `load_data.py` **adiciona** novas mensagens ao Kafka (tópicos são append-only). Para começar limpo:

```bash
# Opção 1: Deletar e recriar tópicos
docker exec datagen-broker kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic banking.transactions
docker exec datagen-broker kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic banking.card-transactions
docker exec datagen-broker kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic banking.trades
docker exec datagen-broker kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic banking.installments

# Opção 2: Reset total (remover volume do broker)
docker compose -f docker/docker-compose.yml stop broker
docker volume rm docker_broker-data
docker compose -f docker/docker-compose.yml up -d broker
```

### Logs e Debug

```bash
# Habilitar logging de debug
export LOG_LEVEL=DEBUG
python scripts/load_data.py --customers 10

# Ver logs do broker
docker compose -f docker/docker-compose.yml logs -f broker
```

## Documentação Relacionada

- [Infraestrutura Docker](./docker.md) - Setup do Confluent Platform
- [Catálogo de Dados](./data-catalog.md) - Referência completa de datasets e relacionamentos
- [Índice da Documentação](./README.md) - Toda a documentação
