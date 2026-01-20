# Guia de Ingestão de Dados

Este guia explica como ingerir dados bancários sintéticos no PostgreSQL e Kafka usando a plataforma `data-gen`.

## Visão Geral

O gerador de dados cria dados bancários brasileiros realistas com dois destinos de saída:

- **PostgreSQL**: Dados mestres/referência (clientes, contas, empréstimos, etc.)
- **Kafka**: Streams de eventos (transações, trades, parcelas)

Todos os dados mantêm integridade referencial entre os dois sistemas através de IDs compartilhados.

## Arquitetura

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

## Quick Start

### Pré-requisitos

1. Docker rodando com Confluent Platform:
   ```bash
   cd docker/
   docker-compose up -d
   ```

2. Dependências Python:
   ```bash
   pip install psycopg[binary] confluent-kafka[avro]
   ```

### Uso Básico

```bash
# Carregar 100 clientes (padrão)
python3 scripts/load_data.py

# Carregar 500 clientes com seed específico
python3 scripts/load_data.py --customers 500 --seed 42

# Criar tópicos Kafka antes de carregar
python3 scripts/load_data.py --customers 500 --create-topics
```

## Opções de Linha de Comando

### Referência Completa

```bash
python3 scripts/load_data.py [OPÇÕES]
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

### Exemplos

```bash
# Apenas PostgreSQL (sem Kafka)
python3 scripts/load_data.py --customers 1000 --skip-kafka

# Apenas Kafka (sem PostgreSQL)
python3 scripts/load_data.py --customers 1000 --skip-postgres

# Strings de conexão customizadas
python3 scripts/load_data.py \
  --postgres-url "postgresql://user:pass@db.example.com:5432/banking" \
  --kafka-bootstrap "kafka1.example.com:9092,kafka2.example.com:9092" \
  --schema-registry "http://schema-registry.example.com:8081"

# Dataset grande com seed reproduzível
python3 scripts/load_data.py --customers 10000 --seed 2024
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

| Tópico | Chave | Eventos/Entidade | Descrição |
|--------|-------|------------------|-----------|
| `banking.transactions` | `account_id` | ~20/cliente | Pix, TED, depósitos, saques |
| `banking.card-transactions` | `card_id` | ~57/portador | Compras no cartão |
| `banking.trades` | `account_id` | ~20/investidor | Ordens de compra/venda de ações |
| `banking.installments` | `loan_id` | ~30/empréstimo | Eventos de pagamento de parcelas |

### Volumes de Eventos Esperados

| Clientes | Transações | Trans. Cartão | Trades | Parcelas | Total Eventos |
|----------|------------|---------------|--------|----------|---------------|
| 100 | ~2.000 | ~4.000 | ~730 | ~1.050 | ~7.800 |
| 500 | ~10.000 | ~20.000 | ~3.600 | ~5.200 | ~39.000 |
| 1.000 | ~20.000 | ~40.000 | ~7.300 | ~10.500 | ~78.000 |
| 10.000 | ~200.000 | ~400.000 | ~73.000 | ~105.000 | ~780.000 |

## Configuração do Kafka

### Tópicos

Os seguintes tópicos são criados (com `--create-topics`):

| Tópico | Partições | Replicação | Chave |
|--------|-----------|------------|-------|
| `banking.transactions` | 3 | 1 | `account_id` |
| `banking.card-transactions` | 3 | 1 | `card_id` |
| `banking.trades` | 3 | 1 | `account_id` |
| `banking.installments` | 3 | 1 | `loan_id` |

### Serialização

Os dados são serializados usando **Avro** com schemas registrados no Schema Registry:

- `banking.transactions-value`
- `banking.card-transactions-value`
- `banking.trades-value`
- `banking.installments-value`

Se o Schema Registry não estiver disponível, faz fallback para serialização JSON.

### Configuração do Producer

O sink Kafka suporta três presets:

```python
from data_gen.sinks.kafka import RELIABLE, FAST, EVENT_BY_EVENT

# RELIABLE (padrão) - Melhor para produção
# acks=all, compression=snappy, batching habilitado
sink = KafkaSink(RELIABLE)

# FAST - Alto throughput, menos durabilidade
# acks=0, batches maiores, mais latência
sink = KafkaSink(FAST)

# EVENT_BY_EVENT - Tempo real, sem batching
# acks=all, sem batching, entrega imediata
sink = KafkaSink(EVENT_BY_EVENT)
```

Configuração customizada:

```python
from data_gen.sinks.kafka import KafkaSink, ProducerConfig

config = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
    acks="all",           # "0", "1", "all"
    batch_size=16384,     # bytes
    linger_ms=5,          # ms para aguardar batching
    compression="snappy", # none, gzip, snappy, lz4
    retries=3,
)
sink = KafkaSink(config)
```

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

## Uso Programático

### Exemplo Básico

```python
from data_gen.generators.financial import (
    CustomerGenerator,
    AccountGenerator,
    TransactionGenerator,
)
from data_gen.sinks.kafka import KafkaSink, RELIABLE
from data_gen.sinks.postgres import PostgresSink
from data_gen.store.financial import FinancialDataStore

# Inicializar store e geradores
store = FinancialDataStore()
customer_gen = CustomerGenerator(seed=42)
account_gen = AccountGenerator(seed=42)
transaction_gen = TransactionGenerator(seed=42)

# Gerar clientes
for _ in range(100):
    customer = customer_gen.generate()
    store.add_customer(customer)

# Gerar contas
for customer in store.customers.values():
    for account in account_gen.generate_for_customer(
        customer.customer_id,
        customer.created_at,
        customer.monthly_income,
    ):
        store.add_account(account)

# Carregar no PostgreSQL
postgres = PostgresSink("postgresql://postgres:postgres@localhost:5432/datagen")
postgres.create_tables()
postgres.write_batch("customers", list(store.customers.values()))
postgres.write_batch("accounts", list(store.accounts.values()))
postgres.close()

# Enviar transações para Kafka
kafka = KafkaSink(RELIABLE)
for account in store.accounts.values():
    for tx in transaction_gen.generate_for_account(account, store, start, end):
        store.add_transaction(tx)
        kafka.send("banking.transactions", tx)
kafka.close()
```

### Streaming com Controle de Taxa

```python
from data_gen.sinks.kafka import KafkaSink, RELIABLE

sink = KafkaSink(RELIABLE)

# Stream a 100 eventos/segundo por 10 minutos
stats = sink.write_stream(
    topic="banking.transactions",
    generator=transaction_generator(),  # Seu gerador
    rate_per_second=100,
    duration_seconds=600,
)

print(f"Enviados: {stats.sent}")
print(f"Entregues: {stats.delivered}")
print(f"Falhas: {stats.failed}")
print(f"Throughput: {stats.throughput:.1f} eventos/seg")
```

## Verificação

### Verificar Dados no PostgreSQL

```bash
# Conectar ao banco
docker exec -it datagen-postgres psql -U postgres -d datagen

# Contar registros
SELECT 'customers' as tabela, count(*) FROM customers
UNION ALL SELECT 'accounts', count(*) FROM accounts
UNION ALL SELECT 'credit_cards', count(*) FROM credit_cards
UNION ALL SELECT 'loans', count(*) FROM loans
UNION ALL SELECT 'stocks', count(*) FROM stocks;

# Verificar relacionamentos FK
SELECT c.customer_id, c.name, count(a.account_id) as contas
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.name
LIMIT 10;
```

### Verificar Dados no Kafka

```bash
# Listar tópicos
docker exec datagen-broker kafka-topics --bootstrap-server localhost:9092 --list

# Verificar offsets dos tópicos (contagem de mensagens)
docker exec datagen-broker kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic banking.transactions

# Consumir mensagens de exemplo
docker exec datagen-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic banking.transactions \
  --from-beginning \
  --max-messages 5

# Verificar lag dos consumidores
docker exec datagen-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --all-groups
```

### Verificar Schema Registry

```bash
# Listar schemas registrados
curl -s http://localhost:8081/subjects | jq

# Obter schema para transactions
curl -s http://localhost:8081/subjects/banking.transactions-value/versions/latest | jq

# Verificar compatibilidade
curl -s http://localhost:8081/config | jq
```

## Troubleshooting

### Problemas Comuns

#### 1. Conexão Recusada (PostgreSQL)

```
psycopg.OperationalError: connection refused
```

**Solução**: Verifique se o PostgreSQL está rodando:
```bash
docker-compose ps postgres
docker-compose logs postgres
```

#### 2. Conexão Recusada (Kafka)

```
KafkaException: Failed to create producer
```

**Solução**: Verifique se o Kafka está rodando e saudável:
```bash
docker-compose ps broker
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

#### 4. Violação de Chave Estrangeira

```
psycopg.errors.ForeignKeyViolation: insert or update violates foreign key constraint
```

**Solução**: Garanta que os dados são carregados na ordem correta. Use o script `load_data.py` que trata a ordenação automaticamente.

### Logs e Debug

```bash
# Habilitar logging de debug
export LOG_LEVEL=DEBUG
python3 scripts/load_data.py --customers 10

# Ver logs do Kafka Connect
docker-compose logs -f connect

# Ver logs do broker
docker-compose logs -f broker
```

## Tuning de Performance

### Para Alto Throughput

```python
from data_gen.sinks.kafka import KafkaSink, ProducerConfig

config = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
    acks="1",              # Não aguardar todas as réplicas
    batch_size=131072,     # Batches de 128KB
    linger_ms=100,         # Aguardar mais para batching
    compression="lz4",     # Compressão mais rápida
)
sink = KafkaSink(config)
```

### Para Baixa Latência

```python
from data_gen.sinks.kafka import KafkaSink, ProducerConfig

config = ProducerConfig(
    bootstrap_servers="localhost:9092",
    schema_registry_url="http://localhost:8081",
    acks="all",
    batch_size=1,          # Sem batching
    linger_ms=0,           # Enviar imediatamente
    compression="none",    # Sem overhead de compressão
)
sink = KafkaSink(config)
```

### Tamanho de Batch no PostgreSQL

Para datasets grandes, o sink PostgreSQL automaticamente faz batch dos inserts. Para controle customizado:

```python
from data_gen.sinks.postgres import PostgresSink

sink = PostgresSink("postgresql://...")

# Escrever em batches menores para eficiência de memória
records = list(store.customers.values())
batch_size = 1000
for i in range(0, len(records), batch_size):
    sink.write_batch("customers", records[i:i+batch_size])
```

## Documentação Relacionada

- [Infraestrutura Docker](./docker.md) - Setup do Confluent Platform
- [Catálogo de Dados](./data-catalog.md) - Referência completa de datasets
- [Índice da Documentação](./README.md) - Toda a documentação
