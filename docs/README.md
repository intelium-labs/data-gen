# Documentação

Bem-vindo à documentação do `data-gen`. Este gerador de dados sintéticos simula um **banco brasileiro** com modelos de dados realistas, geradores e múltiplos destinos de saída.

## Índice

| Documento | Descrição |
|-----------|-----------|
| [Catálogo de Dados](./data-catalog.md) | Referência completa de todos os datasets, campos, tipos e lógica de geração |
| [Ingestão de Dados](./data-ingestion.md) | Como carregar dados no PostgreSQL e Kafka |
| [Infraestrutura Docker](./docker.md) | Setup do Confluent Platform e configuração |
| [Setup no Windows](./windows-setup.md) | Guia completo para rodar no Windows com Docker Desktop e WSL2 |
| [Roadmap v2](./roadmap-v2.md) | Poison pills, padrões realistas, novos produtos e interface TUI |

## Links Rápidos

### Primeiros Passos

```bash
# 1. Iniciar infraestrutura
docker compose -f docker/docker-compose.yml up -d

# 2. Instalar dependências
pip install -e ".[dev]"

# 3. Carregar dados de exemplo (PostgreSQL + Kafka)
.venv/bin/python scripts/load_data.py --customers 100 --seed 42 --create-topics

# 4. Verificar dados no PostgreSQL
docker exec datagen-postgres psql -U postgres -d datagen -c "SELECT count(*) FROM customers;"

# 5. Verificar dados no Kafka
docker exec datagen-broker kafka-get-offsets --bootstrap-server localhost:9092 \
  --topic-partitions banking.transactions:0,banking.transactions:1,banking.transactions:2

# 6. Limpar tudo quando terminar
docker compose -f docker/docker-compose.yml down -v
```

### Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Plataforma data-gen                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   Modelos    │    │  Geradores   │    │    Sinks     │              │
│  │              │    │              │    │              │              │
│  │ - Customer   │───►│ - Customer   │───►│ - PostgreSQL │              │
│  │ - Account    │    │ - Account    │    │ - Kafka      │              │
│  │ - Transaction│    │ - Transaction│    │ - JSON       │              │
│  │ - CreditCard │    │ - CreditCard │    │ - Console    │              │
│  │ - Loan       │    │ - Loan       │    │              │              │
│  │ - Trade      │    │ - Trade      │    │              │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│                              │                   │                      │
│                              ▼                   │                      │
│                      ┌──────────────┐            │                      │
│                      │  DataStore   │◄───────────┘                      │
│                      │ (validação FK)                                    │
│                      └──────────────┘                                   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Fluxo de Dados

| Camada | Componente | Descrição |
|--------|------------|-----------|
| **Modelos** | `data_gen/models/` | Definições de dataclasses para todas as entidades |
| **Geradores** | `data_gen/generators/` | Criam dados sintéticos realistas |
| **Store** | `data_gen/store/` | Armazenamento em memória com validação de FK |
| **Sinks** | `data_gen/sinks/` | Saída para PostgreSQL, Kafka, JSON, etc. |
| **Cenários** | `data_gen/scenarios/` | Cenários pré-construídos de geração de dados |

### Entidades Suportadas

| Categoria | Entidade | Destino | Descrição |
|-----------|----------|---------|-----------|
| **Master** | Customer | PostgreSQL | Clientes do banco com CPF, renda, score de crédito |
| **Master** | Account | PostgreSQL | Contas corrente, poupança, investimentos |
| **Master** | CreditCard | PostgreSQL | Cartões de crédito com limites e bandeiras |
| **Master** | Loan | PostgreSQL | Empréstimos pessoais, imobiliários, veiculares |
| **Referência** | Property | PostgreSQL | Imóveis para financiamento habitacional |
| **Referência** | Stock | PostgreSQL | Ações listadas na B3 |
| **Evento** | Transaction | Kafka | Pix, TED, depósitos, saques |
| **Evento** | CardTransaction | Kafka | Compras no cartão de crédito |
| **Evento** | Trade | Kafka | Ordens de compra/venda de ações |
| **Evento** | Installment | Kafka | Eventos de pagamento de parcelas |

### Relacionamentos entre Sistemas

Dados mestres no PostgreSQL são referenciados por eventos no Kafka através de chaves estrangeiras:

| Evento (Kafka) | FK | Entidade Mestre (PostgreSQL) |
|---|---|---|
| `banking.transactions` | `account_id` | `accounts` |
| `banking.card-transactions` | `card_id` | `credit_cards` |
| `banking.trades` | `account_id`, `stock_id` | `accounts`, `stocks` |
| `banking.installments` | `loan_id` | `loans` |

A integridade é garantida pelo `FinancialDataStore` que valida todas as FKs em memória antes de enviar para qualquer destino. Veja [Catálogo de Dados](./data-catalog.md#relacionamentos-entre-sistemas) para detalhes.

## Convenções da Documentação

### Tipos de Campo

| Notação | Exemplo | Descrição |
|---------|---------|-----------|
| `UUID` | `bdd640fb-0667-...` | Identificador UUID v4 |
| `string(N)` | `string(14)` | String com tamanho máximo N |
| `decimal(P,S)` | `decimal(15,2)` | Decimal com precisão P, escala S |
| `enum` | `EMPLOYED` | Conjunto fixo de valores |
| `timestamp` | `2024-03-15T14:30:00` | Data/hora ISO 8601 |

### Formatos Brasileiros

| Dado | Formato | Exemplo |
|------|---------|---------|
| CPF | XXX.XXX.XXX-XX | 123.456.789-00 |
| CEP | XXXXX-XXX | 01310-100 |
| Telefone | +55 XX XXXXX-XXXX | +55 11 98765-4321 |
| Código Banco | XXX | 341 (Itaú) |
| Ticker B3 | XXXX(N) | PETR4, VALE3 |

## Recursos Relacionados

- **Código Fonte**: [github.com/intelium-labs/data-gen](https://github.com/intelium-labs/data-gen)
- **Confluent Platform**: [docs.confluent.io](https://docs.confluent.io)
- **PostgreSQL**: [postgresql.org/docs](https://www.postgresql.org/docs/)
