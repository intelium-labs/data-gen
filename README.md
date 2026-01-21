<p align="center">
  <strong>Plataforma de Geração de Dados Sintéticos para Múltiplos Domínios</strong>
</p>

<p align="center">
  Desenvolvido por <a href="https://github.com/intelium-labs">Intelium Labs</a>
</p>

---

## Sobre o Projeto

O **Data-Gen** é uma plataforma extensível de geração de dados sintéticos projetada para simular cenários realistas de diversos domínios de negócio. A plataforma gera dados com integridade referencial, distribuições estatísticas realistas e formatos brasileiros.

### Domínios Suportados

| Domínio | Status | Descrição |
|---------|--------|-----------|
| **Serviços Financeiros** | ✅ Disponível | Banco completo: clientes, contas, transações, cartões, empréstimos, B3 |
| **Varejo** | 🔜 Planejado | Lojas, produtos, vendas, estoque, promoções |
| **E-commerce** | 🔜 Planejado | Pedidos, carrinho, pagamentos, entregas, avaliações |
| **Telecomunicações** | 🔜 Planejado | Planos, consumo, faturas, atendimento |
| **Saúde** | 🔜 Planejado | Pacientes, consultas, exames, prontuários |
| **Logística** | 🔜 Planejado | Entregas, rotas, rastreamento, frotas |

### Casos de Uso

- **Demos e POCs** - Dados realistas para apresentações e provas de conceito
- **Testes de integração** - Dados consistentes e reproduzíveis para pipelines
- **Desenvolvimento** - Ambiente local sem dependência de dados de produção
- **Treinamento de modelos** - Datasets balanceados para ML/AI
- **Benchmarking** - Testes de performance com volumes controlados
- **Data Lakes** - População de ambientes de dados para análise

---

## Arquitetura de Sinks

O Data-Gen suporta múltiplos destinos de dados (sinks) para diferentes casos de uso:

### Disponíveis

| Sink | Tipo | Caso de Uso |
|------|------|-------------|
| **PostgreSQL** | OLTP | Dados mestres, tabelas relacionais |
| **Kafka** | Streaming | Eventos em tempo real, CDC |
| **JSON Files** | Arquivo | Exportação, backups, testes locais |
| **Console** | Debug | Desenvolvimento e debugging |

### Planejados

| Sink | Tipo | Caso de Uso | Prioridade |
|------|------|-------------|------------|
| **MongoDB** | NoSQL | Documentos, dados semi-estruturados | Alta |
| **Parquet/CSV** | Arquivo | Data Lake, análise batch | Alta |
| **S3/GCS/ADLS** | Cloud Storage | Data Lake na nuvem | Alta |
| **Apache Iceberg** | Table Format | Data Lakehouse | Média |
| **Delta Lake** | Table Format | Databricks, Spark | Média |
| **BigQuery** | Data Warehouse | Analytics GCP | Média |
| **Snowflake** | Data Warehouse | Analytics multi-cloud | Média |
| **Elasticsearch** | Search | Busca e observabilidade | Baixa |
| **Redis** | Cache | Cache de sessão, real-time | Baixa |

---

## Funcionalidades

- **Modelos de Domínio**: Entidades com relacionamentos e validações
- **Dados Realistas**: Formatos brasileiros (CPF, CNPJ, Pix, CEP), distribuições estatísticas
- **Múltiplos Destinos**: Streaming (Kafka), OLTP (PostgreSQL), arquivos, cloud
- **Cenários Pré-construídos**: Fraud detection, loan portfolio, customer 360
- **Integridade Referencial**: Validação de FKs entre todas as entidades
- **Reprodutibilidade**: Seeds para geração determinística
- **Extensível**: Fácil adição de novos domínios, geradores e sinks

## Documentação

| Documento | Descrição |
|-----------|-----------|
| [Catálogo de Dados](docs/data-catalog.md) | Referência completa de todos os datasets, campos e lógica de geração |
| [Ingestão de Dados](docs/data-ingestion.md) | Como carregar dados no PostgreSQL e Kafka |
| [Infraestrutura Docker](docs/docker.md) | Setup do Confluent Platform e configurações |

---

## Quick Start

### Instalação

```bash
# Instalar o pacote
pip install -e .

# Com dependências de desenvolvimento
pip install -e ".[dev]"
```

### Uso Básico

```python
from data_gen.generators.financial import CustomerGenerator, AccountGenerator
from data_gen.store import FinancialDataStore

# Criar store e generators
store = FinancialDataStore()
customer_gen = CustomerGenerator(seed=42)
account_gen = AccountGenerator(seed=42)

# Gerar customers
for customer in customer_gen.generate(100):
    store.add_customer(customer)

# Gerar accounts para cada customer
for customer_id in store.customers:
    for account in account_gen.generate_for_customer(customer_id):
        store.add_account(account)

print(f"Customers: {len(store.customers)}")
print(f"Accounts: {len(store.accounts)}")
```

### Carregar Dados via CLI

```bash
# Iniciar infraestrutura
cd docker/
docker-compose up -d

# Carregar 500 clientes no PostgreSQL e Kafka
python3 scripts/load_data.py --customers 500 --create-topics

# Carregar 2000 clientes
python3 scripts/load_data.py --customers 2000 --seed 2024
```

---

## Cenários Disponíveis

### Fraud Detection

Gera transações com padrões de fraude para treinar modelos de detecção.

```python
from data_gen.scenarios.financial import FraudDetectionScenario
from data_gen.sinks import KafkaSink

scenario = FraudDetectionScenario(
    num_customers=10_000,
    fraud_rate=0.05,
    seed=42,
)
scenario.generate()
scenario.export([KafkaSink("localhost:9092")])
```

### Loan Portfolio

Simula portfólio de empréstimos com comportamento de pagamento realista.

```python
from data_gen.scenarios.financial import LoanPortfolioScenario

scenario = LoanPortfolioScenario(
    num_customers=10_000,
    loan_penetration=0.30,
    default_rate=0.05,
    seed=42,
)
scenario.generate()
```

---

## Estrutura do Projeto

```
data-gen/
├── data_gen/
│   ├── models/              # Modelos de domínio (dataclasses)
│   │   ├── base.py          # Tipos compartilhados (Address, Event)
│   │   ├── financial/       # Domínio financeiro
│   │   ├── retail/          # Domínio varejo (futuro)
│   │   └── ecommerce/       # Domínio e-commerce (futuro)
│   ├── generators/          # Geradores de dados
│   │   ├── financial/       # Geradores financeiros
│   │   ├── retail/          # Geradores varejo (futuro)
│   │   └── ecommerce/       # Geradores e-commerce (futuro)
│   ├── store/               # Data store in-memory com validação de FK
│   ├── sinks/               # Destinos de saída
│   │   ├── kafka.py         # Apache Kafka + Schema Registry
│   │   ├── postgres.py      # PostgreSQL
│   │   ├── json_file.py     # Arquivos JSON
│   │   ├── mongodb.py       # MongoDB (futuro)
│   │   └── filesystem.py    # Parquet/CSV (futuro)
│   └── scenarios/           # Casos de uso end-to-end
├── docs/                    # Documentação em pt-BR
├── docker/                  # Docker Compose (Confluent Platform)
├── scripts/                 # Scripts utilitários
└── tests/                   # Testes unitários e integração
```

---

## Desenvolvimento

```bash
# Instalar dependências de dev
pip install -e ".[dev]"

# Rodar testes
pytest

# Formatar código
black data_gen tests
ruff check data_gen tests

# Type check
mypy data_gen
```

## Docker (Kafka + PostgreSQL)

```bash
# Subir toda a stack Confluent Platform
docker compose -f docker/docker-compose.yml up -d

# Verificar status
docker-compose -f docker/docker-compose.yml ps

# Acessar Control Center
open http://localhost:9021
```

---

## Roadmap

### Domínio: Serviços Financeiros ✅

Status: **Disponível**

- [x] Clientes (Customer) com CPF, renda, score de crédito
- [x] Contas bancárias (Account) - corrente, poupança, investimentos
- [x] Transações (Transaction) - Pix, TED, depósitos, saques, boletos
- [x] Cartões de crédito (CreditCard) - Visa, Mastercard, Elo
- [x] Compras no cartão (CardTransaction) com MCC e parcelas
- [x] Empréstimos (Loan) - pessoal, imobiliário, veicular
- [x] Parcelas (Installment) com SAC e PRICE
- [x] Imóveis (Property) para financiamento habitacional
- [x] Ações B3 (Stock) e operações (Trade)
- [x] Sink PostgreSQL
- [x] Sink Kafka com Avro e Schema Registry
- [x] Cenário: Fraud Detection
- [x] Cenário: Loan Portfolio

### Domínio: Varejo 🔜

Status: **Planejado**

- [ ] Lojas (Store) - físicas e franquias
- [ ] Produtos (Product) - SKU, categoria, preço
- [ ] Estoque (Inventory) - níveis, reposição
- [ ] Vendas (Sale) - PDV, pagamentos
- [ ] Clientes (RetailCustomer) - programa fidelidade
- [ ] Promoções (Promotion) - descontos, cupons
- [ ] Fornecedores (Supplier)
- [ ] Cenário: Análise de vendas
- [ ] Cenário: Gestão de estoque
- [ ] Cenário: Segmentação de clientes

### Domínio: E-commerce 🔜

Status: **Planejado**

- [ ] Usuários (User) - cadastro, autenticação
- [ ] Catálogo (Catalog) - produtos, categorias, variações
- [ ] Carrinho (Cart) - itens, abandono
- [ ] Pedidos (Order) - checkout, status
- [ ] Pagamentos (Payment) - cartão, Pix, boleto
- [ ] Entregas (Shipment) - rastreamento, transportadoras
- [ ] Avaliações (Review) - produtos, vendedores
- [ ] Cenário: Funil de conversão
- [ ] Cenário: Recomendação de produtos
- [ ] Cenário: Previsão de demanda

### Novos Sinks 🔜

- [ ] **MongoDB** - Documentos e dados semi-estruturados
- [ ] **Parquet/CSV** - Exportação para Data Lake
- [ ] **S3/GCS/ADLS** - Cloud object storage
- [ ] **Apache Iceberg** - Table format para Data Lakehouse
- [ ] **Delta Lake** - Databricks e Spark
- [ ] **BigQuery** - Data Warehouse GCP
- [ ] **Snowflake** - Data Warehouse multi-cloud

### Melhorias Gerais 🔜

- [ ] **CDC com Debezium** - Captura de mudanças do PostgreSQL para Kafka
- [ ] **Gerador de PIX em tempo real** - Stream contínuo de transações
- [ ] **API REST** - Endpoint para geração de dados sob demanda
- [ ] **Geração Distribuída** - Suporte a Spark para grandes volumes
- [ ] **Dashboard de Métricas** - Visualização dos dados gerados
- [ ] **Integração com Great Expectations** - Validação de qualidade
- [ ] **Integração com dbt** - Transformações e lineage

### Domínios Futuros 🔮

- [ ] **Telecomunicações** - Planos, consumo, faturas
- [ ] **Saúde** - Pacientes, consultas, exames
- [ ] **Logística** - Entregas, rotas, frotas
- [ ] **Seguros** - Apólices, sinistros, análise de risco
- [ ] **Educação** - Alunos, cursos, avaliações

---

## Contribuindo

Contribuições são bem-vindas! Por favor, leia o guia de contribuição antes de submeter PRs.

1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Add nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

---

## Licença

MIT

---

<p align="center">
  <sub>Feito com ❤️ por <a href="https://github.com/intelium-labs">Intelium Labs</a></sub>
</p>
