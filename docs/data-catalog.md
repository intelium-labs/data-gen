# Catálogo de Dados

Este documento descreve todos os datasets gerados pela plataforma `data-gen`, incluindo definições de campos, tipos de dados, relacionamentos e lógica de geração.

> **Docs relacionados**: [Ingestão de Dados](./data-ingestion.md) | [Infraestrutura Docker](./docker.md) | [Índice da Documentação](./README.md)

## Visão Geral

O gerador de dados simula um **banco brasileiro** com os seguintes tipos de entidade:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DIAGRAMA DE RELACIONAMENTO DE ENTIDADES                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                              ┌──────────────┐                               │
│                              │   Customer   │                               │
│                              │   (master)   │                               │
│                              └──────┬───────┘                               │
│                     ┌───────────────┼───────────────┬──────────────┐        │
│                     │               │               │              │        │
│                     ▼               ▼               ▼              ▼        │
│              ┌──────────┐    ┌──────────┐    ┌──────────┐   ┌──────────┐   │
│              │ Account  │    │  Credit  │    │   Loan   │   │ Property │   │
│              │          │    │   Card   │    │          │───│ (para    │   │
│              └────┬─────┘    └────┬─────┘    └────┬─────┘   │ imóveis) │   │
│                   │               │               │         └──────────┘   │
│         ┌─────────┴─────────┐     │               │                        │
│         ▼                   ▼     ▼               ▼                        │
│  ┌─────────────┐     ┌──────────────┐     ┌─────────────┐                 │
│  │ Transaction │     │     Card     │     │ Installment │                 │
│  │   (evento)  │     │ Transaction  │     │   (evento)  │                 │
│  └─────────────┘     │   (evento)   │     └─────────────┘                 │
│         │            └──────────────┘                                      │
│         │                                                                   │
│         │            ┌──────────────┐     ┌─────────────┐                 │
│         │            │    Stock     │────►│    Trade    │                 │
│         │            │ (referência) │     │   (evento)  │                 │
│         │            └──────────────┘     └──────┬──────┘                 │
│         │                                        │                         │
│         └────────────────────────────────────────┘                         │
│                              │                                              │
│                    (todos eventos vinculados a accounts)                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Classificação dos Dados

| Dataset | Tipo | Destino | Frequência de Atualização |
|---------|------|---------|---------------------------|
| Customer | Master | PostgreSQL | Raramente muda |
| Account | Master | PostgreSQL | Raramente muda |
| CreditCard | Master | PostgreSQL | Raramente muda |
| Loan | Master | PostgreSQL | Atualizações de status |
| Property | Referência | PostgreSQL | Imutável |
| Stock | Referência | PostgreSQL | Atualizações de preço |
| Transaction | Evento | Kafka | Stream em tempo real |
| CardTransaction | Evento | Kafka | Stream em tempo real |
| Trade | Evento | Kafka | Stream em tempo real |
| Installment | Evento | Kafka | Agendado (mensal) |

---

## Dados Mestres (Master Data)

### Customer

Informações do cliente do banco.

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `customer_id` | UUID | Identificador único | `bdd640fb-0667-4ad1-9c80-317fa3b1799d` | UUID v4 |
| `cpf` | string(14) | CPF (documento fiscal) | `123.456.789-00` | CPF válido com dígitos verificadores |
| `name` | string(255) | Nome completo | `João Silva` | Nomes brasileiros do Faker |
| `email` | string(255) | Endereço de e-mail | `joao.silva@email.com` | Derivado do nome |
| `phone` | string(20) | Telefone | `+55 11 98765-4321` | Formato brasileiro |
| `address` | Address | Endereço completo | Ver tipo Address | Endereços brasileiros |
| `monthly_income` | decimal(15,2) | Renda mensal (BRL) | `8500.00` | Dist. normal (μ=5000, σ=3000) |
| `employment_status` | enum | Situação de emprego | `EMPLOYED` | Aleatório ponderado |
| `credit_score` | int | Score de crédito (300-850) | `720` | Baseado na renda + aleatório |
| `created_at` | timestamp | Data de cadastro | `2023-06-15T14:30:00` | Últimos 3 anos |

**Distribuição de Situação de Emprego:**
| Status | Peso | Descrição |
|--------|------|-----------|
| `EMPLOYED` | 60% | Empregado CLT |
| `SELF_EMPLOYED` | 20% | Autônomo/freelancer |
| `RETIRED` | 15% | Aposentado |
| `UNEMPLOYED` | 5% | Desempregado |

**Cálculo do Score de Crédito:**
```python
base_score = 300
income_factor = min(monthly_income / 50000 * 400, 400)  # 0-400 pontos
random_factor = random(-50, +50)
credit_score = base_score + income_factor + random_factor
# Limitado a 300-850
```

---

### Account

Conta bancária vinculada a um cliente.

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `account_id` | UUID | Identificador único | `bc36dc59-e134-4dfc-9777-7f10f2d2393f` | UUID v4 |
| `customer_id` | UUID | Referência ao titular | `bdd640fb-0667-...` | FK para Customer |
| `account_type` | enum | Tipo de conta | `CONTA_CORRENTE` | Aleatório ponderado |
| `bank_code` | string(10) | Código do banco (COMPE) | `341` | Da lista de bancos |
| `branch` | string(10) | Número da agência | `0001` | 4 dígitos |
| `account_number` | string(20) | Número da conta | `123456-7` | 6 dígitos + verificador |
| `balance` | decimal(15,2) | Saldo atual | `15420.50` | Baseado na renda |
| `status` | enum | Status da conta | `ACTIVE` | Sempre ACTIVE inicialmente |
| `created_at` | timestamp | Data de abertura | `2023-07-20T10:00:00` | Após created_at do customer |

**Distribuição de Tipo de Conta:**
| Tipo | Peso | Descrição |
|------|------|-----------|
| `CONTA_CORRENTE` | 70% | Conta corrente |
| `POUPANCA` | 20% | Conta poupança |
| `INVESTIMENTOS` | 10% | Conta investimentos |

**Códigos de Bancos Brasileiros:**
| Código | Banco |
|--------|-------|
| `001` | Banco do Brasil |
| `033` | Santander |
| `104` | Caixa Econômica |
| `237` | Bradesco |
| `341` | Itaú |
| `756` | Sicoob |

**Contas por Cliente:** 1-3 contas (ponderado: 1=50%, 2=35%, 3=15%)

---

### CreditCard

Cartão de crédito emitido para um cliente.

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `card_id` | UUID | Identificador único | `a1b2c3d4-...` | UUID v4 |
| `customer_id` | UUID | Referência ao titular | `bdd640fb-...` | FK para Customer |
| `card_number_masked` | string(20) | Número mascarado | `****-****-****-4532` | Últimos 4 dígitos aleatórios |
| `brand` | enum | Bandeira do cartão | `VISA` | Aleatório ponderado |
| `credit_limit` | decimal(15,2) | Limite de crédito | `12000.00` | 2-5x a renda mensal |
| `available_limit` | decimal(15,2) | Limite disponível | `8500.00` | 50-100% do limite |
| `due_day` | int | Dia de vencimento | `15` | Aleatório 1-28 |
| `status` | enum | Status do cartão | `ACTIVE` | Sempre ACTIVE |
| `created_at` | timestamp | Data de emissão | `2023-08-01T...` | Após customer |

**Distribuição de Bandeira:**
| Bandeira | Peso | Descrição |
|----------|------|-----------|
| `VISA` | 45% | Visa Internacional |
| `MASTERCARD` | 40% | Mastercard |
| `ELO` | 15% | Bandeira brasileira |

**Cartões por Cliente:** 0-2 cartões (70% dos clientes têm pelo menos 1)

---

### Loan

Contrato de empréstimo com um cliente.

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `loan_id` | UUID | Identificador único | `1a2bd69c-...` | UUID v4 |
| `customer_id` | UUID | Referência ao tomador | `bdd640fb-...` | FK para Customer |
| `loan_type` | enum | Tipo de empréstimo | `PERSONAL` | Baseado no cenário |
| `principal` | decimal(15,2) | Valor emprestado | `50000.00` | Baseado no tipo |
| `interest_rate` | decimal(10,6) | Taxa mensal | `0.0199` | Baseado no score + tipo |
| `term_months` | int | Prazo em meses | `36` | Baseado no tipo |
| `amortization_system` | enum | Sistema de amortização | `PRICE` | Baseado no tipo |
| `status` | enum | Status do empréstimo | `ACTIVE` | Estado do ciclo de vida |
| `disbursement_date` | date | Data de desembolso | `2024-01-15` | Após aprovação |
| `property_id` | UUID | Garantia (imobiliário) | `prop-123-...` | FK para Property |
| `created_at` | timestamp | Data da solicitação | `2024-01-10T...` | Antes do desembolso |

**Tipos de Empréstimo:**
| Tipo | Faixa de Principal | Prazo | Taxa de Juros | Amortização |
|------|-------------------|-------|---------------|-------------|
| `PERSONAL` | R$1K - R$100K | 6-72 meses | 1,5% - 8% ao mês | PRICE |
| `HOUSING` | R$100K - R$2M | 120-360 meses | 0,7% - 1,2% ao mês | SAC ou PRICE |
| `VEHICLE` | R$20K - R$300K | 12-60 meses | 1,0% - 3,5% ao mês | PRICE |

**Cálculo da Taxa de Juros:**
```python
base_rate = LOAN_BASE_RATES[loan_type]  # ex: 0.02 para pessoal
score_factor = (850 - credit_score) / 550 * 0.03  # ajuste de 0-3%
interest_rate = base_rate + score_factor
```

**Fluxo de Status do Empréstimo:**
```
PENDING → APPROVED → ACTIVE → PAID_OFF
              ↓
          REJECTED

ACTIVE → DEFAULT (se 90+ dias em atraso)
```

---

### Property

Imóvel usado como garantia em financiamentos habitacionais.

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `property_id` | UUID | Identificador único | `prop-abc123-...` | UUID v4 |
| `property_type` | enum | Tipo de imóvel | `APARTMENT` | Aleatório ponderado |
| `address` | Address | Localização | Ver tipo Address | Endereços brasileiros |
| `appraised_value` | decimal(15,2) | Valor de avaliação | `450000.00` | R$150K - R$2M |
| `area_sqm` | float | Área em m² | `85.5` | Baseado no tipo |
| `registration_number` | string(50) | Matrícula | `MAT-123456-SP` | Formato: MAT-NNNNNN-UF |

**Distribuição de Tipo de Imóvel:**
| Tipo | Peso | Faixa de Área | Faixa de Valor |
|------|------|---------------|----------------|
| `APARTMENT` | 60% | 40-200 m² | R$150K - R$1,5M |
| `HOUSE` | 35% | 80-400 m² | R$200K - R$2M |
| `LAND` | 5% | 200-1000 m² | R$50K - R$500K |

---

### Stock

Ações listadas na B3 (Bolsa de Valores Brasileira).

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `stock_id` | UUID | Identificador único | `stock-petr4-...` | UUID v4 |
| `ticker` | string(10) | Código na B3 | `PETR4` | Tickers reais da B3 |
| `company_name` | string(255) | Nome da empresa | `Petrobras` | Empresas reais |
| `sector` | enum | Setor econômico | `ENERGY` | Baseado na empresa |
| `segment` | enum | Segmento de listagem | `NOVO_MERCADO` | Baseado na empresa |
| `current_price` | decimal(15,2) | Preço atual | `32.50` | Faixa realista |
| `currency` | string(3) | Moeda | `BRL` | Sempre BRL |
| `isin` | string(20) | Código ISIN | `BRPETRACNPR6` | Formato: BR + ... |
| `lot_size` | int | Lote padrão | `100` | Geralmente 100 |
| `created_at` | timestamp | Data de criação | `2024-01-01T...` | Fixo |

**Setores:**
- `ENERGY` - Petróleo, gás, utilidades
- `FINANCE` - Bancos, seguradoras
- `MINING` - Mineração
- `RETAIL` - Varejo
- `TELECOM` - Telecomunicações
- `HEALTH` - Saúde
- `INDUSTRIAL` - Indústria

**Segmentos da B3:**
| Segmento | Descrição |
|----------|-----------|
| `NOVO_MERCADO` | Mais alto padrão de governança |
| `N1` | Nível 1 de governança |
| `N2` | Nível 2 de governança |
| `TRADICIONAL` | Listagem tradicional |

**Ações Pré-definidas (42 total):**
Inclui principais empresas brasileiras: PETR4, VALE3, ITUB4, BBDC4, ABEV3, B3SA3, etc.

---

## Dados de Eventos

### Transaction

Transações de conta bancária (Pix, TED, depósitos, etc.).

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `transaction_id` | UUID | Identificador único | `tx-abc123-...` | UUID v4 |
| `account_id` | UUID | Referência da conta | `bc36dc59-...` | FK para Account |
| `transaction_type` | enum | Tipo de transação | `PIX` | Aleatório ponderado |
| `amount` | decimal(15,2) | Valor da transação | `150.00` | Dist. lognormal |
| `direction` | enum | Direção do dinheiro | `DEBIT` | Baseado no tipo |
| `counterparty_key` | string | ID da contraparte | `12345678901` | Chave Pix, nº conta |
| `counterparty_name` | string | Nome da contraparte | `Maria Santos` | Nome aleatório |
| `description` | string | Descrição | `Pix para Maria` | Gerado |
| `timestamp` | timestamp | Horário da transação | `2024-03-15T14:23:45` | Dentro do período |
| `status` | enum | Status da transação | `COMPLETED` | 98% completado |
| `pix_e2e_id` | string | ID E2E do Pix | `E12345...` | Apenas para Pix |
| `pix_key_type` | enum | Tipo de chave Pix | `CPF` | Apenas para Pix |

**Distribuição de Tipo de Transação:**
| Tipo | Peso | Direção | Descrição |
|------|------|---------|-----------|
| `PIX` | 45% | Ambos | Pagamento instantâneo |
| `TED` | 15% | Ambos | Transferência bancária |
| `DEPOSIT` | 15% | CREDIT | Depósito em dinheiro/cheque |
| `WITHDRAW` | 10% | DEBIT | Saque no caixa eletrônico |
| `BOLETO` | 10% | DEBIT | Pagamento de boleto |
| `DOC` | 5% | Ambos | Transferência legada |

**Distribuição de Valores:**
```python
# Distribuição lognormal
# Transações pequenas são mais comuns
mean_amount = account_balance * 0.05
amount = lognormal(mean=log(mean_amount), sigma=1.5)
# Limitado ao saldo da conta para débitos
```

**Tipos de Chave Pix:**
| Tipo | Peso | Formato |
|------|------|---------|
| `CPF` | 40% | 11 dígitos |
| `PHONE` | 30% | +55... |
| `EMAIL` | 20% | email@dominio |
| `EVP` | 10% | Chave aleatória |

**Taxa de Geração:** ~20 transações/cliente/ano (0,3/dia × 365 dias ÷ 5 contas)

---

### CardTransaction

Compras no cartão de crédito.

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `transaction_id` | UUID | Identificador único | `ctx-abc123-...` | UUID v4 |
| `card_id` | UUID | Referência do cartão | `a1b2c3d4-...` | FK para CreditCard |
| `merchant_name` | string | Nome do estabelecimento | `Supermercado Extra` | Da categoria MCC |
| `merchant_category` | string | Nome da categoria | `Grocery Stores` | Descrição MCC |
| `mcc_code` | string(4) | Código MCC | `5411` | MCC padrão |
| `amount` | decimal(15,2) | Valor da compra | `234.50` | Baseado no MCC |
| `installments` | int | Parcelas | `3` | 1 ou 2-12 |
| `timestamp` | timestamp | Horário da compra | `2024-03-15T19:45:00` | Horário comercial |
| `status` | enum | Status da transação | `APPROVED` | 95% aprovado |
| `location_city` | string | Cidade | `São Paulo` | Cidade do cliente |
| `location_country` | string(2) | País | `BR` | Geralmente BR |

**Códigos de Categoria de Estabelecimento (MCC):**
| MCC | Categoria | Valor Médio | Peso |
|-----|-----------|-------------|------|
| `5411` | Supermercados | R$150-300 | 25% |
| `5812` | Restaurantes | R$50-200 | 20% |
| `5541` | Postos de Gasolina | R$100-250 | 15% |
| `5311` | Lojas de Departamento | R$100-500 | 10% |
| `5912` | Farmácias | R$30-150 | 10% |
| `5814` | Fast Food | R$20-80 | 10% |
| `4121` | Uber/Transporte | R$15-60 | 5% |
| `5999` | Varejo Diversos | R$50-300 | 5% |

**Distribuição de Parcelas:**
| Parcelas | Peso | Descrição |
|----------|------|-----------|
| 1 | 60% | À vista |
| 2-3 | 20% | Curto prazo |
| 4-6 | 15% | Médio prazo |
| 7-12 | 5% | Longo prazo |

**Taxa de Geração:** ~57 transações/portador/ano (0,5/dia × 365 dias × 0,7 taxa aprovação ÷ 2 cartões)

---

### Trade

Ordens de compra/venda de ações executadas na B3.

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `trade_id` | UUID | Identificador único | `trade-abc123-...` | UUID v4 |
| `account_id` | UUID | Conta de investimentos | `bc36dc59-...` | FK para Account (INVESTIMENTOS) |
| `stock_id` | UUID | Referência da ação | `stock-petr4-...` | FK para Stock |
| `ticker` | string | Ticker da ação | `PETR4` | Desnormalizado |
| `trade_type` | enum | Compra ou venda | `BUY` | Aleatório ponderado |
| `quantity` | int | Ações negociadas | `100` | Múltiplos do lot_size |
| `price_per_share` | decimal(15,2) | Preço de execução | `32.45` | Próximo do current_price |
| `total_amount` | decimal(15,2) | Valor da operação | `3245.00` | quantity × price |
| `fees` | decimal(15,2) | Taxas totais | `3.25` | ~0,1% do total |
| `net_amount` | decimal(15,2) | Valor líquido | `3248.25` | total +/- fees |
| `order_type` | enum | Tipo de ordem | `MARKET` | Aleatório ponderado |
| `status` | enum | Status da operação | `EXECUTED` | 95% executado |
| `executed_at` | timestamp | Horário de execução | `2024-03-15T10:30:00` | Horário de pregão |
| `settlement_date` | timestamp | Data de liquidação | `2024-03-18T...` | D+2 (pula fins de semana) |

**Distribuição de Tipo de Operação:**
| Tipo | Peso | Descrição |
|------|------|-----------|
| `BUY` | 55% | Compra de ações |
| `SELL` | 45% | Venda de ações |

**Distribuição de Tipo de Ordem:**
| Tipo | Peso | Descrição |
|------|------|-----------|
| `MARKET` | 70% | Executar a preço de mercado |
| `LIMIT` | 25% | Executar a preço específico |
| `STOP` | 5% | Ordem stop-loss |

**Cálculo de Taxas:**
```python
brokerage_fee = total_amount * 0.0005     # 0,05%
b3_emoluments = total_amount * 0.000275   # 0,0275%
settlement_fee = total_amount * 0.0000275 # 0,00275%
total_fees = brokerage_fee + b3_emoluments + settlement_fee
```

**Data de Liquidação:** D+2 dias úteis (pulando fins de semana)

**Taxa de Geração:** ~20 operações/conta de investimentos

---

### Installment

Eventos de pagamento de parcelas de empréstimos.

| Campo | Tipo | Descrição | Exemplo | Lógica de Geração |
|-------|------|-----------|---------|-------------------|
| `installment_id` | UUID | Identificador único | `inst-abc123-...` | UUID v4 |
| `loan_id` | UUID | Referência do empréstimo | `1a2bd69c-...` | FK para Loan |
| `installment_number` | int | Número da parcela | `5` | 1 até term_months |
| `due_date` | date | Data de vencimento | `2024-03-15` | Mensal a partir do desembolso |
| `principal_amount` | decimal(15,2) | Parte do principal | `1200.00` | Baseado na amortização |
| `interest_amount` | decimal(15,2) | Parte dos juros | `450.00` | Baseado na taxa |
| `total_amount` | decimal(15,2) | Pagamento total | `1650.00` | principal + juros |
| `paid_date` | date | Data de pagamento | `2024-03-14` | Antes/no/após vencimento |
| `paid_amount` | decimal(15,2) | Valor pago | `1650.00` | Geralmente total_amount |
| `status` | enum | Status do pagamento | `PAID` | Baseado no comportamento |

**Sistemas de Amortização:**

**PRICE (Sistema Francês):**
```python
# Pagamento mensal fixo
pmt = principal * (rate * (1 + rate)^n) / ((1 + rate)^n - 1)
# Juros diminuem, principal aumenta ao longo do tempo
interest = remaining_balance * rate
principal = pmt - interest
```

**SAC (Amortização Constante):**
```python
# Principal fixo, pagamentos decrescentes
principal = original_principal / n  # Constante
interest = remaining_balance * rate  # Decrescente
total = principal + interest  # Decrescente
```

**Distribuição de Status de Pagamento (aplicada por PaymentBehavior):**
| Status | Taxa Padrão | Descrição |
|--------|-------------|-----------|
| `PAID` | 85% | Pago no vencimento ou antes |
| `LATE` | 10% | Pago 1-30 dias após vencimento |
| `DEFAULT` | 5% | 90+ dias em atraso |
| `PENDING` | - | Parcela futura |

**Taxa de Geração:** ~30 parcelas/empréstimo (prazo médio)

---

## Tipos Comuns

### Address

Estrutura de endereço brasileiro.

| Campo | Tipo | Descrição | Exemplo |
|-------|------|-----------|---------|
| `street` | string | Nome da rua | `Rua das Flores` |
| `number` | string | Número | `123` |
| `complement` | string | Complemento | `Apt 42` |
| `neighborhood` | string | Bairro | `Jardim Paulista` |
| `city` | string | Cidade | `São Paulo` |
| `state` | string(2) | UF | `SP` |
| `postal_code` | string | CEP | `01310-100` |
| `country` | string(2) | País | `BR` |

### Envelope de Evento

Wrapper padrão para eventos de streaming (opcional).

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `event_id` | UUID | ID único do evento |
| `event_type` | string | Tipo (ex: `transaction.created`) |
| `event_time` | timestamp | Timestamp do evento |
| `source` | string | Sistema de origem |
| `subject` | string | ID da entidade afetada |
| `data` | object | Payload do evento |
| `metadata` | object | Metadados adicionais |

---

## Qualidade de Dados

### Regras de Validação

| Entidade | Regra | Descrição |
|----------|-------|-----------|
| Customer | CPF válido | CPF passa na validação de dígitos verificadores |
| Customer | Faixa de score | 300 ≤ score ≤ 850 |
| Account | FK existe | customer_id existe em customers |
| Transaction | FK existe | account_id existe em accounts |
| Trade | Conta investimentos | account_type = 'INVESTIMENTOS' |
| Loan | Taxa válida | 0 < interest_rate < 0.15 |
| Installment | Soma confere | Σ(parcelas) ≈ principal + juros |

### Integridade Referencial

Todas as chaves estrangeiras são validadas no momento da geração pelo `FinancialDataStore`:

```python
store.add_account(account)  # Valida se customer_id existe
store.add_trade(trade)      # Valida se account_id E stock_id existem
                            # Também valida se account_type = 'INVESTIMENTOS'
```

---

## Parâmetros de Geração

### Proporções Padrão

| Parâmetro | Padrão | Descrição |
|-----------|--------|-----------|
| Contas por cliente | 1.5 | Média de contas |
| Cartões por cliente | 0.7 | 70% têm cartões |
| Empréstimos por cliente | 0.35 | 35% têm empréstimos |
| Transações por cliente/ano | ~20 | Transações de conta |
| Transações cartão por portador/ano | ~57 | Compras no cartão |
| Operações por conta investimentos | 20 | Trades de ações |

### Reprodutibilidade

Todos os geradores aceitam um parâmetro `seed` para saída reproduzível:

```python
from data_gen.generators.financial import CustomerGenerator

gen = CustomerGenerator(seed=42)
customer1 = gen.generate()  # Sempre mesmo resultado com seed=42
```

---

## Localização dos Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `data_gen/models/base.py` | Address, Event |
| `data_gen/models/financial/customer.py` | Customer |
| `data_gen/models/financial/account.py` | Account |
| `data_gen/models/financial/transaction.py` | Transaction |
| `data_gen/models/financial/credit_card.py` | CreditCard, CardTransaction |
| `data_gen/models/financial/loan.py` | Loan, Installment |
| `data_gen/models/financial/property.py` | Property |
| `data_gen/models/financial/stock.py` | Stock, Trade |
| `data_gen/generators/financial/` | Todos os geradores |
| `data_gen/store/financial.py` | FinancialDataStore (validação FK) |
