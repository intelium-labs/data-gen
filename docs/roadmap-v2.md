# Roadmap v2 — Novas Implementações

Planejamento de expansão do `data-gen` com foco em **poison pills**, **padrões realistas** e **novos produtos financeiros**.

---

## Fase 1 — Poison Pills (Qualidade de Dados)

Dados do mundo real **nunca são limpos**. Essa fase injeta problemas de qualidade de dados que pipelines de dados precisam tratar no dia a dia.

### 1.1 PoisonPillGenerator

Novo gerador configurável que injeta problemas de qualidade nos dados existentes.

| Poison Pill | Descrição | Impacto no Pipeline |
|---|---|---|
| **Eventos duplicados** | Mesmo `transaction_id` enviado 2x ao Kafka | Testa lógica de deduplicação |
| **Dados atrasados (late-arriving)** | Transação com timestamp de 3 dias atrás chegando agora | Testa watermarks e janelas de processamento |
| **Eventos fora de ordem** | Transação B chega antes da A (mas A aconteceu primeiro) | Testa ordenação de eventos |
| **Evolução de schema** | Novo campo `pix_instant_payment_id` adicionado no meio do stream | Testa compatibilidade do Schema Registry |
| **Campos nulos/ausentes** | `counterparty_name` ausente, `email` nulo | Testa tratamento de nulos |
| **Dados malformados** | CPF com letras, CEP inválido, amount como string | Testa validação de dados |
| **Registros órfãos** | Transação referenciando conta inexistente | Testa integridade referencial |
| **Valores extremos** | Transação de R$ 0,01 ou R$ 9.999.999,99 | Testa condições de borda |
| **Timezone mismatch** | Evento com timestamp UTC vs BRT | Testa normalização de fuso horário |
| **Correções retroativas** | Valor atualizado em transação já processada (estorno) | Testa idempotência e correções |
| **Tombstone events** | Evento de fechamento de conta (payload nulo no Kafka) | Testa tratamento de compacted topics |
| **Encoding issues** | Caracteres especiais: "José Conceição Müller" | Testa tratamento UTF-8 |

### 1.2 Configuração

```python
@dataclass
class PoisonPillConfig:
    enabled: bool = False
    duplicate_rate: float = 0.02       # 2% de eventos duplicados
    late_arriving_rate: float = 0.05   # 5% de dados atrasados
    out_of_order_rate: float = 0.03    # 3% fora de ordem
    null_fields_rate: float = 0.01     # 1% com campos nulos
    malformed_rate: float = 0.005      # 0.5% malformados
    orphan_rate: float = 0.002         # 0.2% órfãos
    extreme_values_rate: float = 0.01  # 1% valores extremos
    timezone_mismatch_rate: float = 0.01  # 1% timezone errado
    tombstone_rate: float = 0.005      # 0.5% tombstones
```

### 1.3 Novo Cenário: DataQualityScenario

Cenário que gera dados propositalmente "sujos" para testar robustez de pipelines:

- Combina todos os poison pills configuráveis
- Gera relatório de quais poison pills foram injetados e onde
- Labels para validação: `get_quality_issues()` retorna mapa de `event_id → issue_type`
- Útil para testes de frameworks como Great Expectations, Soda, dbt tests

### 1.4 Arquivos a Criar/Modificar

| Arquivo | Ação | Descrição |
|---|---|---|
| `data_gen/generators/poison_pills.py` | **Criar** | PoisonPillGenerator com todos os tipos |
| `data_gen/models/poison_pills.py` | **Criar** | PoisonPillConfig, PoisonPillType enum, PoisonPillRecord |
| `data_gen/scenarios/financial/data_quality.py` | **Criar** | DataQualityScenario |
| `data_gen/config.py` | **Modificar** | Adicionar PoisonPillConfig ao DataGenConfig |
| `data_gen/sinks/kafka_sink.py` | **Modificar** | Suporte a duplicatas, late-arriving, tombstones |
| `data_gen/sinks/postgres_sink.py` | **Modificar** | Suporte a órfãos, malformados |
| `tests/test_poison_pills.py` | **Criar** | Testes do PoisonPillGenerator |
| `tests/test_data_quality_scenario.py` | **Criar** | Testes do DataQualityScenario |

---

## Fase 2 — Padrões Realistas de Dados

Padrões comportamentais que existem em todo banco brasileiro.

### 2.1 Pagamentos Recorrentes

| Campo | Tipo | Descrição |
|---|---|---|
| `recurring_id` | UUID | Identificador do pagamento recorrente |
| `account_id` | UUID (FK) | Conta do pagador |
| `payment_type` | enum | SUBSCRIPTION, UTILITY_BILL, RENT, INSURANCE_PREMIUM |
| `payee_name` | string | Nome do recebedor (Netflix, CPFL, Imobiliária X) |
| `amount` | decimal | Valor fixo ou variável |
| `frequency` | enum | MONTHLY, WEEKLY, BIWEEKLY |
| `day_of_month` | int | Dia de vencimento (1-28) |
| `status` | enum | ACTIVE, PAUSED, CANCELLED |

**Lógica de geração:**
- 70% dos clientes têm pelo menos 1 pagamento recorrente
- Tipos comuns: streaming (Netflix, Spotify), utilities (CPFL, Sabesp), aluguel
- Valores realistas por tipo (streaming: R$ 20-60, aluguel: R$ 800-5.000)
- Gera transações mensais automáticas no histórico

### 2.2 Depósito de Salário

| Campo | Tipo | Descrição |
|---|---|---|
| `payroll_id` | UUID | Identificador |
| `account_id` | UUID (FK) | Conta salário |
| `employer_name` | string | Nome da empresa |
| `employer_cnpj` | string | CNPJ do empregador |
| `gross_salary` | decimal | Salário bruto |
| `net_salary` | decimal | Salário líquido (desconta INSS, IR) |
| `payment_day` | int | Dia do pagamento (5, 10, 15, 20, 25, 30) |

**Lógica de geração:**
- Apenas clientes EMPLOYED e SELF_EMPLOYED
- Desconta INSS (7.5-14%) e IRRF (0-27.5%) do bruto
- Depósito como transação CREDIT no histórico
- 13o salário em novembro/dezembro
- Férias com adicional de 1/3

### 2.3 Estornos e Reversões

| Campo | Tipo | Descrição |
|---|---|---|
| `reversal_id` | UUID | Identificador do estorno |
| `original_transaction_id` | UUID (FK) | Transação original |
| `reason` | enum | FRAUD, CUSTOMER_REQUEST, DUPLICATE, ERROR, CHARGEBACK |
| `reversal_amount` | decimal | Valor estornado (total ou parcial) |
| `status` | enum | REQUESTED, APPROVED, DENIED, COMPLETED |
| `requested_at` | timestamp | Data da solicitação |
| `completed_at` | timestamp | Data da conclusão |

**Lógica de geração:**
- 1-3% das transações geram estorno
- Chargebacks: 0.5% das transações de cartão
- Pix devolução (MED): 0.2% dos Pix
- Gera evento CREDIT reverso no Kafka

### 2.4 Ciclo de Vida da Conta

Transição de estados para modelar churn e dormência:

```
OPENING → ACTIVE → DORMANT → CLOSED
                 ↓
              BLOCKED → ACTIVE (reativação)
                     → CLOSED
```

| Evento | Descrição | Taxa |
|---|---|---|
| `account.opened` | Abertura de conta | 100% dos clientes |
| `account.activated` | Primeira transação | 95% em até 7 dias |
| `account.dormant` | Sem transação por 90 dias | 5% das contas |
| `account.blocked` | Bloqueio por fraude ou judicial | 1% das contas |
| `account.closed` | Encerramento voluntário | 3% ao ano |

### 2.5 Transferências P2P (entre clientes do datagen)

- 20% das transações Pix são entre clientes do próprio dataset
- Gera par DEBIT (remetente) + CREDIT (destinatário)
- Mesma chave Pix referenciando outro customer
- Útil para análise de grafos e detecção de lavagem

### 2.6 Padrões Sazonais

| Período | Comportamento |
|---|---|
| **Janeiro** | Baixo consumo (pós-festas), IPTU, IPVA |
| **Fevereiro/Março** | Carnaval, material escolar |
| **Maio** | Dia das Mães (pico em varejo) |
| **Junho** | São João (regional NE) |
| **Agosto** | Dia dos Pais |
| **Outubro** | Dia das Crianças |
| **Novembro** | Black Friday (pico em e-commerce e cartão) |
| **Dezembro** | Natal, 13o salário, pico de consumo |

**Implementação:**
- Multiplicador sazonal por mês aplicado ao volume e valor de transações
- Ex: dezembro = 1.8x volume normal, janeiro = 0.6x

### 2.7 Arquivos a Criar/Modificar

| Arquivo | Ação | Descrição |
|---|---|---|
| `data_gen/models/financial/recurring.py` | **Criar** | RecurringPayment, Payroll |
| `data_gen/models/financial/reversal.py` | **Criar** | Reversal, AccountLifecycleEvent |
| `data_gen/generators/financial/recurring.py` | **Criar** | RecurringPaymentGenerator, PayrollGenerator |
| `data_gen/generators/financial/reversal.py` | **Criar** | ReversalGenerator |
| `data_gen/generators/financial/lifecycle.py` | **Criar** | AccountLifecycleGenerator |
| `data_gen/generators/financial/transaction.py` | **Modificar** | Adicionar P2P e sazonalidade |
| `data_gen/store/financial.py` | **Modificar** | Registrar novos tipos de entidade |
| `data_gen/sinks/kafka_sink.py` | **Modificar** | Novos tópicos e schemas |
| `data_gen/sinks/postgres_sink.py` | **Modificar** | Novas tabelas |
| `tests/test_recurring.py` | **Criar** | Testes de recorrência e folha |
| `tests/test_reversals.py` | **Criar** | Testes de estornos |
| `tests/test_lifecycle.py` | **Criar** | Testes de ciclo de vida |

---

## Fase 3 — Novos Produtos Financeiros

### 3.1 Seguros (Insurance)

| Campo | Tipo | Descrição |
|---|---|---|
| `policy_id` | UUID | Identificador da apólice |
| `customer_id` | UUID (FK) | Cliente segurado |
| `insurance_type` | enum | AUTO, LIFE, HOME, TRAVEL |
| `insurer_name` | string | Seguradora (Porto Seguro, Bradesco Seguros, etc.) |
| `premium_monthly` | decimal | Prêmio mensal |
| `coverage_amount` | decimal | Valor de cobertura |
| `deductible` | decimal | Franquia |
| `start_date` | date | Início da vigência |
| `end_date` | date | Fim da vigência |
| `status` | enum | ACTIVE, EXPIRED, CANCELLED, CLAIM_IN_PROGRESS |

**Claims (Sinistros):**

| Campo | Tipo | Descrição |
|---|---|---|
| `claim_id` | UUID | Identificador do sinistro |
| `policy_id` | UUID (FK) | Apólice relacionada |
| `claim_type` | string | Descrição do sinistro |
| `claimed_amount` | decimal | Valor solicitado |
| `approved_amount` | decimal | Valor aprovado |
| `status` | enum | OPEN, UNDER_REVIEW, APPROVED, DENIED, PAID |

### 3.2 Fundos de Investimento

| Campo | Tipo | Descrição |
|---|---|---|
| `fund_id` | UUID | Identificador do fundo |
| `fund_name` | string | Nome do fundo |
| `fund_type` | enum | RENDA_FIXA, RENDA_VARIAVEL, MULTIMERCADO, CAMBIAL, IMOBILIARIO |
| `cnpj` | string | CNPJ do fundo |
| `management_fee` | decimal | Taxa de administração (% ao ano) |
| `performance_fee` | decimal | Taxa de performance (% sobre benchmark) |
| `benchmark` | enum | CDI, IBOVESPA, IPCA, SELIC |
| `nav_per_quota` | decimal | Valor da cota |
| `minimum_investment` | decimal | Aplicação mínima |

**Posições do Investidor:**

| Campo | Tipo | Descrição |
|---|---|---|
| `position_id` | UUID | Identificador |
| `customer_id` | UUID (FK) | Investidor |
| `fund_id` | UUID (FK) | Fundo |
| `quotas` | decimal | Quantidade de cotas |
| `invested_amount` | decimal | Valor investido |
| `current_value` | decimal | Valor atual |
| `yield_amount` | decimal | Rendimento |

### 3.3 Consórcio (Produto Brasileiro)

| Campo | Tipo | Descrição |
|---|---|---|
| `consortium_id` | UUID | Identificador |
| `customer_id` | UUID (FK) | Consorciado |
| `consortium_type` | enum | AUTO, IMOBILIARIO, SERVICOS |
| `group_number` | string | Número do grupo |
| `quota_number` | string | Número da cota |
| `total_value` | decimal | Valor do bem (crédito) |
| `monthly_payment` | decimal | Parcela mensal |
| `term_months` | int | Prazo total |
| `admin_fee_rate` | decimal | Taxa de administração |
| `status` | enum | ACTIVE, CONTEMPLATED, NOT_CONTEMPLATED, CANCELLED |
| `contemplation_date` | date | Data da contemplação (se contemplado) |
| `contemplation_type` | enum | DRAW, BID | Sorteio ou lance |

### 3.4 Arquivos a Criar/Modificar

| Arquivo | Ação | Descrição |
|---|---|---|
| `data_gen/models/financial/insurance.py` | **Criar** | Policy, Claim |
| `data_gen/models/financial/fund.py` | **Criar** | InvestmentFund, FundPosition |
| `data_gen/models/financial/consortium.py` | **Criar** | Consortium |
| `data_gen/generators/financial/insurance.py` | **Criar** | InsuranceGenerator |
| `data_gen/generators/financial/fund.py` | **Criar** | FundGenerator |
| `data_gen/generators/financial/consortium.py` | **Criar** | ConsortiumGenerator |
| `data_gen/scenarios/financial/insurance_portfolio.py` | **Criar** | InsurancePortfolioScenario |
| `data_gen/store/financial.py` | **Modificar** | Novos tipos de entidade |
| `tests/test_insurance.py` | **Criar** | Testes de seguros |
| `tests/test_funds.py` | **Criar** | Testes de fundos |
| `tests/test_consortium.py` | **Criar** | Testes de consórcio |

---

## Fase 4 — Interface TUI (Terminal User Interface)

Interface interativa no terminal usando [Textual](https://github.com/Textualize/textual) para facilitar a configuração e execução do `data-gen` sem necessidade de memorizar flags CLI.

### 4.1 Motivação

- CLI atual requer conhecimento de múltiplas flags (`--customers`, `--fast`, `--no-avro`, `--seed`, etc.)
- Usuários novos precisam ler documentação para entender as opções disponíveis
- TUI oferece experiência guiada com menus, formulários e feedback visual em tempo real
- Mantém compatibilidade total com CLI — TUI é uma camada opcional

### 4.2 Funcionalidades Planejadas

| Funcionalidade | Descrição |
|---|---|
| **Wizard de configuração** | Formulário guiado para definir número de clientes, sinks, cenários |
| **Seleção de cenário** | Menu interativo: Fraud Detection, Loan Portfolio, Customer 360, Data Quality |
| **Seleção de sinks** | Checkboxes para escolher destinos (PostgreSQL, Kafka, JSON, Console) |
| **Progresso em tempo real** | Barra de progresso com métricas: registros/seg, memória, tempo estimado |
| **Log viewer** | Painel lateral com logs em tempo real durante a geração |
| **Pré-visualização** | Amostra dos dados gerados antes de confirmar a carga |
| **Status da infraestrutura** | Verificação de saúde dos serviços Docker (Kafka, PG, SR) |
| **Configuração de poison pills** | Sliders para taxas de duplicatas, atrasados, malformados, etc. |

### 4.3 Arquitetura

```
┌─────────────────────────────────────────────────┐
│                   TUI (Textual)                    │
│                                                    │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐  │
│  │  Wizard   │  │ Progresso │  │  Log Viewer  │  │
│  │  Config   │  │  Barras   │  │  em tempo    │  │
│  │           │  │  Métricas │  │  real         │  │
│  └─────┬─────┘  └─────┬─────┘  └──────────────┘  │
│        │              │                            │
└────────┼──────────────┼────────────────────────────┘
         │              │
         ▼              ▼
┌─────────────────────────────────────────────────┐
│              Core data-gen (sem mudanças)          │
│  Generators → Store → Sinks                        │
└─────────────────────────────────────────────────┘
```

### 4.4 Instalação

A TUI será uma dependência opcional, instalada via extra:

```bash
# Instalar com suporte a TUI
pip install -e ".[ui]"

# Executar a interface
data-gen-tui
# ou
python -m data_gen.tui
```

### 4.5 Tecnologia

| Componente | Tecnologia | Justificativa |
|---|---|---|
| **Framework** | [Textual](https://github.com/Textualize/textual) | TUI moderna, Python puro, rica em widgets |
| **Renderização** | [Rich](https://github.com/Textualize/rich) | Tabelas, barras de progresso, syntax highlight |
| **Empacotamento** | Extra `[ui]` no `pyproject.toml` | Não adiciona dependência para quem usa só CLI |

### 4.6 Arquivos a Criar

| Arquivo | Descrição |
|---|---|
| `data_gen/tui/__init__.py` | Módulo TUI |
| `data_gen/tui/app.py` | Aplicação principal Textual |
| `data_gen/tui/screens/wizard.py` | Tela de configuração guiada |
| `data_gen/tui/screens/progress.py` | Tela de progresso durante geração |
| `data_gen/tui/screens/preview.py` | Tela de pré-visualização de dados |
| `data_gen/tui/widgets/health.py` | Widget de status da infraestrutura |
| `data_gen/tui/widgets/log_panel.py` | Widget de logs em tempo real |
| `tests/test_tui.py` | Testes da interface TUI |

---

## Resumo de Implementação

| Fase | Itens | Prioridade | Valor |
|---|---|---|---|
| **Fase 1** | Poison Pills | Alta | Essencial para testar pipelines de dados |
| **Fase 2** | Padrões Realistas | Alta | Completa a simulação bancária |
| **Fase 3** | Novos Produtos | Média | Expande o domínio financeiro |
| **Fase 4** | Interface TUI | Média | Facilita uso e reduz curva de aprendizado |

### Métricas de Sucesso

- [ ] Todos os poison pills são injetáveis e rastreáveis
- [ ] Cenário DataQuality gera relatório de issues injetados
- [ ] Padrões sazonais visíveis em análise temporal
- [ ] P2P gera grafo de transações entre clientes
- [ ] TUI permite gerar dados sem memorizar flags CLI
- [ ] TUI exibe progresso em tempo real com métricas de performance
- [ ] Testes com 80%+ de cobertura para novos módulos
- [ ] Documentação atualizada no catálogo de dados

---

*Última atualização: 2026-02-09*
