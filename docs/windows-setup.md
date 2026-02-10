# Setup no Windows

Guia completo para executar o `data-gen` no Windows usando Docker Desktop com WSL2.

> **Docs relacionados**: [Infraestrutura Docker](./docker.md) | [Ingestão de Dados](./data-ingestion.md) | [Índice da Documentação](./README.md)

---

## Pré-requisitos

| Requisito | Versão Mínima | Descrição |
|-----------|---------------|-----------|
| **Windows** | 10 (build 19041+) ou 11 | Necessário para WSL2 |
| **Docker Desktop** | 4.x | Com backend WSL2 habilitado |
| **WSL2** | — | Subsistema Windows para Linux |
| **Python** | 3.11+ | Interpretador Python |
| **Git** | 2.x | Controle de versão |

---

## 1. Instalar WSL2

O Docker Desktop no Windows requer o WSL2 como backend. Abra o **PowerShell como Administrador** e execute:

```powershell
# Instalar WSL2 com Ubuntu (distribuição padrão)
wsl --install

# Reiniciar o computador após a instalação
# Após reiniciar, o Ubuntu será configurado automaticamente
```

> **Nota**: Se o WSL2 já estiver instalado, verifique a versão com `wsl --version`.

### Verificar Instalação

```powershell
# Verificar que WSL2 está ativo
wsl --list --verbose

# Saída esperada:
#   NAME      STATE    VERSION
# * Ubuntu    Running  2
```

---

## 2. Instalar Docker Desktop

1. Baixe o [Docker Desktop para Windows](https://www.docker.com/products/docker-desktop/)
2. Execute o instalador
3. Durante a instalação, marque **"Use WSL 2 instead of Hyper-V"**
4. Reinicie o computador se solicitado

### Configurar Recursos

O `data-gen` precisa de memória suficiente para rodar o Confluent Platform. Configure no Docker Desktop:

1. Abra **Docker Desktop** → **Settings** → **Resources** → **WSL Integration**
2. Habilite a integração com sua distribuição WSL (Ubuntu)
3. Em **Resources** → **Advanced**, configure:

| Recurso | Mínimo | Recomendado | Stack Completa |
|---------|--------|-------------|----------------|
| **CPU** | 2 cores | 4 cores | 4+ cores |
| **Memória** | 4 GB | 6 GB | 8 GB |
| **Disco** | 20 GB | 40 GB | 60 GB |

> **Importante**: Com menos de 4 GB de memória, o broker Kafka pode não iniciar corretamente.

### Verificar Docker

```powershell
# No PowerShell ou terminal WSL
docker --version
docker compose version

# Testar se o Docker está funcionando
docker run --rm hello-world
```

---

## 3. Clonar o Repositório

Recomendamos clonar o repositório **dentro do filesystem WSL** para melhor performance de I/O:

```bash
# Abrir terminal WSL (Ubuntu)
wsl

# Clonar dentro do home do WSL (NÃO em /mnt/c/)
cd ~
git clone https://github.com/intelium-labs/data-gen.git
cd data-gen
```

> **Performance**: Arquivos em `/mnt/c/` (disco Windows) são ~10x mais lentos que arquivos nativos do WSL (`~/`). Sempre trabalhe dentro do filesystem WSL.

---

## 4. Instalar Python e Dependências

### Opção A: Python no WSL (Recomendado)

```bash
# No terminal WSL
sudo apt update && sudo apt install -y python3.11 python3.11-venv python3-pip

# Criar ambiente virtual
python3.11 -m venv .venv
source .venv/bin/activate

# Instalar o pacote
pip install -e ".[dev]"
```

### Opção B: Python no Windows

```powershell
# No PowerShell
# Instalar Python via winget
winget install Python.Python.3.11

# Criar ambiente virtual
python -m venv .venv
.venv\Scripts\activate

# Instalar o pacote
pip install -e ".[dev]"
```

> **Nota**: Ambas as opções funcionam. A Opção A (WSL) tem melhor performance de I/O quando o repositório está no filesystem WSL.

### Verificar Dependências

```bash
# Todas as dependências devem instalar sem erro
python -c "import faker; import confluent_kafka; import psycopg; print('OK')"
```

As dependências `confluent-kafka` e `psycopg[binary]` possuem wheels pré-compilados para Windows, portanto não é necessário compilar nada manualmente.

---

## 5. Iniciar Infraestrutura Docker

```bash
# Iniciar todos os serviços
docker compose -f docker/docker-compose.yml up -d

# Verificar status (todos devem estar "healthy" ou "running")
docker compose -f docker/docker-compose.yml ps

# Aguardar ~60 segundos para todos os serviços iniciarem
```

### Verificar Serviços

| Serviço | URL | Teste |
|---------|-----|-------|
| Kafka Broker | localhost:9092 | `docker exec datagen-broker kafka-broker-api-versions --bootstrap-server localhost:9092` |
| Schema Registry | http://localhost:8081 | `curl http://localhost:8081/subjects` |
| PostgreSQL | localhost:5432 | `docker exec datagen-postgres psql -U postgres -d datagen -c "SELECT 1"` |
| Control Center | http://localhost:9021 | Abrir no navegador |

> **Dica**: No Windows, use `curl.exe` no PowerShell (o alias `curl` do PowerShell é diferente do curl real).

---

## 6. Carregar Dados

```bash
# Carregar 100 clientes (modo rápido: COPY + BULK + streaming)
python scripts/load_data.py --customers 100 --create-topics --truncate

# Carregar 10K clientes
python scripts/load_data.py --customers 10000 --create-topics --truncate

# Carga paralela para volumes maiores
python scripts/load_data_parallel.py --customers 100000 --create-topics --truncate
```

### Verificar Dados

```bash
# PostgreSQL
docker exec datagen-postgres psql -U postgres -d datagen -c "SELECT count(*) FROM customers;"

# Kafka
docker exec datagen-broker kafka-get-offsets --bootstrap-server localhost:9092 \
  --topic-partitions banking.transactions:0,banking.transactions:1,banking.transactions:2
```

---

## Troubleshooting

### Docker Desktop Não Inicia

```powershell
# Verificar se WSL2 está habilitado
wsl --status

# Reiniciar o serviço Docker
net stop com.docker.service
net start com.docker.service

# Se persistir, reiniciar WSL
wsl --shutdown
```

### Erro "Cannot connect to Docker daemon"

```powershell
# Verificar se Docker Desktop está rodando
docker info

# Se no WSL, verificar integração
# Docker Desktop → Settings → Resources → WSL Integration → Habilitar Ubuntu
```

### Portas em Uso

O Windows pode ter serviços ocupando portas necessárias:

```powershell
# Verificar porta em uso (ex: 5432 para PostgreSQL)
netstat -ano | findstr :5432

# Se Hyper-V reservou portas, liberar:
netsh interface ipv4 show excludedportrange protocol=tcp

# Solução: alterar portas no docker-compose.yml se necessário
```

### Performance Lenta

1. **Filesystem**: Certifique-se que o repositório está no filesystem WSL (`~/`), não em `/mnt/c/`
2. **Memória**: Aumente a memória do Docker Desktop para 6-8 GB
3. **Antivírus**: Adicione exceções para o diretório do Docker e do WSL no Windows Defender
4. **VPN**: Desative VPNs corporativas que podem interferir com a rede Docker

### Erro de Codificação (UTF-8)

O `data-gen` gera dados com caracteres brasileiros (acentos, cedilha). Se encontrar erros de encoding:

```powershell
# No PowerShell, configurar UTF-8
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"
```

```bash
# No WSL, geralmente já funciona. Verificar:
locale
# Deve mostrar: LANG=C.UTF-8 ou pt_BR.UTF-8
```

### Erro "pip install confluent-kafka" Falha

```bash
# Usar a versão binária (wheel pré-compilado)
pip install confluent-kafka[avro]

# Se falhar no Windows nativo, usar WSL como alternativa
```

### WSL Consumindo Muita Memória

O WSL2 pode consumir muita RAM. Crie o arquivo `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=6GB
processors=4
swap=2GB
```

Após criar/editar, reinicie o WSL:

```powershell
wsl --shutdown
```

---

## Diferenças entre Windows, macOS e Linux

| Aspecto | Windows (WSL2) | macOS | Linux |
|---------|----------------|-------|-------|
| **Docker** | Docker Desktop + WSL2 | Docker Desktop | Docker Engine nativo |
| **Performance I/O** | Nativo WSL > /mnt/c/ | Boa | Melhor |
| **Memória Docker** | Configurável via Docker Desktop | Configurável via Docker Desktop | Sem limite (usa RAM do host) |
| **Portas** | Hyper-V pode reservar portas | Sem conflitos comuns | Sem conflitos comuns |
| **Python wheels** | Disponíveis (Windows e manylinux) | Disponíveis | Disponíveis |
| **UTF-8** | Configurar manualmente no PowerShell | Padrão | Padrão |

---

## Referências

- [Instalar WSL2](https://learn.microsoft.com/pt-br/windows/wsl/install)
- [Docker Desktop para Windows](https://docs.docker.com/desktop/install/windows-install/)
- [Melhores práticas WSL + Docker](https://docs.docker.com/desktop/wsl/)
- [Configurar .wslconfig](https://learn.microsoft.com/pt-br/windows/wsl/wsl-config)

---

*Última atualização: 2026-02-09*
