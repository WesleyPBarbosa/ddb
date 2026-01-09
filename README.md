# Distributed Database Middleware (DDB) com MySQL

Este projeto implementa um **middleware de Banco de Dados Distribuído (DDB)** baseado no **SGBD MySQL**, desenvolvido para fins acadêmicos na disciplina de **Banco de Dados Distribuídos**.

O sistema permite que múltiplos nós, cada um com seu próprio MySQL, atuem de forma coordenada, garantindo **replicação total**, **consistência**, **tolerância a falhas** e **transparência ao cliente**, utilizando **sockets TCP**, **protocolo próprio**, **Two-Phase Commit (2PC)** e **eleição dinâmica de coordenador**.

---

## Objetivo do Projeto

Desenvolver um middleware capaz de:
- Integrar múltiplas instâncias MySQL independentes;
- Fornecer ao usuário uma interface única para execução de queries;
- Garantir propriedades **ACID** em ambiente distribuído;
- Replicar automaticamente todas as alterações entre os nós;
- Manter alta disponibilidade mesmo em caso de falha do coordenador.

---

## Conceitos Aplicados

- Banco de Dados Distribuído (DDB)
- DDB homogêneo e autônomo
- Comunicação via sockets TCP
- Protocolo de comunicação customizado
- Checksum (CRC32) para integridade
- Two-Phase Commit (2PC)
- Heartbeat e detecção de falhas
- Eleição de coordenador (Bully Algorithm)
- Balanceamento de carga
- Tolerância a falhas

---

## Arquitetura

Cada nó do sistema executa:
- Uma instância **MySQL local**
- Um processo **middleware** (Node)

Características da arquitetura:
- Todos os nós são **iguais** (homogêneos)
- Cada nó é **autônomo**
- Um nó assume dinamicamente o papel de **coordenador**
- O coordenador pode mudar automaticamente em caso de falha

A comunicação entre os nós ocorre exclusivamente via **sockets TCP**, dentro de uma rede Docker.

---

## Execução das Queries

### SELECT (Leitura)

- Executado em **apenas um nó**
- O coordenador escolhe o nó menos carregado
- Implementa **balanceamento de carga**
- O cliente recebe:
  - o resultado da query
  - a identificação do nó que executou a operação

### INSERT / UPDATE / DELETE / DDL (Escrita)

As operações de escrita utilizam o **Two-Phase Commit (2PC)**:

1. **PREPARE**: todos os nós executam a query dentro de uma transação local  
2. **COMMIT**: se todos responderem positivamente, a transação é confirmada  
3. **ROLLBACK**: se algum nó falhar, todas as transações são revertidas  

Esse mecanismo garante **atomicidade** e **consistência distribuída**, enquanto as demais propriedades ACID são asseguradas pelo MySQL (InnoDB).

---

## Heartbeat e Tolerância a Falhas

- Todos os nós enviam **heartbeats periódicos** informando que estão ativos
- O coordenador monitora a disponibilidade dos nós
- Em caso de falha do coordenador:
  - os nós detectam o timeout
  - uma nova eleição é iniciada
  - um novo coordenador é automaticamente eleito

O sistema continua operando mesmo durante falhas.

---

## Estrutura do Projeto


```
ddb/
├── node.py
├── protocol.py
├── client_cli.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── config/
│   ├── node1.json
│   ├── node2.json
│   └── node3.json
└── mysql-init/
└── 01-init.sql

```
---

## Execução com Docker

### Pré-requisitos

- Docker
- Docker Compose
- Python 3 (opcional, apenas para o cliente)

### Subir o cluster distribuído

```bash
docker compose up --build -d
````

O cluster será iniciado com:

* 3 containers MySQL
* 3 containers do middleware

Um dos nós será automaticamente eleito como **coordenador**.

---

## Testando o Sistema

### Executar o cliente

```bash
python client_cli.py 127.0.0.1 9001
```

(O cliente pode se conectar a qualquer nó.)

### Exemplos de testes

```sql
CREATE TABLE pessoas (
  id INT PRIMARY KEY,
  nome VARCHAR(50)
);

INSERT INTO pessoas VALUES (1, 'Ana');
INSERT INTO pessoas VALUES (2, 'Bruno');

SELECT * FROM pessoas;
```

Exemplo de saída:

```
executed_on_node: 2
```

---

## Teste de Falha do Coordenador

```bash
docker stop node3
```

Outro nó será eleito automaticamente como coordenador e o sistema continuará aceitando queries normalmente.

---

## Parar o Sistema

Parar os containers mantendo os dados:

```bash
docker compose down
```

Parar e apagar também os dados:

```bash
docker compose down -v
```

---

## Requisitos Atendidos

* Uso do SGBD MySQL
* Execução em múltiplos nós
* Comunicação via sockets
* Protocolo próprio de comunicação
* Configuração por IP/host
* DDB homogêneo e autônomo
* Replicação total dos dados
* Coordenador com eleição dinâmica
* Garantia das propriedades ACID
* Heartbeat entre os nós
* Checksum para integridade
* Balanceamento de carga
* Log das queries e dados transmitidos
* Interface simples para execução de queries

---

## Observações Finais

Este projeto possui caráter **acadêmico e didático**, simulando o funcionamento interno de um banco de dados distribuído real.
Ele demonstra conceitos fundamentais da disciplina de **Banco de Dados Distribuídos**, como coordenação, replicação e tolerância a falhas.

```
