# Restaurante Digital · RabbitMQ

Mini sistema desacoplado de restaurante usando **RabbitMQ** como broker de mensagens.
Demonstra o conceito de **mensageria assíncrona**: cliente, cozinha e financeiro
não conversam diretamente — conversam pelas filas.

```
Cliente → fila_cozinha → Cozinha → fila_pagamento → Financeiro
docker compose up --build
```
### 2. Acessar
- **Dashboard:** http://localhost:3000
- **Painel do RabbitMQ:** http://localhost:15672 (login `guest` / senha `guest`)

### 3. Derrubar
```bash
docker compose down
```
## Comandos úteis

```bash
docker compose up --build        # sobe tudo
docker compose down              # para tudo (mantém volumes)
docker compose down -v           # para tudo e apaga volumes
docker compose logs -f cozinha   # vê logs ao vivo da cozinha
docker compose stop cozinha      # desliga só a cozinha
docker compose start cozinha     # religa
docker compose ps                # lista containers ativos

USUARIO: Guest 
SENHA: Guest 
```
