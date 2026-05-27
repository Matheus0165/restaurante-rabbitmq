/**
 * Server: Frontend + Produtor + Hub de eventos via Socket.IO
 *
 * - Serve o dashboard em /
 * - POST /api/pedido publica na fila_cozinha
 * - Escuta a exchange 'eventos_sistema' (fanout) e repassa os eventos
 *   publicados por cozinha e financeiro para o navegador via Socket.IO
 * - Faz polling na Management API do RabbitMQ a cada 1.5s para mostrar
 *   o número real de mensagens e consumers ativos em cada fila
 */

const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');
const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const RABBITMQ_MGMT_URL = process.env.RABBITMQ_MGMT_URL || 'http://localhost:15672';
const RABBITMQ_USER = process.env.RABBITMQ_USER || 'guest';
const RABBITMQ_PASS = process.env.RABBITMQ_PASS || 'guest';
const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

let channel = null;
let proximoPedidoId = 100;

// ============================================================
// CONEXÃO COM O RABBITMQ (com retry porque o container demora)
// ============================================================
async function connectRabbit() {
  while (true) {
    try {
      console.log('[server] conectando ao RabbitMQ em', RABBITMQ_URL);
      const conn = await amqp.connect(RABBITMQ_URL);
      const ch = await conn.createChannel();

      // declara as filas e exchange de eventos
      await ch.assertQueue('fila_cozinha', { durable: true });
      await ch.assertQueue('fila_pagamento', { durable: true });
      await ch.assertExchange('eventos_sistema', 'fanout', { durable: false });

      // fila anônima exclusiva para receber eventos do sistema
      const q = await ch.assertQueue('', { exclusive: true });
      await ch.bindQueue(q.queue, 'eventos_sistema', '');

      ch.consume(q.queue, msg => {
        if (!msg) return;
        try {
          const evento = JSON.parse(msg.content.toString());
          // repassa o evento para todos os clientes conectados
          io.emit(evento.tipo, evento.data);
          if (evento.log) io.emit('log', evento.log);
        } catch (err) {
          console.error('[server] erro ao processar evento:', err.message);
        }
      }, { noAck: true });

      conn.on('error', err => {
        console.error('[server] conexão perdida:', err.message);
      });
      conn.on('close', () => {
        console.error('[server] conexão fechada · reconectando...');
        channel = null;
        setTimeout(connectRabbit, 3000);
      });

      channel = ch;
      console.log('[server] ✓ conectado ao RabbitMQ');
      io.emit('rabbitmq:status', { conectado: true });
      return;
    } catch (err) {
      console.log('[server] RabbitMQ ainda não disponível, retry em 3s...', err.code || err.message);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

// ============================================================
// ENDPOINT PARA O PRODUTOR (frontend chama via fetch)
// ============================================================
app.post('/api/pedido', async (req, res) => {
  if (!channel) {
    return res.status(503).json({ erro: 'RabbitMQ não conectado' });
  }

  const pedido = {
    pedido_id: req.body.pedido_id || ++proximoPedidoId,
    item: req.body.item || 'Item sem nome',
    mesa: parseInt(req.body.mesa) || 1,
    valor: parseFloat(req.body.valor) || 0,
    timestamp: new Date().toISOString(),
  };

  // publica na fila da cozinha
  channel.sendToQueue('fila_cozinha', Buffer.from(JSON.stringify(pedido)), {
    persistent: true,
    contentType: 'application/json',
  });

  // emite evento para o frontend
  channel.publish('eventos_sistema', '', Buffer.from(JSON.stringify({
    tipo: 'pedido:publicado',
    data: pedido,
    log: {
      nivel: 'producer',
      mensagem: `Pedido #${pedido.pedido_id} publicado em fila_cozinha · ${pedido.item} · Mesa ${pedido.mesa}`,
    },
  })));

  console.log(`[server] pedido #${pedido.pedido_id} publicado`);
  res.json({ ok: true, pedido });
});

// envia múltiplos pedidos de teste (para o botão "Enviar 5 pedidos")
app.post('/api/pedido/teste', async (req, res) => {
  const quantidade = parseInt(req.body.quantidade) || 5;
  const itens = [
    { item: 'Hambúrguer Duplo', valor: 32.90 },
    { item: 'X-Salada', valor: 24.50 },
    { item: 'Pizza Margherita', valor: 48.00 },
    { item: 'Refrigerante 350ml', valor: 7.00 },
    { item: 'Sundae de Chocolate', valor: 12.90 },
    { item: 'Batata Frita G', valor: 18.50 },
  ];

  for (let i = 0; i < quantidade; i++) {
    const ex = itens[Math.floor(Math.random() * itens.length)];
    const pedido = {
      pedido_id: ++proximoPedidoId,
      item: ex.item,
      mesa: Math.floor(Math.random() * 12) + 1,
      valor: ex.valor,
      timestamp: new Date().toISOString(),
    };

    channel.sendToQueue('fila_cozinha', Buffer.from(JSON.stringify(pedido)), { persistent: true });
    channel.publish('eventos_sistema', '', Buffer.from(JSON.stringify({
      tipo: 'pedido:publicado',
      data: pedido,
      log: {
        nivel: 'producer',
        mensagem: `Pedido #${pedido.pedido_id} publicado em fila_cozinha · ${pedido.item} · Mesa ${pedido.mesa}`,
      },
    })));

    await new Promise(r => setTimeout(r, 200));
  }

  res.json({ ok: true, quantidade });
});

// ============================================================
// POLLING DA MANAGEMENT API
// Pega tamanho real das filas e número de consumers ativos.
// Quando você faz `docker compose stop cozinha`, o consumer some
// e o frontend reflete automaticamente.
// ============================================================
async function pollStatus() {
  const auth = 'Basic ' + Buffer.from(`${RABBITMQ_USER}:${RABBITMQ_PASS}`).toString('base64');
  const headers = { Authorization: auth };

  try {
    const [cozinhaRes, pagRes] = await Promise.all([
      fetch(`${RABBITMQ_MGMT_URL}/api/queues/%2F/fila_cozinha`, { headers }),
      fetch(`${RABBITMQ_MGMT_URL}/api/queues/%2F/fila_pagamento`, { headers }),
    ]);

    if (cozinhaRes.ok && pagRes.ok) {
      const cozinha = await cozinhaRes.json();
      const pagamento = await pagRes.json();

      io.emit('status:filas', {
        cozinha: {
          mensagens: cozinha.messages || 0,
          consumers: cozinha.consumers || 0,
        },
        pagamento: {
          mensagens: pagamento.messages || 0,
          consumers: pagamento.consumers || 0,
        },
      });
    }
  } catch (err) {
    // silencioso — Management API pode estar indisponível por alguns segundos
  }
}

setInterval(pollStatus, 1500);

// ============================================================
// SOCKET.IO
// ============================================================
io.on('connection', socket => {
  console.log('[server] cliente conectado:', socket.id);
  socket.emit('rabbitmq:status', { conectado: !!channel });
});

// ============================================================
// START
// ============================================================
server.listen(PORT, () => {
  console.log(`[server] HTTP rodando em http://localhost:${PORT}`);
  connectRabbit();
});
