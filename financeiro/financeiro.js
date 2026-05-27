/**
 * Consumer do Financeiro
 *
 * 1. Lê pedidos prontos de 'fila_pagamento'
 * 2. Simula o processamento da cobrança (1.5 segundos)
 * 3. Publica eventos na exchange 'eventos_sistema' para o frontend
 */

const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const TEMPO_COBRANCA_MS = 1500;

async function start() {
  while (true) {
    try {
      console.log('[financeiro] conectando ao RabbitMQ em', RABBITMQ_URL);
      const conn = await amqp.connect(RABBITMQ_URL);
      const ch = await conn.createChannel();

      await ch.assertQueue('fila_pagamento', { durable: true });
      await ch.assertExchange('eventos_sistema', 'fanout', { durable: false });

      ch.prefetch(1);

      console.log('[financeiro] ✓ aguardando pagamentos em fila_pagamento...');

      ch.consume('fila_pagamento', async msg => {
        if (!msg) return;

        const pedido = JSON.parse(msg.content.toString());

        console.log('');
        console.log('───────────────────────────────────');
        console.log(`[financeiro] Financeiro recebeu o pedido ${pedido.pedido_id}`);
        console.log(`[financeiro] Processando pagamento de R$ ${pedido.valor.toFixed(2)}...`);

        publicarEvento(ch, {
          tipo: 'pagamento:recebido',
          data: pedido,
          log: {
            nivel: 'payment',
            mensagem: `Financeiro recebeu pedido #${pedido.pedido_id} · cobrando R$ ${pedido.valor.toFixed(2)}`,
          },
        });

        // simula tempo da cobrança
        await new Promise(r => setTimeout(r, TEMPO_COBRANCA_MS));

        console.log(`[financeiro] Pagamento aprovado!`);

        publicarEvento(ch, {
          tipo: 'pagamento:aprovado',
          data: pedido,
          log: {
            nivel: 'payment',
            mensagem: `Pagamento do pedido #${pedido.pedido_id} aprovado · R$ ${pedido.valor.toFixed(2)}`,
          },
        });

        ch.ack(msg);
      });

      conn.on('error', err => console.error('[financeiro] erro:', err.message));
      conn.on('close', () => {
        console.error('[financeiro] conexão fechada · reconectando em 3s...');
        setTimeout(start, 3000);
      });

      return;
    } catch (err) {
      console.log('[financeiro] RabbitMQ ainda não disponível, retry em 3s...', err.code || err.message);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

function publicarEvento(ch, evento) {
  ch.publish('eventos_sistema', '', Buffer.from(JSON.stringify(evento)));
}

process.on('SIGINT', () => { console.log('[financeiro] desligando...'); process.exit(0); });
process.on('SIGTERM', () => { console.log('[financeiro] desligando...'); process.exit(0); });

start();
