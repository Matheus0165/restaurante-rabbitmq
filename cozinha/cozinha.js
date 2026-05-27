/**
 * Consumer da Cozinha
 *
 * 1. Lê pedidos de 'fila_cozinha'
 * 2. Simula o preparo (2.5 segundos)
 * 3. Publica o pedido pronto em 'fila_pagamento'
 * 4. Publica eventos na exchange 'eventos_sistema' para o frontend
 *    ver o que está acontecendo em tempo real
 */

const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const TEMPO_PREPARO_MS = 2500;

async function start() {
  while (true) {
    try {
      console.log('[cozinha] conectando ao RabbitMQ em', RABBITMQ_URL);
      const conn = await amqp.connect(RABBITMQ_URL);
      const ch = await conn.createChannel();

      await ch.assertQueue('fila_cozinha', { durable: true });
      await ch.assertQueue('fila_pagamento', { durable: true });
      await ch.assertExchange('eventos_sistema', 'fanout', { durable: false });

      // garante que só processa um pedido por vez (preparo é sequencial)
      ch.prefetch(1);

      console.log('[cozinha] ✓ aguardando pedidos em fila_cozinha...');

      ch.consume('fila_cozinha', async msg => {
        if (!msg) return;

        const pedido = JSON.parse(msg.content.toString());

        console.log('');
        console.log('───────────────────────────────────');
        console.log(`[cozinha] Pedido recebido na cozinha:`);
        console.log(`[cozinha] Pedido ${pedido.pedido_id} - ${pedido.item} - Mesa ${pedido.mesa}`);

        publicarEvento(ch, {
          tipo: 'cozinha:recebido',
          data: pedido,
          log: {
            nivel: 'kitchen',
            mensagem: `Cozinha recebeu pedido #${pedido.pedido_id} · ${pedido.item}`,
          },
        });

        console.log(`[cozinha] Preparando pedido...`);

        // simula tempo de preparo
        await new Promise(r => setTimeout(r, TEMPO_PREPARO_MS));

        console.log(`[cozinha] Pedido pronto!`);

        // envia para a próxima fila (financeiro vai consumir)
        ch.sendToQueue('fila_pagamento', Buffer.from(JSON.stringify(pedido)), {
          persistent: true,
          contentType: 'application/json',
        });

        publicarEvento(ch, {
          tipo: 'cozinha:pronto',
          data: pedido,
          log: {
            nivel: 'kitchen',
            mensagem: `Pedido #${pedido.pedido_id} pronto · publicado em fila_pagamento`,
          },
        });

        // confirma que processou
        ch.ack(msg);
      });

      conn.on('error', err => console.error('[cozinha] erro:', err.message));
      conn.on('close', () => {
        console.error('[cozinha] conexão fechada · reconectando em 3s...');
        setTimeout(start, 3000);
      });

      return;
    } catch (err) {
      console.log('[cozinha] RabbitMQ ainda não disponível, retry em 3s...', err.code || err.message);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

function publicarEvento(ch, evento) {
  ch.publish('eventos_sistema', '', Buffer.from(JSON.stringify(evento)));
}

// shutdown gracioso (importante para o docker stop)
process.on('SIGINT', () => {
  console.log('[cozinha] desligando...');
  process.exit(0);
});
process.on('SIGTERM', () => {
  console.log('[cozinha] desligando...');
  process.exit(0);
});

start();
