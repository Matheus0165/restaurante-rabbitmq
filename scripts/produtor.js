/**
 * Produtor standalone (CLI)
 *
 * Roda fora do Docker, direto no terminal, para demonstrar
 * o envio de pedidos sem precisar do frontend.
 *
 * Uso:
 *   node produtor.js              → envia 1 pedido
 *   node produtor.js 5            → envia 5 pedidos
 *
 * Pré-requisitos:
 *   npm install amqplib
 */

const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const QUANTIDADE = parseInt(process.argv[2]) || 1;

const itens = [
  { item: 'Hambúrguer Duplo', valor: 32.90 },
  { item: 'X-Salada', valor: 24.50 },
  { item: 'Pizza Margherita', valor: 48.00 },
  { item: 'Refrigerante 350ml', valor: 7.00 },
  { item: 'Sundae de Chocolate', valor: 12.90 },
];

async function main() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const ch = await conn.createChannel();
  await ch.assertQueue('fila_cozinha', { durable: true });

  for (let i = 0; i < QUANTIDADE; i++) {
    const exemplo = itens[Math.floor(Math.random() * itens.length)];
    const pedido = {
      pedido_id: 100 + Math.floor(Math.random() * 9000),
      item: exemplo.item,
      mesa: Math.floor(Math.random() * 12) + 1,
      valor: exemplo.valor,
    };

    ch.sendToQueue('fila_cozinha', Buffer.from(JSON.stringify(pedido)), { persistent: true });
    console.log(`✓ Pedido #${pedido.pedido_id} enviado: ${pedido.item} (Mesa ${pedido.mesa}) R$ ${pedido.valor}`);
  }

  await ch.close();
  await conn.close();
  console.log(`\n${QUANTIDADE} pedido(s) publicado(s) em fila_cozinha.`);
}

main().catch(err => {
  console.error('Erro:', err.message);
  process.exit(1);
});
