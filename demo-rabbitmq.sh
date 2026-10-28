#!/usr/bin/env bash
#
# Demo en vivo — RabbitMQ en Pedidos360
# Ejecuta: ./demo-rabbitmq.sh
# Modo sin pausas (para ensayar): AUTO=1 ./demo-rabbitmq.sh
#
set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO" || exit 1

CATALOG=http://localhost:8081
ORDERS=http://localhost:8082
RABBITADMIN=http://localhost:8084
MGMT=http://localhost:15672
USER=pedidos360
PASS=pedidos360
QUEUE=demo.inspeccion
EXCHANGE=pedidos360.orders.exchange

pause() {
  if [ "${AUTO:-0}" = "1" ]; then return; fi
  echo
  read -rp "  ⏎  Pulsa Enter para continuar..." _ || true
  echo
}

section() {
  echo
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

jq_or_cat() { python3 -m json.tool 2>/dev/null || cat; }

# ---------------------------------------------------------------- 0
section "0. El broker está vivo"
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages consumers 2>/dev/null \
  | grep -E "^name|orders\."
echo
echo "  ↑ 'consumers: 1' = hay un microservicio escuchando."
pause

# ---------------------------------------------------------------- 1
section "1. Un pedido real dispara un evento"
PID=$(curl -s "$CATALOG/api/catalog/products" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)[0]['id'])")
echo "  Producto de catálogo usado: #$PID"

ORDER=$(curl -s -X POST "$ORDERS/api/orders" \
  -H "Content-Type: application/json" \
  -H "X-User-Email: demo@test.cl" \
  -d "{\"items\":[{\"productId\":$PID,\"quantity\":2}]}")
OID=$(echo "$ORDER" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "  Pedido creado: #$OID"
echo "$ORDER" | jq_or_cat
echo
echo "  ── Log del productor (últimos eventos) ──"
docker compose logs --since=30s ms-pedidos360-orders 2>&1 \
  | grep -E "Evento publicado|publicado en Pub/Sub" | tail -4
pause

# ---------------------------------------------------------------- 2
section "2. Cola de inspección enlazada al exchange"
echo "  Creando cola '$QUEUE' con la API del proyecto (8084)..."
curl -s -X POST "$RABBITADMIN/api/rabbit/queues" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"$QUEUE\",\"durable\":true}" | jq_or_cat

echo
echo "  Enlazándola a '$EXCHANGE' con routing key 'order.#'..."
echo "  (La API del proyecto protege el prefijo 'pedidos360.', así que se usa la API de gestión)"
curl -s -o /dev/null -w "  HTTP %{http_code}\n" \
  -u "$USER:$PASS" -X POST \
  "$MGMT/api/bindings/%2F/e/$EXCHANGE/q/$QUEUE" \
  -H "Content-Type: application/json" \
  -d '{"routing_key":"order.#","arguments":{}}'

echo
echo "  Ahora creamos OTRO pedido y el evento debe llegar a la cola..."
curl -s -o /dev/null -X POST "$ORDERS/api/orders" \
  -H "Content-Type: application/json" \
  -H "X-User-Email: demo@test.cl" \
  -d "{\"items\":[{\"productId\":$PID,\"quantity\":1}]}"
sleep 2
echo -n "  Estado de la cola: "
curl -s "$RABBITADMIN/api/rabbit/queues/$QUEUE"
echo
echo "  ↑ messageCount > 0 = el exchange enrutó el evento hasta aquí."
pause

# ---------------------------------------------------------------- 3
section "3. Leer el evento sin borrarlo"
curl -s -u "$USER:$PASS" -X POST \
  "$MGMT/api/queues/%2F/$QUEUE/get" \
  -H "Content-Type: application/json" \
  -d '{"count":3,"ackmode":"ack_requeue_true","encoding":"auto"}' \
  | python3 -c "
import sys, json
msgs = json.load(sys.stdin)
for m in msgs:
    print('  routing_key :', m['routing_key'])
    print('  payload     :', m['payload'][:180])
    print()
"
echo "  ↑ El payload es el OrderStatusEvent completo."
pause

# ---------------------------------------------------------------- 4
section "4. Publicar un mensaje a mano"
echo "  Enviando un evento 'order.status.changed' directamente al exchange..."
curl -s -u "$USER:$PASS" -X POST \
  "$MGMT/api/exchanges/%2F/$EXCHANGE/publish" \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {"content_type":"application/json"},
    "routing_key": "order.status.changed",
    "payload": "{\"eventId\":\"demo-manual\",\"orderId\":'"$OID"',\"userEmail\":\"demo@test.cl\",\"previousStatus\":\"RECIBIDO\",\"newStatus\":\"CONFIRMADO\",\"total\":1000.00,\"occurredAt\":\"2026-10-28T18:00:00-03:00\"}",
    "payload_encoding": "string"
  }'
echo
echo "  ↑ {\"routed\":true} = entregado al exchange."
echo
echo "  ── Reacción de los consumidores ──"
sleep 2
docker compose logs --since=15s ms-pedidos360-orders 2>&1 \
  | grep -E "orderId|pedido #|Pub/Sub" | tail -4
pause

# ---------------------------------------------------------------- 5
section "5. Mensaje inválido → DLQ"
echo "  Publicando un evento con un pedido que NO existe (orderId 999999)..."
curl -s -u "$USER:$PASS" -X POST \
  "$MGMT/api/exchanges/%2F/$EXCHANGE/publish" \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {"content_type":"application/json"},
    "routing_key": "order.status.changed",
    "payload": "{\"eventId\":\"demo-dlq\",\"orderId\":999999,\"userEmail\":\"demo@test.cl\",\"previousStatus\":\"RECIBIDO\",\"newStatus\":\"CONFIRMADO\",\"total\":1000.00,\"occurredAt\":\"2026-10-28T18:00:00-03:00\"}",
    "payload_encoding": "string"
  }'
echo
sleep 2
echo "  ── El consumidor lo rechaza y lo manda a la DLQ ──"
docker compose logs --since=15s ms-pedidos360-orders 2>&1 \
  | grep -iE "dlq|rechazado" | tail -3
echo
echo "  ↑ La DLQ tiene su propio consumidor que registra el mensaje."
pause

# ---------------------------------------------------------------- 6
section "6. Limpieza"
curl -s -o /dev/null -w "  borrar binding : HTTP %{http_code}\n" \
  -u "$USER:$PASS" -X DELETE \
  "$MGMT/api/bindings/%2F/e/$EXCHANGE/q/$QUEUE/order.%23"
curl -s -o /dev/null -w "  borrar cola    : HTTP %{http_code}\n" \
  -X DELETE "$RABBITADMIN/api/rabbit/queues/$QUEUE"
echo
echo "  Demo terminada."
