#!/usr/bin/env bash
#
# send-many.sh - Envía muchas peticiones al endpoint de Kafka de Pedidos360
#                (microservicio ms-pedidos360-kafka, guía 3.2.1).
#
# Uso básico:
#   ./send-many.sh              # 100 mensajes, 1 petición a la vez
#   ./send-many.sh 500          # 500 mensajes
#   ./send-many.sh 1000 20      # 1000 mensajes con 20 peticiones en paralelo
#
# Variables de entorno (opcionales):
#   URL          Endpoint destino      (default: http://localhost:8888/api/kafka/send)
#   COUNT        Cantidad de mensajes  (default: 100)
#   PARALLEL     Peticiones simultáneas(default: 1)
#   KEY_POOL     Cantidad de keys distintas (default: 12)
#   KEY_PREFIX   Prefijo de la key     (default: orden)
#   MSG_PREFIX   Prefijo del mensaje   (default: mensaje)
#   METHOD       GET o POST            (default: POST)
#   TIMEOUT      Timeout por petición  (default: 15)
#   DELAY        Segundos entre envíos por worker (default: 0)
#   VERBOSE      1 muestra cada petición, 0 solo errores (default: 0)
#
# Ejemplos:
#   PARALLEL=10 ./send-many.sh 2000
#   KEY_POOL=3 PARALLEL=5 ./send-many.sh 300
#   METHOD=GET ./send-many.sh 50
#
set -uo pipefail

URL="${URL:-http://localhost:8888/api/kafka/send}"
COUNT="${COUNT:-${1:-100}}"
PARALLEL="${PARALLEL:-${2:-1}}"
KEY_POOL="${KEY_POOL:-12}"
KEY_PREFIX="${KEY_PREFIX:-orden}"
MSG_PREFIX="${MSG_PREFIX:-mensaje}"
METHOD="${METHOD:-POST}"
TIMEOUT="${TIMEOUT:-15}"
DELAY="${DELAY:-0}"
VERBOSE="${VERBOSE:-0}"

for var in COUNT PARALLEL KEY_POOL TIMEOUT; do
    if ! [[ "${!var}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: $var debe ser un número entero (valor actual: ${!var})" >&2
        exit 2
    fi
done

if (( COUNT < 1 )); then
    echo "ERROR: COUNT debe ser mayor que 0" >&2
    exit 2
fi
if (( PARALLEL < 1 )); then
    echo "ERROR: PARALLEL debe ser mayor que 0" >&2
    exit 2
fi
if (( KEY_POOL < 1 )); then
    echo "ERROR: KEY_POOL debe ser mayor que 0" >&2
    exit 2
fi
if [[ "$METHOD" != "GET" && "$METHOD" != "POST" ]]; then
    echo "ERROR: METHOD debe ser GET o POST" >&2
    exit 2
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "ERROR: curl no está instalado" >&2
    exit 2
fi

send_one() {
    local i="$1"
    local key="${KEY_PREFIX}-$(( (i - 1) % KEY_POOL ))"
    local msg="${MSG_PREFIX}-$i"
    local code

    if (( DELAY > 0 )); then
        sleep "$DELAY"
    fi

    code=$(curl -sS -o /dev/null -w '%{http_code}' \
        -X "$METHOD" -G \
        --data-urlencode "message=$msg" \
        --data-urlencode "key=$key" \
        --max-time "$TIMEOUT" \
        "$URL" 2>/dev/null) || code="000"

    printf '%s\t%s\t%s\n' "$code" "$i" "$key"
}
export -f send_one
export URL METHOD TIMEOUT DELAY KEY_PREFIX KEY_POOL MSG_PREFIX VERBOSE

# Comprueba que el endpoint esté disponible antes de empezar (sin enviar mensajes).
probe=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$URL" 2>/dev/null) || probe="000"
if [[ "$probe" == "000" || "$probe" =~ ^5 ]]; then
    echo "ERROR: el endpoint $URL no responde correctamente (HTTP $probe)." >&2
    echo "       Verifica que la app esté corriendo y que Kafka esté arriba:" >&2
    echo "       docker compose up -d zookeeper kafka && (cd ms-pedidos360-kafka && ./mvnw spring-boot:run)" >&2
    exit 1
fi

results="$(mktemp)"
trap 'rm -f "$results"' EXIT

echo "Enviando $COUNT mensajes a $URL"
echo "Método: $METHOD | Paralelismo: $PARALLEL | Keys distintas: $KEY_POOL"
echo

start=$(date +%s)
seq 1 "$COUNT" | xargs -r -P "$PARALLEL" -I{} bash -c 'send_one "$@"' _ {} > "$results"
end=$(date +%s)

ok=$(awk -F'\t' '$1 == 200' "$results" | wc -l)
fail=$(( COUNT - ok ))
elapsed=$(( end - start ))
(( elapsed == 0 )) && elapsed=1

if (( VERBOSE == 0 )); then
    # Muestra solo los fallos (si hay)
    awk -F'\t' '$1 != 200 { printf "FALLO  HTTP %s  mensaje=%s  key=%s\n", $1, $2, $3 }' "$results"
fi

echo
echo "----------------------------------------"
echo "Total enviado : $COUNT"
echo "OK (HTTP 200) : $ok"
echo "Fallidos      : $fail"
echo "Tiempo        : ${elapsed}s (~$(( COUNT / elapsed )) msg/s)"
echo "----------------------------------------"

if (( fail > 0 )); then
    exit 1
fi
