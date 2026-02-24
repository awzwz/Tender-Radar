#!/bin/bash
# Запускает ngrok туннели и обновляет env для доступа извне
set -e
cd "$(dirname "$0")/.."

echo "=== Запуск ngrok ==="
echo "Убедитесь, что docker compose поднят (backend на 8000, frontend на 3002)"
echo ""

# Проверяем ngrok
if ! command -v ngrok &>/dev/null; then
  echo "ngrok не найден. Установите: brew install ngrok"
  echo "Или: npx ngrok http 3002"
  exit 1
fi

# Запускаем ngrok в фоне
echo "Запускаю ngrok туннели..."
ngrok start --config ngrok.yml --all &
NGROK_PID=$!

sleep 5

# Получаем URL из ngrok API
TUNNELS=$(curl -s http://127.0.0.1:4040/api/tunnels 2>/dev/null || true)
if [ -z "$TUNNELS" ]; then
  echo "Ошибка: не удалось получить туннели от ngrok. Проверьте, что ngrok запущен."
  kill $NGROK_PID 2>/dev/null || true
  exit 1
fi

BACKEND_URL=$(echo "$TUNNELS" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for t in d.get('tunnels',[]):
    if t.get('config',{}).get('addr','').endswith(':8000'):
        print(t['public_url'].rstrip('/'))
        break
" 2>/dev/null || echo "")

FRONTEND_URL=$(echo "$TUNNELS" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for t in d.get('tunnels',[]):
    if t.get('config',{}).get('addr','').endswith(':3002'):
        print(t['public_url'].rstrip('/'))
        break
" 2>/dev/null || echo "")

if [ -z "$BACKEND_URL" ] || [ -z "$FRONTEND_URL" ]; then
  echo "Не удалось получить URL. Проверьте ngrok."
  kill $NGROK_PID 2>/dev/null || true
  exit 1
fi

echo ""
echo "Backend URL:  $BACKEND_URL"
echo "Frontend URL: $FRONTEND_URL"
echo ""

# Обновляем .env
ENV_FILE=".env"
API_URL="${BACKEND_URL}/api/v1"

# Добавляем ngrok в CORS если ещё нет
if ! grep -q "ngrok" "$ENV_FILE" 2>/dev/null; then
  # Читаем текущий CORS и добавляем наши URL
  sed -i.bak "s|CORS_ORIGINS=.*|CORS_ORIGINS=http://localhost:3000,http://localhost:3001,http://localhost:3002,http://127.0.0.1:3000,http://127.0.0.1:3001,http://127.0.0.1:3002,${BACKEND_URL},${FRONTEND_URL}|" "$ENV_FILE" 2>/dev/null || true
fi

# Добавляем NEXT_PUBLIC_API_URL
if grep -q "^NEXT_PUBLIC_API_URL=" "$ENV_FILE" 2>/dev/null; then
  sed -i.bak "s|^NEXT_PUBLIC_API_URL=.*|NEXT_PUBLIC_API_URL=${API_URL}|" "$ENV_FILE" 2>/dev/null || true
else
  echo "" >> "$ENV_FILE"
  echo "NEXT_PUBLIC_API_URL=${API_URL}" >> "$ENV_FILE"
fi

echo "Обновил .env (CORS и NEXT_PUBLIC_API_URL)"
echo "Перезапускаю backend и frontend..."

docker compose restart backend frontend

sleep 5

echo ""
echo "=========================================="
echo "  ПОДЕЛИТЕСЬ ЭТОЙ ССЫЛКОЙ:"
echo "  $FRONTEND_URL"
echo "=========================================="
echo ""
echo "ngrok работает в фоне (PID $NGROK_PID). Ctrl+C для остановки ngrok."
wait $NGROK_PID 2>/dev/null || true
